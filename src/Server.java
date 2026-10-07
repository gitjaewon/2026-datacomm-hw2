import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

// 좌석 예매 서버. 실행: java -cp out Server --host 0.0.0.0 --port 5000
public class Server {
    static final int WORKER_COUNT = 10;  // Worker 스레드 수 (고정)

    static final int expectedClients = 30;
    static final int queueCapacity = 1000;
    static final int poolSec = 5;
    static final int deadlockSec = 30;

    // 실행 인자 (host, port는 필수)
    static String host;
    static int port = -1;
    static int requestsPerClient = 5000;
    static String logDir = "logs";

    // Worker, Notifier와 같이 쓰는 객체
    static RequestQueue requestQueue;
    static Notifier notifier;
    static Log log;
    static long totalExpected;  // 30 x 요청 수. 이만큼 응답하면 종료

    // 통계
    static final AtomicLong processed = new AtomicLong();
    static final LongAdder successCount = new LongAdder();
    static final LongAdder failCount = new LongAdder();
    static final LongAdder waitlistedCount = new LongAdder();
    static final LongAdder deadlockCount = new LongAdder();
    static volatile long startMillis;  // 처리량 계산용
    static volatile long lastResponseMillis;

    public static void main(String[] args) throws Exception {
        parseArgs(args);
        Files.createDirectories(Paths.get(logDir));
        log = new Log("SERVER", Paths.get(logDir, "Server.txt"));
        Thread.currentThread().setName("Listener");  // main 스레드가 Listener 역할
        totalExpected = (long) expectedClients * requestsPerClient;

        // 좌석, 큐, Worker 10개, Notifier 생성
        SeatManager.init();
        requestQueue = new RequestQueue(queueCapacity);
        notifier = new Notifier();
        Worker[] workers = new Worker[WORKER_COUNT];
        for (int i = 0; i < WORKER_COUNT; i++) {
            workers[i] = new Worker(i + 1);
        }
        Listener.openServerSocket();
        log.console("INIT", "SUCCESS", String.format(
                "Seat map(%d) initialized. Worker pool size=%d. queue_capacity=%d listen=%s:%d expected=%dx%d timezone=%s.",
                SeatManager.SEAT_COUNT, WORKER_COUNT, queueCapacity, host, port, expectedClients, requestsPerClient,
                Log.ZONE));
        for (Worker w : workers) {
            w.start();
        }
        notifier.start();

        // 모든 요청을 처리할 때까지 여기서 돈다
        Listener.listenLoop();

        // 종료 처리
        log.write("TERMINATE", "INFO", "Shutdown started: " + Listener.shutdownReason + ".");
        // 큐를 닫으면 Worker들이 남은 요청 처리 후 종료
        requestQueue.close();
        for (Worker w : workers) {
            w.join();
        }
        // 남은 NOTIFY 다 보내고 Notifier 종료
        notifier.closeAndDrain();
        notifier.join();
        boolean pass = writeFinalReport();
        int byeCount = Listener.broadcastBye();
        Listener.waitForClientsToClose(5000);
        Listener.closeAll();
        log.console("TERMINATE", "SUCCESS", "Graceful shutdown. Termination signal sent to " + byeCount
                + " clients, all threads joined. integrity=" + (pass ? "PASS" : "FAIL") + ".");
        log.close();
    }

    static void parseArgs(String[] args) {
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Arguments must be --key value pairs.");
        }
        for (int i = 0; i < args.length; i += 2) {
            String v = args[i + 1];
            switch (args[i]) {
                case "--host" -> host = v;
                case "--port" -> port = Integer.parseInt(v);
                case "--requests" -> requestsPerClient = Integer.parseInt(v);
                case "--log-dir" -> logDir = v;
                default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
        if (host == null || port < 0) {
            throw new IllegalArgumentException("--host and --port are required. e.g. --host 0.0.0.0 --port 5000");
        }
    }

    // 최종 좌석 현황, 이중예약 검사, 지표 기록
    static boolean writeFinalReport() {
        SeatManager.Snapshot snap = SeatManager.snapshot();
        for (int start = 1; start <= SeatManager.SEAT_COUNT; start += 10) {
            StringBuilder sb = new StringBuilder("Final seat map [" + start + "-" + (start + 9) + "]:");
            for (int n = start; n <= start + 9; n++) {
                int owner = snap.owners()[n];
                sb.append(' ').append(n).append('=').append(owner == SeatManager.EMPTY ? "EMPTY" : "Client" + owner);
            }
            log.write("TERMINATE", "INFO", sb.toString());
        }

        // 배정 - 해제 = 현재 예약 좌석 수인지 확인
        long db = SeatManager.doubleBooking.sum();
        long assigned = SeatManager.assigned.sum();
        long released = SeatManager.released.sum();
        int reservedNow = snap.reservedCount();
        boolean balanceOk = assigned - released == reservedNow;

        // WAITLISTED = 넘겨준 수 + 미해결 대기
        long waitlisted = waitlistedCount.sum();
        long handoffs = SeatManager.handoffs.sum();
        long notifySent = notifier.sent.sum();
        int pending = snap.waitlistTotal();
        boolean waitlistOk = waitlisted == handoffs + pending && notifySent == handoffs;

        log.console("DOUBLE_BOOKING_CHECK", db == 0 && balanceOk ? "SUCCESS" : "FAIL", String.format(
                "double_booking=%d assigned=%d released=%d reserved_now=%d balance=%s",
                db, assigned, released, reservedNow, balanceOk ? "OK" : "MISMATCH"));
        String waitlistMsg = String.format(
                "pending_waitlist=%d waitlisted=%d handoffs=%d notify_sent=%d notify_failed=%d waitlist_balance=%s",
                pending, waitlisted, handoffs, notifySent, notifier.failed.sum(), waitlistOk ? "OK" : "MISMATCH");
        if (waitlistOk) {
            log.write("TERMINATE", "INFO", waitlistMsg);
        } else {
            log.console("TERMINATE", "WARN", waitlistMsg);
        }

        long done = processed.get();
        double elapsedSec = startMillis == 0 ? 0 : Math.max(1, lastResponseMillis - startMillis) / 1000.0;
        double throughput = elapsedSec == 0 ? 0 : done / elapsedSec;
        double avgWaitSec = notifySent == 0 ? 0 : notifier.waitMillisSum.sum() / (double) notifySent / 1000.0;
        log.console("TERMINATE", "INFO", String.format(
                "Metrics: processed=%d elapsed_sec=%.1f throughput=%.1f max_queue=%d double_booking=%d deadlock=%d "
                        + "avg_waitlist_wait_sec=%.3f contention=%d success=%d fail=%d waitlisted=%d",
                done, elapsedSec, throughput, requestQueue.maxSize(), db, deadlockCount.sum(),
                avgWaitSec, SeatManager.contention.sum(), successCount.sum(), failCount.sum(), waitlisted));

        return db == 0 && balanceOk && waitlistOk;
    }
}
