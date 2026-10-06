import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 좌석 예매 서버 진입점: 시작 → 실행 → 종료 흐름, 공유 객체·통계, 종료 보고서.
 *
 * 실행 예:
 *   java -cp out Server --host 0.0.0.0 --port 5000
 *
 * 인자 (괄호 안은 기본값)
 *   --host 바인딩 IP (필수, 예: 0.0.0.0)   --port 포트 (필수, 예: 5000)
 *   ※ IP/포트는 하드코딩하지 않는다는 조건(명세 §0-2)에 따라 기본값 없이 반드시 인자로 받는다.
 *   --clients Client 수 (30)     --requests Client당 요청 수 (5000)
 *   --queue Request Queue 용량 (1000)   --pool-sec POOL 로그 주기 (5)
 *   --deadlock-sec Deadlock 의심 무진행 시간 (30)   --log-dir 로그 폴더 (logs)
 *
 * 서버 스레드 구성 (이 외의 스레드는 만들지 않는다)
 *   Listener 1개 (= main 스레드, Listener.java): 연결 수락, 소켓 30개 감시, 요청 파싱 → Request Queue
 *   Worker 10개 (Worker.java)             : Request Queue에서 꺼내 좌석 판정(SeatManager.java) → 응답
 *   Notifier 1개 (Notifier.java)          : CANCEL로 대기자에게 좌석이 넘어가면 NOTIFY 전송
 *   (5초 POOL 로그와 Deadlock 감시는 Listener의 select 타임아웃을 이용 → Monitor 스레드 없음)
 *
 * 메시지 형식 (한 줄 = 한 메시지, 끝에 '\n')
 *   Client → Server : HELLO <id> / RESERVE <reqId> <seat> / RESERVE_MULTI <reqId> <s1,s2,..> / CANCEL <reqId> <seat>
 *   Server → Client : RESP <reqId> SUCCESS|FAIL|WAITLISTED [reason] / NOTIFY <reqId> <seat> / BYE
 */
public class Server {

    static final int WORKER_COUNT = 10; // 과제 조건: Worker 10개 고정

    // ---- 실행 인자 ----
    // host, port는 하드코딩 금지 조건(명세 §0-2) 때문에 기본값이 없다. 인자로 안 주면 실행되지 않는다.
    static String host;
    static int port = -1;
    static int expectedClients = 30;
    static int requestsPerClient = 5000;
    static int queueCapacity = 1000;
    static int poolSec = 5;
    static int deadlockSec = 30;
    static String logDir = "logs";

    // ---- Worker / Notifier가 같이 쓰는 공유 객체 ----
    static RequestQueue requestQueue;
    static Notifier notifier;
    static Log log;
    static long totalExpected; // Client 수 × Client당 요청 수. 첫 응답이 이만큼 나가면 종료

    // ---- 응답 통계 (좌석 집계는 SeatManager, 통지 집계는 Notifier에 있음) ----
    static final AtomicLong processed = new AtomicLong(); // 첫 응답을 보낸 요청 수 (종료 조건이라 정확한 값 필요)
    static final LongAdder successCount = new LongAdder();
    static final LongAdder failCount = new LongAdder();
    static final LongAdder waitlistedCount = new LongAdder();
    static final LongAdder deadlockCount = new LongAdder();
    static volatile long startMillis;        // 첫 Client 연결 시각 (처리량 계산 구간 시작)
    static volatile long lastResponseMillis; // 마지막 첫 응답 시각 (처리량 계산 구간 끝)

    // =====================================================================
    // main: 시작 → 실행 → 종료 (명세 §3 Step 1 ~ 7)
    // =====================================================================

    /**
     * 서버 전체 흐름.
     * Step 1 초기화(좌석·큐·Worker·Notifier) → Step 2~6 listenLoop()로 요청 수신
     * → Step 7 종료(Worker·Notifier 정리, 최종 보고서, BYE 전송, 소켓 닫기).
     */
    public static void main(String[] args) throws Exception {
        parseArgs(args);
        Files.createDirectories(Paths.get(logDir));
        log = new Log("SERVER", Paths.get(logDir, "Server.txt"));
        Thread.currentThread().setName("Listener"); // main 스레드가 곧 Listener
        totalExpected = (long) expectedClients * requestsPerClient;

        // ---- Step 1: 좌석 100개(전부 EMPTY), Request Queue, Worker 10개, Notifier 1개 ----
        SeatManager.init();
        requestQueue = new RequestQueue(queueCapacity);
        notifier = new Notifier();
        Worker[] workers = new Worker[WORKER_COUNT];
        for (int i = 0; i < WORKER_COUNT; i++) {
            workers[i] = new Worker(i + 1);
        }
        Listener.openServerSocket(); // 포트를 못 열면 여기서 예외로 바로 끝난다
        log.console("INIT", "SUCCESS", String.format(
                "Seat map(%d) initialized. Worker pool size=%d. queue_capacity=%d listen=%s:%d expected=%dx%d timezone=%s.",
                SeatManager.SEAT_COUNT, WORKER_COUNT, queueCapacity, host, port, expectedClients, requestsPerClient,
                Log.ZONE));
        for (Worker w : workers) {
            w.start();
        }
        notifier.start();

        // ---- Step 2~6: 모든 요청에 첫 응답이 나갈 때까지 연결 수락·요청 수신 ----
        Listener.listenLoop();

        // ---- Step 7: Graceful Termination ----
        log.write("TERMINATE", "INFO", "Shutdown started: " + Listener.shutdownReason + ".");
        // (1) 큐를 닫으면 Worker는 남은 요청을 마저 처리하고 끝난다
        requestQueue.close();
        for (Worker w : workers) {
            w.join();
        }
        // (2) Worker가 다 끝났으니 새 통지는 더 없다. 남은 통지를 다 보내고 Notifier 종료
        notifier.closeAndDrain();
        notifier.join();
        // (3) 최종 좌석 현황 · 이중예약 검사 · 전체 통계
        boolean pass = writeFinalReport();
        // (4) 모든 Client에 종료 신호 → Client들이 끊을 때까지 잠시 대기 → 소켓 정리
        int byeCount = Listener.broadcastBye();
        Listener.waitForClientsToClose(5000);
        Listener.closeAll();
        log.console("TERMINATE", "SUCCESS", "Graceful shutdown. Termination signal sent to " + byeCount
                + " clients, all threads joined. integrity=" + (pass ? "PASS" : "FAIL") + ".");
        log.close();
    }

    /**
     * 실행 인자를 읽어 위의 설정값에 넣는다.
     * --host, --port가 없거나 모르는 인자가 있으면 예외로 바로 종료한다 (오타로 기본값 실행되는 사고 방지).
     */
    static void parseArgs(String[] args) {
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Arguments must be --key value pairs.");
        }
        for (int i = 0; i < args.length; i += 2) {
            String v = args[i + 1];
            switch (args[i]) {
                case "--host" -> host = v;
                case "--port" -> port = Integer.parseInt(v);
                case "--clients" -> expectedClients = Integer.parseInt(v);
                case "--requests" -> requestsPerClient = Integer.parseInt(v);
                case "--queue" -> queueCapacity = Integer.parseInt(v);
                case "--pool-sec" -> poolSec = Integer.parseInt(v);
                case "--deadlock-sec" -> deadlockSec = Integer.parseInt(v);
                case "--log-dir" -> logDir = v;
                default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
        if (host == null || port < 0) {
            throw new IllegalArgumentException("--host and --port are required. e.g. --host 0.0.0.0 --port 5000");
        }
    }

    // =====================================================================
    // 종료 보고서 (Worker/Notifier가 모두 끝난 뒤 호출 → 값이 더 이상 안 바뀜)
    //
    //  서버 혼자 확인 가능한 정합성 항목 (명세 §4-2)
    //   (a) 이중예약 0건
    //   (b) 배정 수 − 해제 수 = 종료 시 예약 좌석 수
    //   (c) WAITLISTED 수 = 대기자에게 넘긴 수 + 종료 시 미해결 대기 수
    //   (d) 대기자에게 넘긴 좌석은 모두 NOTIFY 전송 완료
    //  Client 로그와의 대조(서버 최종 좌석 = Client 최종 보유 좌석)는 Verify.java로 한다.
    // =====================================================================

    /**
     * 최종 좌석 현황, 이중예약 검사 결과, 성능 지표를 Server.txt에 기록한다.
     * @return 서버 쪽 정합성 PASS 여부
     */
    static boolean writeFinalReport() {
        SeatManager.Snapshot snap = SeatManager.snapshot();
        // 최종 좌석 현황: 10석씩 한 줄 (Verify.java가 "번호=ClientN" 형식을 읽는다)
        for (int start = 1; start <= SeatManager.SEAT_COUNT; start += 10) {
            StringBuilder sb = new StringBuilder("Final seat map [" + start + "-" + (start + 9) + "]:");
            for (int n = start; n <= start + 9; n++) {
                int owner = snap.owners()[n];
                sb.append(' ').append(n).append('=').append(owner == SeatManager.EMPTY ? "EMPTY" : "Client" + owner);
            }
            log.write("TERMINATE", "INFO", sb.toString());
        }

        long db = SeatManager.doubleBooking.sum();
        long assigned = SeatManager.assigned.sum();
        long released = SeatManager.released.sum();
        int reservedNow = snap.reservedCount();
        boolean balanceOk = assigned - released == reservedNow;

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
            log.write("TERMINATE", "INFO", waitlistMsg);      // 정상이면 파일에만
        } else {
            log.console("TERMINATE", "WARN", waitlistMsg);    // 안 맞으면 화면에도 띄운다
        }

        // 성능 지표 (명세 §4-1)
        long done = processed.get();
        double elapsedSec = startMillis == 0 ? 0 : Math.max(1, lastResponseMillis - startMillis) / 1000.0;
        double throughput = elapsedSec == 0 ? 0 : done / elapsedSec;
        double avgWaitSec = notifySent == 0 ? 0 : notifier.waitMillisSum.sum() / (double) notifySent / 1000.0;
        log.console("TERMINATE", "INFO", String.format(
                "Metrics: processed=%d elapsed_sec=%.1f throughput=%.1f max_queue=%d double_booking=%d deadlock=%d "
                        + "avg_waitlist_wait_sec=%.3f contention=%d success=%d fail=%d waitlisted=%d",
                done, elapsedSec, throughput, requestQueue.maxSize(), db, deadlockCount.sum(),
                avgWaitSec, SeatManager.contention.sum(), successCount.sum(), failCount.sum(), waitlisted));

        boolean pass = db == 0 && balanceOk && waitlistOk;
        log.write("TERMINATE", pass ? "SUCCESS" : "FAIL",
                "Server-side integrity=" + (pass ? "PASS" : "FAIL") + " (cross-check with client logs: Verify).");
        return pass;
    }
}
