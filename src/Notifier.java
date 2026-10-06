import java.util.ArrayDeque;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Waitlist 통지 전용 스레드 (Notifier Thread, 1개).
 *
 * 흐름 (명세 §0-6, §0-7 예제 3)
 *   1) Worker가 CANCEL을 처리하며 좌석 Lock 안에서 대기 1번에게 좌석을 넘긴다. (SeatManager.cancel)
 *   2) Worker는 좌석 Lock을 푼 뒤 통지 작업을 Notify Queue에 넣고 Condition Variable로 Notifier를 깨운다.
 *   3) Notifier는 깨어나 NOTIFY를 보내고 대기시간을 기록한 뒤, 일이 없으면 다시 잠든다.
 *
 * 이렇게 나눠서 Worker는 통지 때문에 멈추지 않고 바로 다음 요청을 처리한다.
 * FIFO(배정 순서)는 SeatManager.cancel()에서 Lock 안에 이미 정해졌고, 여기서는 받은 순서대로 보내기만 한다.
 */
public class Notifier extends Thread {

    /** 통지 작업: 대기 요청(entry)에게 좌석(seat)이 배정됐음을 알린다 */
    record Job(SeatManager.WaitEntry entry, int seat) {
    }

    // ---- Notify Queue: Mutex + Condition Variable ----
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition hasWork = lock.newCondition();
    private final ArrayDeque<Job> jobs = new ArrayDeque<>();
    private boolean closing;

    // ---- 통계 ----
    final LongAdder sent = new LongAdder();
    final LongAdder failed = new LongAdder();        // 대기자 연결이 끊겨 못 보낸 수
    final LongAdder waitMillisSum = new LongAdder(); // Waitlist 등록 ~ NOTIFY 전송 시간 합 (보낸 것만)

    /** 스레드 이름을 "Notifier"로 정한다. 실제 실행은 Server.main의 start()에서 시작된다. */
    Notifier() {
        super("Notifier");
    }

    /** 통지 작업을 Notify Queue에 넣고 잠든 Notifier를 깨운다. Worker가 CANCEL 처리 후 좌석 Lock을 모두 푼 뒤에 부른다. */
    void enqueue(Job job) {
        lock.lock();
        try {
            jobs.addLast(job);
            hasWork.signal(); // 잠든 Notifier 깨우기
        } finally {
            lock.unlock();
        }
    }

    /** 종료 시: 남은 통지를 모두 보낸 뒤 run()이 끝난다 (명세 Step 7) */
    void closeAndDrain() {
        lock.lock();
        try {
            closing = true;
            hasWork.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Notifier 스레드 본체. Notify Queue에서 작업을 하나씩 꺼내 deliver() 한다.
     * 일이 없으면 CV에서 잠들고, closeAndDrain() 이후 큐가 비면 끝난다.
     */
    @Override
    public void run() {
        while (true) {
            Job job;
            lock.lock();
            try {
                while (jobs.isEmpty() && !closing) {
                    hasWork.await(); // 할 일이 없으면 CV에서 잠든다 (busy-waiting 없음)
                }
                if (jobs.isEmpty()) {
                    return; // 종료 요청 + 남은 통지 없음
                }
                job = jobs.pollFirst();
            } catch (InterruptedException e) {
                return;
            } finally {
                lock.unlock();
            }
            deliver(job); // 전송·로그는 Notify Queue Lock 밖에서 (그동안 Worker가 enqueue 할 수 있게)
        }
    }

    /** NOTIFY 한 건을 대기자에게 보내고, Waitlist 대기시간을 기록하고, 로그를 남긴다. */
    private void deliver(Job job) {
        SeatManager.WaitEntry e = job.entry();
        long waitedMs = System.currentTimeMillis() - e.registeredAtMillis();
        if (e.conn().send("NOTIFY " + e.reqId() + " " + job.seat())) {
            sent.increment();
            waitMillisSum.add(waitedMs);
            Server.log.write("NOTIFY", "SUCCESS", String.format("Notifier sent NOTIFY to Client%d req=%d seat#%d (waited %.3fs).",
                    e.clientId(), e.reqId(), job.seat(), waitedMs / 1000.0));
        } else {
            // 대기자가 이미 연결을 끊은 경우. 좌석 배정 자체는 유효하므로 되돌리지 않는다.
            failed.increment();
            Server.log.write("NOTIFY", "FAIL", "Notifier could not deliver NOTIFY to Client" + e.clientId()
                    + " req=" + e.reqId() + " seat#" + job.seat() + " (connection closed).");
        }
    }
}
