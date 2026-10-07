import java.util.ArrayDeque;
import java.util.Locale;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

// 대기자에게 좌석이 넘어갔을 때 NOTIFY를 보내는 스레드
public class Notifier extends Thread {
    // 알림 작업
    record Job(SeatManager.WaitEntry entry, int seat) {
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition hasWork = lock.newCondition();
    private final ArrayDeque<Job> jobs = new ArrayDeque<>();
    private boolean closing;

    final LongAdder sent = new LongAdder();
    final LongAdder failed = new LongAdder();  // 연결이 끊겨서 못 보낸 수
    final LongAdder waitMillisSum = new LongAdder();  // 대기 시간 합 (평균 계산용)

    Notifier() {
        super("Notifier");
    }

    // Worker가 CANCEL 처리 후 호출
    void enqueue(Job job) {
        lock.lock();
        try {
            jobs.addLast(job);
            hasWork.signal();
        } finally {
            lock.unlock();
        }
    }

    // 종료 시 남은 작업 다 보내고 끝나게 함
    void closeAndDrain() {
        lock.lock();
        try {
            closing = true;
            hasWork.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void run() {
        while (true) {
            Job job;
            lock.lock();
            try {
                while (jobs.isEmpty() && !closing) {
                    hasWork.await();  // 할 일 없으면 대기
                }
                if (jobs.isEmpty()) {
                    return;
                }
                job = jobs.pollFirst();
            } catch (InterruptedException e) {
                return;
            } finally {
                lock.unlock();
            }
            deliver(job);
        }
    }

    private void deliver(Job job) {
        SeatManager.WaitEntry e = job.entry();
        if (e.conn().send("NOTIFY " + e.reqId() + " " + job.seat())) {
            long waitedMs = (System.nanoTime() - e.registeredAtNanos()) / 1_000_000;
            sent.increment();
            waitMillisSum.add(waitedMs);
            Server.log.write("NOTIFY", "SUCCESS", String.format(Locale.ROOT, "Notifier sent NOTIFY to Client%d req=%d seat#%d (waited %.3fs).",
                    e.clientId(), e.reqId(), job.seat(), waitedMs / 1000.0));
        } else {
            failed.increment();
            Server.log.write("NOTIFY", "FAIL", "Notifier could not deliver NOTIFY to Client" + e.clientId()
                    + " req=" + e.reqId() + " seat#" + job.seat() + " (connection closed).");
        }
    }
}
