import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

// Listener -> Worker 요청 큐 (lock + condition)
public class RequestQueue {
    // 요청 1건 (seats가 null이면 좌석 형식 오류)
    record Request(Conn conn, int clientId, int reqId, String type, int[] seats) {
    }

    private final int capacity;
    private final ArrayDeque<Request> items = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();  // 비었을 때 Worker 대기
    private final Condition notFull = lock.newCondition();  // 꽉 찼을 때 Listener 대기
    private boolean closed;
    private int maxSize;  // 최대 길이 (지표)

    RequestQueue(int capacity) {
        this.capacity = capacity;
    }

    // 꽉 차 있으면 최대 timeoutMs 기다림. 못 넣으면 false
    boolean offer(Request req, long timeoutMs) throws InterruptedException {
        long nanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        lock.lock();
        try {
            while (items.size() >= capacity && !closed) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = notFull.awaitNanos(nanos);
            }
            if (closed) {
                return false;
            }
            items.addLast(req);
            maxSize = Math.max(maxSize, items.size());
            notEmpty.signal();  // Worker 하나 깨움
            return true;
        } finally {
            lock.unlock();
        }
    }

    // 비어 있으면 대기. 닫혔고 비었으면 null
    Request take() throws InterruptedException {
        lock.lock();
        try {
            while (items.isEmpty() && !closed) {
                notEmpty.await();
            }
            if (items.isEmpty()) {
                return null;
            }
            Request req = items.pollFirst();
            notFull.signal();  // Listener 깨움
            return req;
        } finally {
            lock.unlock();
        }
    }

    // 종료할 때 자고 있는 스레드 전부 깨움
    void close() {
        lock.lock();
        try {
            closed = true;
            notEmpty.signalAll();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    boolean isClosed() {
        lock.lock();
        try {
            return closed;
        } finally {
            lock.unlock();
        }
    }

    int size() {
        lock.lock();
        try {
            return items.size();
        } finally {
            lock.unlock();
        }
    }

    int maxSize() {
        lock.lock();
        try {
            return maxSize;
        } finally {
            lock.unlock();
        }
    }
}
