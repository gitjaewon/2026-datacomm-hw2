import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

// 좌석 100개와 예약/취소 처리. 좌석마다 lock이 따로 있다
public class SeatManager {
    static final int SEAT_COUNT = 100;
    static final int EMPTY = 0;  // 빈 좌석

    static final Seat[] seats = new Seat[SEAT_COUNT + 1];

    static void init() {
        for (int n = 1; n <= SEAT_COUNT; n++) {
            seats[n] = new Seat();
        }
    }

    // 통계 (대기자에게 넘길 때도 해제 1 + 배정 1로 셈)
    static final LongAdder assigned = new LongAdder();
    static final LongAdder released = new LongAdder();
    static final LongAdder handoffs = new LongAdder();
    static final LongAdder doubleBooking = new LongAdder();  // 0이어야 함
    static final LongAdder contention = new LongAdder();  // lock 기다린 횟수

    // 단일 예약: 비었으면 배정, 남의 좌석이면 대기열 등록
    static Result reserve(Conn conn, int clientId, int reqId, int seatNo) {
        if (seatNo < 1 || seatNo > SEAT_COUNT) {
            return new Result("FAIL", "INVALID_SEAT");
        }
        Seat seat = seats[seatNo];
        boolean waited = lockSeat(seat);
        try {
            Result r;
            if (seat.owner == EMPTY) {
                assign(seat, clientId);
                r = new Result("SUCCESS", null);
            } else if (seat.owner == clientId) {
                r = new Result("FAIL", "ALREADY_OWNER");
            } else if (seat.isWaiting(clientId)) {
                r = new Result("FAIL", "ALREADY_WAITING");
            } else {
                // 대기열에 넣고 바로 응답 (기다리지 않음)
                seat.waitlist.addLast(new WaitEntry(conn, clientId, reqId, System.currentTimeMillis()));
                r = new Result("WAITLISTED", null);
                r.waitPos = seat.waitlist.size();
                r.holder = seat.owner;
            }
            r.waited = waited;
            return r;
        } finally {
            seat.lock.unlock();
        }
    }

    // 2~4석 예약. 전부 비었을 때만 배정 (대기열 없음)
    static Result reserveMulti(int clientId, int[] requested) {
        if (requested.length < 2 || requested.length > 4) {
            return new Result("FAIL", "BAD_COUNT");
        }
        for (int s : requested) {
            if (s < 1 || s > SEAT_COUNT) {
                return new Result("FAIL", "INVALID_SEAT");
            }
        }
        // deadlock 방지: 항상 좌석 번호 오름차순으로 lock
        int[] order = requested.clone();
        Arrays.sort(order);
        for (int i = 1; i < order.length; i++) {
            if (order[i] == order[i - 1]) {
                return new Result("FAIL", "DUPLICATE_SEAT");
            }
        }

        boolean[] waited = new boolean[order.length];
        int locked = 0;
        try {
            for (int i = 0; i < order.length; i++) {
                waited[i] = lockSeat(seats[order[i]]);
                locked++;
            }
            int taken = 0;
            for (int s : order) {
                if (seats[s].owner != EMPTY) {
                    taken = s;
                    break;
                }
            }
            Result r;
            // 하나라도 차 있으면 아무것도 안 바꿈
            if (taken != 0) {
                r = new Result("FAIL", "TAKEN");
                r.takenSeat = taken;
                r.holder = seats[taken].owner;
            } else {
                for (int s : order) {
                    assign(seats[s], clientId);
                }
                r = new Result("SUCCESS", null);
            }
            r.lockOrder = order;
            r.lockWaited = waited;
            return r;
        } finally {
            // 잡은 lock 전부 해제
            for (int i = locked - 1; i >= 0; i--) {
                seats[order[i]].lock.unlock();
            }
        }
    }

    // 취소: 대기자가 있으면 lock 안에서 바로 넘겨줌 (먼저 온 순서)
    static Result cancel(int clientId, int seatNo) {
        if (seatNo < 1 || seatNo > SEAT_COUNT) {
            return new Result("FAIL", "INVALID_SEAT");
        }
        Seat seat = seats[seatNo];
        boolean waited = lockSeat(seat);
        try {
            Result r;
            if (seat.owner != clientId) {
                r = new Result("FAIL", "NOT_OWNER");
            } else {
                seat.owner = EMPTY;
                released.increment();
                r = new Result("SUCCESS", null);
                WaitEntry head = seat.waitlist.pollFirst();
                if (head != null) {
                    assign(seat, head.clientId());
                    handoffs.increment();
                    r.notifyJob = new Notifier.Job(head, seatNo);
                }
            }
            r.waited = waited;
            return r;
        } finally {
            seat.lock.unlock();
        }
    }

    // tryLock 실패하면 경합 +1 하고 대기
    static boolean lockSeat(Seat seat) {
        if (seat.lock.tryLock()) {
            return false;
        }
        contention.increment();
        seat.lock.lock();
        return true;
    }

    // lock 잡은 상태에서만 호출
    static void assign(Seat seat, int clientId) {
        if (seat.owner != EMPTY) {
            doubleBooking.increment();
        }
        seat.owner = clientId;
        assigned.increment();
    }

    // 좌석 현황 (POOL 로그, 종료 보고서용)
    static Snapshot snapshot() {
        int[] owners = new int[SEAT_COUNT + 1];
        int[] waits = new int[SEAT_COUNT + 1];
        for (int n = 1; n <= SEAT_COUNT; n++) {
            seats[n].lock.lock();
            try {
                owners[n] = seats[n].owner;
                waits[n] = seats[n].waitlist.size();
            } finally {
                seats[n].lock.unlock();
            }
        }
        return new Snapshot(owners, waits);
    }

    // owner, waitlist는 이 좌석 lock 안에서만 접근
    static class Seat {
        final ReentrantLock lock = new ReentrantLock();
        int owner = EMPTY;
        final ArrayDeque<WaitEntry> waitlist = new ArrayDeque<>();

        boolean isWaiting(int clientId) {
            for (WaitEntry e : waitlist) {
                if (e.clientId() == clientId) {
                    return true;
                }
            }
            return false;
        }
    }

    // 대기열 항목
    record WaitEntry(Conn conn, int clientId, int reqId, long registeredAtMillis) {
    }

    // 처리 결과 (Worker가 로그, 응답에 사용)
    static class Result {
        final String result;
        final String reason;
        boolean waited;
        int waitPos;
        int holder;
        int[] lockOrder;  // MULTI: lock 잡은 순서
        boolean[] lockWaited;
        int takenSeat;
        Notifier.Job notifyJob;  // CANCEL로 대기자에게 넘겼을 때

        Result(String result, String reason) {
            this.result = result;
            this.reason = reason;
        }
    }

    record Snapshot(int[] owners, int[] waits) {
        int reservedCount() {
            int c = 0;
            for (int n = 1; n <= SEAT_COUNT; n++) {
                if (owners[n] != EMPTY) {
                    c++;
                }
            }
            return c;
        }

        int waitlistTotal() {
            int c = 0;
            for (int n = 1; n <= SEAT_COUNT; n++) {
                c += waits[n];
            }
            return c;
        }

        // #: 예약, .: 빈 좌석
        String bitmap() {
            StringBuilder sb = new StringBuilder();
            for (int n = 1; n <= SEAT_COUNT; n++) {
                if (n > 1 && (n - 1) % 10 == 0) {
                    sb.append(' ');
                }
                sb.append(owners[n] == EMPTY ? '.' : '#');
            }
            return sb.toString();
        }
    }
}
