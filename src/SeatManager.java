import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 좌석 맵(1~100번)과 좌석 판정 규칙 (명세 §0-3).  ← 과제 핵심
 *
 *  - 범위 밖·개수·중복 같은 요청 자체의 오류는 Lock을 잡기 전에 바로 FAIL.
 *  - 그 외 판정은 그 좌석의 Lock을 잡은 상태에서 한다.
 *    "확인(비었나?)"과 "변경(owner = 나)"은 반드시 같은 Lock 안에서 이어서 한다.
 *    Lock 밖에서 확인하고 안에서 변경하면 그 사이에 다른 Worker가 끼어들어 이중예약이 생긴다.
 *  - Lock은 상태를 바꾸는 짧은 순간에만 쥐고, 응답 전송·로그는 Lock을 푼 뒤 Worker가 한다.
 *  - Lock을 여러 개 동시에 잡는 곳은 RESERVE_MULTI 하나뿐이며 항상 좌석 번호 오름차순으로 잡는다.
 */
public class SeatManager {

    static final int SEAT_COUNT = 100;
    static final int EMPTY = 0; // owner 없음

    /** 인덱스 = 좌석 번호 (0번은 안 씀). init()에서 채운다 */
    static final Seat[] seats = new Seat[SEAT_COUNT + 1];

    /**
     * 좌석 맵 초기화 (명세 Step 1). 서버 시작 시 Worker를 띄우기 전에 한 번만 호출한다.
     * 좌석마다 owner = EMPTY, 빈 Waitlist, 그 좌석 전용 Lock 1개가 만들어진다.
     */
    static void init() {
        for (int n = 1; n <= SEAT_COUNT; n++) {
            seats[n] = new Seat();
        }
    }

    // ---- 좌석 집계 ----
    // 여러 Worker가 "서로 다른" 좌석 Lock을 쥔 채 동시에 올리므로 원자적 카운터를 쓴다.
    // 집계 기준: owner가 정해질 때마다 assigned +1, CANCEL 성공마다 released +1
    // (대기자에게 바로 넘기는 경우도 "해제 1 + 배정 1") → 항상 assigned − released = 현재 예약 좌석 수
    static final LongAdder assigned = new LongAdder();
    static final LongAdder released = new LongAdder();
    static final LongAdder handoffs = new LongAdder();      // CANCEL 시 대기자에게 넘긴 횟수
    static final LongAdder doubleBooking = new LongAdder(); // 반드시 0
    static final LongAdder contention = new LongAdder();    // Worker가 좌석 Lock을 기다려야 했던 횟수

    // =====================================================================
    // RESERVE: 범위 밖 → FAIL / EMPTY → SUCCESS / 이미 내 것·이미 대기 중 → FAIL / 남의 것 → WAITLISTED
    // =====================================================================
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
                // 남이 보유 중 → Waitlist 맨 뒤에 등록하고 바로 WAITLISTED.
                // Worker는 여기서 좌석이 빌 때까지 기다리지 않는다 (기다리는 건 대기열 데이터가 대신 한다).
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

    // =====================================================================
    // RESERVE_MULTI (2~4석, All-or-Nothing)
    //   개수·중복·범위 오류 → Lock 전에 FAIL
    //   오름차순으로 Lock 전부 획득 → 전부 EMPTY면 전부 배정, 하나라도 아니면 아무것도 안 바꾸고 FAIL
    //   다중 예약은 Waitlist에 등록하지 않는다.
    // =====================================================================
    static Result reserveMulti(int clientId, int[] requested) {
        // ---- Lock 전 검사 ----
        if (requested.length < 2 || requested.length > 4) {
            return new Result("FAIL", "BAD_COUNT");
        }
        for (int s : requested) {
            if (s < 1 || s > SEAT_COUNT) {
                return new Result("FAIL", "INVALID_SEAT");
            }
        }
        // ---- Lock Ordering: [5,3]으로 오든 [3,5]로 오든 항상 3 → 5 순서로 잡는다 ----
        // 모든 Worker가 같은 순서로 잡으면 "서로 상대 Lock을 기다리는 고리"가 생길 수 없어 Deadlock이 없다.
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
                waited[i] = lockSeat(seats[order[i]]); // 정렬된 순서 그대로, 하나씩
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
            if (taken != 0) {
                // 하나라도 차 있으면 아무 좌석도 바꾸지 않는다 (일부만 예약된 상태가 남으면 안 됨)
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
            // 성공이든 실패든 잡았던 Lock은 전부 푼다 (잡은 역순)
            for (int i = locked - 1; i >= 0; i--) {
                seats[order[i]].lock.unlock();
            }
        }
    }

    // =====================================================================
    // CANCEL: 범위 밖 → FAIL / 내가 owner → SUCCESS / 아니면 FAIL
    //   대기자가 있으면 좌석을 EMPTY로 두지 않고 같은 Lock 안에서 Waitlist 맨 앞에게 바로 넘긴다.
    //   → 그 사이에 다른 Client가 끼어들 수 없고, pollFirst()로 먼저 등록한 사람부터(FIFO) 받는다.
    //   NOTIFY 전송은 Lock을 푼 뒤 Notifier가 한다.
    // =====================================================================
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
                seat.owner = EMPTY; // 해제 1
                released.increment();
                r = new Result("SUCCESS", null);
                WaitEntry head = seat.waitlist.pollFirst();
                if (head != null) {
                    assign(seat, head.clientId()); // 배정 1 (대기 1번에게 즉시)
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

    // =====================================================================
    // 내부 도구
    // =====================================================================

    /**
     * Worker가 좌석 Lock을 잡는다. 먼저 tryLock()으로 바로 얻어 보고,
     * 다른 Worker가 쥐고 있어 실패하면 경합 +1 후 lock()으로 기다린다.
     * (Lock 경합 횟수 측정 방식 = tryLock 실패 횟수)
     *
     * @return 기다려야 했으면 true
     */
    static boolean lockSeat(Seat seat) {
        if (seat.lock.tryLock()) {
            return false;
        }
        contention.increment();
        seat.lock.lock();
        return true;
    }

    /**
     * owner 지정 (배정 1). 반드시 그 좌석의 Lock을 쥔 상태에서 호출한다.
     * 호출부가 이미 EMPTY를 확인했으므로 정상이면 doubleBooking은 절대 오르지 않는다.
     * 그래도 이중예약 0건을 "확인"하기 위해 한 번 더 검사해 센다.
     */
    static void assign(Seat seat, int clientId) {
        if (seat.owner != EMPTY) {
            doubleBooking.increment();
        }
        seat.owner = clientId;
        assigned.increment();
    }

    /**
     * 현재 좌석 현황 (POOL 로그, 종료 보고서용).
     * 좌석을 1번부터 하나씩만 Lock 잡고 읽으므로 Deadlock 위험이 없다.
     * Worker가 아닌 스레드의 접근이라 경합 횟수에는 넣지 않는다.
     */
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

    // =====================================================================
    // 자료형
    // =====================================================================

    /**
     * 좌석 1개. 좌석마다 Lock 1개 (좌석 맵 전체를 Lock 하나로 막는 방식은 금지).
     * ★ owner와 waitlist는 반드시 이 좌석의 lock을 쥔 상태에서만 읽고 쓴다. ★
     * synchronized 대신 ReentrantLock을 쓰는 이유: tryLock()으로 경합 횟수를 셀 수 있어서.
     */
    static class Seat {
        final ReentrantLock lock = new ReentrantLock();
        int owner = EMPTY;
        final ArrayDeque<WaitEntry> waitlist = new ArrayDeque<>(); // 먼저 온 순서(FIFO)

        /** 이 Client가 이 좌석 Waitlist에 이미 있는지 (중복 대기 → FAIL 판정용). 좌석 Lock 안에서만 호출. */
        boolean isWaiting(int clientId) {
            for (WaitEntry e : waitlist) {
                if (e.clientId() == clientId) {
                    return true;
                }
            }
            return false;
        }
    }

    /** Waitlist 대기 1건. registeredAtMillis는 Waitlist 평균 대기시간 계산용 (서버 시계) */
    record WaitEntry(Conn conn, int clientId, int reqId, long registeredAtMillis) {
    }

    /** 판정 결과. Worker가 Lock을 푼 뒤 응답·로그에 쓴다. */
    static class Result {
        final String result;     // SUCCESS / FAIL / WAITLISTED
        final String reason;     // FAIL 사유 (아니면 null)
        boolean waited;          // 단일 좌석: Lock을 기다렸는지
        int waitPos;             // WAITLISTED: 대기 순번
        int holder;              // 좌석을 갖고 있던 Client (로그용)
        int[] lockOrder;         // RESERVE_MULTI: 실제로 Lock을 잡은 순서 (Lock 전 FAIL이면 null)
        boolean[] lockWaited;    // RESERVE_MULTI: 각 좌석에서 기다렸는지
        int takenSeat;           // RESERVE_MULTI: 실패 원인 좌석
        Notifier.Job notifyJob;  // CANCEL: 대기자에게 넘겼으면 통지 작업

        /** 결과 종류와 FAIL 사유로 만든다. 나머지 필드는 판정 함수가 필요할 때 채운다. */
        Result(String result, String reason) {
            this.result = result;
            this.reason = reason;
        }
    }

    /** 좌석 현황 */
    record Snapshot(int[] owners, int[] waits) {
        /** 예약된(owner가 있는) 좌석 수. */
        int reservedCount() {
            int c = 0;
            for (int n = 1; n <= SEAT_COUNT; n++) {
                if (owners[n] != EMPTY) {
                    c++;
                }
            }
            return c;
        }

        /** 모든 좌석의 Waitlist에 남은 대기 건수 합. 종료 시점에는 "미해결 대기 N건"이 된다. */
        int waitlistTotal() {
            int c = 0;
            for (int n = 1; n <= SEAT_COUNT; n++) {
                c += waits[n];
            }
            return c;
        }

        /** 100석을 10개씩 끊어 '#'(예약됨) / '.'(빈 좌석)로 표시 */
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
