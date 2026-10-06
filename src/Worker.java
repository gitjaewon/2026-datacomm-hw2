import java.util.Arrays;

/**
 * Worker Thread. 서버 시작 시 정확히 10개를 만들어 서버가 끝날 때까지 재사용한다 (고정 Thread Pool).
 *
 * 한 Worker가 반복하는 일 (명세 §0-7)
 *   ① Request Queue에서 요청 1건 꺼내기 (비어 있으면 Condition Variable에서 잠듦)
 *   ② 좌석 Lock 잡고 판정 → ③ 상태 변경 → Lock 해제          (SeatManager 함수 안에서)
 *   ④ 로그 → (대기자에게 넘겼으면 Notify Queue에 넣기) → 응답 전송   (Lock을 모두 푼 뒤)
 *
 * ④를 Lock을 푼 뒤에 하므로, 응답을 보내는 동안 다른 Worker가 그 좌석을 기다리지 않는다.
 */
public class Worker extends Thread {

    private final int id;

    /** 스레드 이름을 "Worker#id"로 정한다. 실제 실행은 Server.main의 start()에서 시작된다. */
    Worker(int id) {
        super("Worker#" + id);
        this.id = id;
    }

    /**
     * Worker 스레드 본체 (무한 루프). 큐에서 요청을 꺼내(없으면 잠듦) handle()로 처리한다.
     * take()가 null을 돌려주면(큐 닫힘) 끝난다.
     */
    @Override
    public void run() {
        while (true) {
            RequestQueue.Request req;
            try {
                req = Server.requestQueue.take(); // ① 비어 있으면 여기서 잠든다
            } catch (InterruptedException e) {
                return;
            }
            if (req == null) {
                return; // 큐가 닫혔고 남은 요청 없음 → 종료
            }
            handle(req);
        }
    }

    /**
     * 요청 1건 처리 전체: 좌석 판정(Lock 안) → 로그 → 통지 넘기기 → 응답 전송 → 통계
     * → 마지막(150,000번째) 응답이면 서버 종료 요청.
     */
    private void handle(RequestQueue.Request req) {
        String who = "Worker#" + id + " Client" + req.clientId() + " req=" + req.reqId();
        logReceived(req, who);

        // ②③ 판정 (좌석 Lock은 이 호출 안에서만 잡혔다가 모두 풀린다)
        SeatManager.Result r;
        try {
            r = judge(req);
        } catch (RuntimeException e) {
            // 예상 못 한 오류가 나도 응답은 반드시 보낸다 (안 보내면 Client와 서버 종료가 멈춘다)
            Server.log.console("TERMINATE", "WARN", who + " unexpected error: " + e);
            r = new SeatManager.Result("FAIL", "SERVER_ERROR");
        }

        // ④ 여기부터는 좌석 Lock을 하나도 쥐고 있지 않다.
        // 결과 로그를 통지보다 먼저 써야 Server.txt에서 CANCEL 줄이 그에 따른 NOTIFY 줄보다 앞에 온다.
        logResult(req, r, who);
        if (r.notifyJob != null) {
            Server.notifier.enqueue(r.notifyJob); // 통지는 Notifier 몫. Worker는 기다리지 않는다
        }
        req.conn().send("RESP " + req.reqId() + " " + r.result + (r.reason == null ? "" : " " + r.reason));
        switch (r.result) {
            case "SUCCESS" -> Server.successCount.increment();
            case "WAITLISTED" -> Server.waitlistedCount.increment();
            default -> Server.failCount.increment();
        }

        // 첫 응답 누적 수가 정확히 totalExpected가 되는 순간 한 번만 종료를 요청한다
        long done = Server.processed.incrementAndGet();
        Server.lastResponseMillis = System.currentTimeMillis();
        if (done == Server.totalExpected) {
            Listener.requestShutdown("all " + Server.totalExpected + " requests answered");
        }
    }

    /** 요청 종류에 따라 SeatManager의 reserve / reserveMulti / cancel 중 하나를 부른다. */
    private SeatManager.Result judge(RequestQueue.Request req) {
        int[] s = req.seats();
        if (s == null) {
            return new SeatManager.Result("FAIL", "BAD_FORMAT"); // 좌석을 숫자로 못 읽은 요청
        }
        return switch (req.type()) {
            case "RESERVE" -> SeatManager.reserve(req.conn(), req.clientId(), req.reqId(), s[0]);
            case "RESERVE_MULTI" -> SeatManager.reserveMulti(req.clientId(), s);
            default -> SeatManager.cancel(req.clientId(), s[0]); // CANCEL
        };
    }

    // ---------------------------------------------------------------------
    // 로그
    // ---------------------------------------------------------------------

    /** 요청을 꺼낸 직후 Server.txt에 "누가 어떤 좌석을 요청했는지" INFO 로그를 남긴다. */
    private void logReceived(RequestQueue.Request req, String who) {
        int[] s = req.seats();
        if (s == null) {
            Server.log.write(req.type(), "INFO", who + " malformed request.");
        } else if (req.type().equals("RESERVE_MULTI")) {
            Server.log.write("RESERVE_MULTI", "INFO", who + " seats" + seatList(s)
                    + ". Lock order -> " + seatList(sorted(s)) + ".");
        } else {
            Server.log.write(req.type(), "INFO", who + " seat#" + s[0] + ".");
        }
    }

    /** 판정 결과를 Server.txt에 기록한다. 요청 종류별로 메시지가 다르고, RESERVE_MULTI는 Lock 획득 순서(LOCK 로그)도 남긴다. */
    private void logResult(RequestQueue.Request req, SeatManager.Result r, String who) {
        Log log = Server.log;
        String type = req.type();
        if (req.seats() == null) {
            log.write(type, "FAIL", who + " rejected: " + r.reason + ".");
            return;
        }
        int seat = req.seats()[0];

        if (type.equals("RESERVE")) {
            switch (r.result) {
                case "SUCCESS" -> log.write("RESERVE", "SUCCESS", "Worker#" + id + " acquired lock(seat#" + seat + ")"
                        + (r.waited ? " after waiting (contention)" : " first")
                        + " -> seat#" + seat + " assigned to Client" + req.clientId() + " (req=" + req.reqId() + ").");
                case "WAITLISTED" -> log.write("WAITLIST", "SUCCESS", "Worker#" + id + " seat#" + seat
                        + " already taken by Client" + r.holder + " -> Client" + req.clientId()
                        + " req=" + req.reqId() + " registered (pos=" + r.waitPos + ").");
                default -> log.write("RESERVE", "FAIL", who + " seat#" + seat + " rejected: " + r.reason + ".");
            }

        } else if (type.equals("RESERVE_MULTI")) {
            if (r.lockOrder != null) {
                // 실제로 Lock을 잡은 순서를 남긴다 (Lock Ordering 증거)
                StringBuilder chain = new StringBuilder();
                for (int i = 0; i < r.lockOrder.length; i++) {
                    if (r.lockWaited[i]) {
                        log.write("LOCK", "INFO", "Worker#" + id + " waited for lock(seat#" + r.lockOrder[i] + ") (contention).");
                    }
                    chain.append(i > 0 ? " -> " : "").append("seat#").append(r.lockOrder[i]);
                }
                log.write("LOCK", "SUCCESS", "Worker#" + id + " acquired " + chain + " (ascending). No deadlock.");
            }
            if (r.result.equals("SUCCESS")) {
                log.write("RESERVE_MULTI", "SUCCESS", who + " seats" + seatList(sorted(req.seats())) + " assigned (atomic).");
            } else if (r.lockOrder != null) {
                log.write("RESERVE_MULTI", "FAIL", who + " rejected: seat#" + r.takenSeat + " already taken by Client"
                        + r.holder + " (all-or-nothing, nothing changed).");
            } else {
                log.write("RESERVE_MULTI", "FAIL", who + " seats" + seatList(req.seats())
                        + " rejected before locking: " + r.reason + ".");
            }

        } else { // CANCEL
            if (!r.result.equals("SUCCESS")) {
                log.write("CANCEL", "FAIL", who + " seat#" + seat + " rejected: " + r.reason + ".");
            } else if (r.notifyJob == null) {
                log.write("CANCEL", "SUCCESS", who + " canceled seat#" + seat + ". Seat is now EMPTY.");
            } else {
                SeatManager.WaitEntry next = r.notifyJob.entry();
                log.write("CANCEL", "SUCCESS", who + " canceled seat#" + seat + ". Assigned to waitlist head Client"
                        + next.clientId() + " (req=" + next.reqId() + ").");
            }
        }
    }

    /** {5,3} → "[5,3]" */
    private static String seatList(int[] seats) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < seats.length; i++) {
            sb.append(i > 0 ? "," : "").append(seats[i]);
        }
        return sb.append(']').toString();
    }

    /** 오름차순 정렬한 복사본 (로그 출력용, 원본은 안 바꿈). */
    private static int[] sorted(int[] seats) {
        int[] copy = seats.clone();
        Arrays.sort(copy);
        return copy;
    }
}
