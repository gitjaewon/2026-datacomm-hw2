import java.util.Arrays;

// Worker 스레드 (10개). 큐에서 요청을 꺼내 처리하고 응답한다
public class Worker extends Thread {
    private final int id;

    Worker(int id) {
        super("Worker#" + id);
        this.id = id;
    }

    @Override
    public void run() {
        while (true) {
            RequestQueue.Request req;
            try {
                req = Server.requestQueue.take();  // 큐가 비어 있으면 대기
            } catch (InterruptedException e) {
                return;
            }
            // 큐가 닫힘 (서버 종료)
            if (req == null) {
                return;
            }
            handle(req);
        }
    }

    private void handle(RequestQueue.Request req) {
        String who = "Worker#" + id + " Client" + req.clientId() + " req=" + req.reqId();

        SeatManager.Result r;
        try {
            r = judge(req);
        } catch (RuntimeException e) {
            // 예외가 나도 응답은 보냄
            Server.log.console("TERMINATE", "WARN", who + " unexpected error: " + e);
            r = new SeatManager.Result("FAIL", "SERVER_ERROR");
        }

        // 여기서부터는 좌석 lock을 안 잡고 있음
        logResult(req, r, who);
        if (r.notifyJob != null) {
            Server.notifier.enqueue(r.notifyJob);  // NOTIFY는 Notifier가 보냄
        }
        req.conn().send("RESP " + req.reqId() + " " + r.result + (r.reason == null ? "" : " " + r.reason));
        switch (r.result) {
            case "SUCCESS" -> Server.successCount.increment();
            case "WAITLISTED" -> Server.waitlistedCount.increment();
            default -> Server.failCount.increment();
        }

        // 마지막 요청이면 종료 요청
        long done = Server.processed.incrementAndGet();
        Server.lastResponseMillis = System.currentTimeMillis();
        if (done == Server.totalExpected) {
            Listener.requestShutdown("all " + Server.totalExpected + " requests answered");
        }
    }

    private SeatManager.Result judge(RequestQueue.Request req) {
        int[] s = req.seats();
        if (s == null) {
            return new SeatManager.Result("FAIL", "BAD_FORMAT");
        }
        return switch (req.type()) {
            case "RESERVE" -> SeatManager.reserve(req.conn(), req.clientId(), req.reqId(), s[0]);
            case "RESERVE_MULTI" -> SeatManager.reserveMulti(req.clientId(), s);
            default -> SeatManager.cancel(req.clientId(), s[0]);
        };
    }

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
                StringBuilder chain = new StringBuilder();
                for (int i = 0; i < r.lockOrder.length; i++) {
                    if (r.lockWaited[i]) {
                        log.write("LOCK", "INFO", "Worker#" + id + " waited for lock(seat#" + r.lockOrder[i] + ") (contention).");
                    }
                    chain.append(i > 0 ? " -> " : "").append("seat#").append(r.lockOrder[i]);
                }
                log.write("LOCK", "SUCCESS", who + " seats" + seatList(req.seats())
                        + " -> acquired " + chain + " (ascending). No deadlock.");
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

        } else {
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

    private static String seatList(int[] seats) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < seats.length; i++) {
            sb.append(i > 0 ? "," : "").append(seats[i]);
        }
        return sb.append(']').toString();
    }

    private static int[] sorted(int[] seats) {
        int[] copy = seats.clone();
        Arrays.sort(copy);
        return copy;
    }
}
