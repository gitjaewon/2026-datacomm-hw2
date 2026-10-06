import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Client 1개. 서버와 독립된 TCP 연결 1개를 유지한다. (실행은 ClientMain이 30개를 띄운다)
 *
 * Client 1개는 스레드 2개로 동작한다 (Client 쪽은 스레드 제한 없음)
 *   송신 스레드 : 0.2~1.0초 간격으로 요청을 보낸다. 응답을 기다리지 않고 다음 요청을 보낸다.
 *   수신 스레드 : RESP / NOTIFY / BYE를 받아 상태를 갱신하고 ClientN.txt에 기록한다.
 *
 * 요청 1건의 '처리 완료' = 첫 응답(SUCCESS/FAIL/WAITLISTED) 수신. WAITLISTED 뒤의 NOTIFY는 별도 이벤트.
 * NOTIFY가 WAITLISTED보다 먼저 와도 요청 번호(reqId)로 매칭하므로 문제없다.
 */
public class Client implements Runnable {

    static final int MANY_SEATS = 5; // 보유 좌석이 이 이상이면 CANCEL을 우선 (좌석 고갈 방지)

    // =====================================================================
    // Client 1개의 상태
    //
    //  held       : 확실히 내 것인 좌석 (SUCCESS 또는 NOTIFY로 받음). CANCEL은 여기서만 고른다.
    //  cancelling : CANCEL을 보냈지만 아직 응답이 없는 좌석 → 다시 취소하거나 다시 예약하지 않는다 (pickSeat 참고).
    //  응답이 아직 안 온 예약 좌석은 held에 넣지 않는다 (내 것인지 아직 모르므로).
    //  송신/수신 두 스레드가 같이 쓰므로 모두 lock으로 보호한다.
    // =====================================================================

    final int id;
    final Log log;
    final Random rnd = new Random();
    Socket socket;
    OutputStream out; // 송신 스레드만 쓴다

    final ReentrantLock lock = new ReentrantLock();
    final Condition progress = lock.newCondition();
    final Map<Integer, Pending> pending = new HashMap<>(); // 첫 응답을 기다리는 요청 (reqId → 정보)
    final TreeSet<Integer> held = new TreeSet<>();
    final Set<Integer> cancelling = new HashSet<>();
    int sent, responded, success, fail, waitlisted, notified;
    long respNanosSum;
    boolean connectionEnded; // 수신 스레드가 끝났는지 (BYE 또는 연결 끊김)
    boolean byeReceived;

    /** 첫 응답을 기다리는 요청 정보 */
    record Pending(String type, int[] seats, long sentNanos) {
    }

    /** Client 1개를 만든다. 자기 로그 파일(ClientN.txt)만 열고, 서버 접속은 run()에서 한다. */
    Client(int id) throws IOException {
        this.id = id;
        this.log = new Log("CLIENT" + id, Paths.get(ClientMain.logDir, "Client" + id + ".txt"));
    }

    // =====================================================================
    // 송신 스레드
    // =====================================================================

    /**
     * 송신 스레드 본체 (Client 1개의 전체 흐름).
     * 접속 → HELLO → 요청 N건을 0.2~1.0초 간격으로 전송 → 첫 응답을 모두 받을 때까지 대기
     * → 서버 종료 신호(BYE)까지 연결 유지 → 종료 로그(최종 보유 좌석) 기록.
     */
    @Override
    public void run() {
        try {
            connect();
        } catch (IOException e) {
            log.write("CONNECT", "FAIL", "Cannot connect to server: " + e.getMessage());
            System.err.println("Client" + id + " cannot connect to server: " + e.getMessage());
            markEnded();
            log.close();
            return;
        }
        Thread receiver = new Thread(this::receiveLoop, "Client" + id + "-recv");
        receiver.start();

        try {
            sendLine("HELLO " + id);
            log.write("CONNECT", "SUCCESS", "Connected to server, HELLO sent.");

            for (int reqId = 1; reqId <= ClientMain.requests; reqId++) {
                String line = planAndRegister(reqId); // 다음 요청을 고르고 pending에 먼저 등록
                if (line == null) {
                    break; // 연결이 이미 끊김
                }
                sendLine(line);
                logSent(reqId, line);
                Thread.sleep(ClientMain.minIntervalMs + rnd.nextInt(ClientMain.maxIntervalMs - ClientMain.minIntervalMs + 1));
            }
            // 모두 보냈으면 첫 응답을 전부 받을 때까지 기다린다
            awaitAllResponses();
            // 그 뒤 서버 종료 신호(BYE)까지 연결 유지 (그동안 오는 NOTIFY도 수신 스레드가 기록)
            receiver.join();
        } catch (IOException e) {
            log.write("CONNECT", "WARN", "Send failed, connection lost: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            try {
                socket.close();
                receiver.join(2000);
            } catch (IOException | InterruptedException ignored) {
                // 종료 중
            }
            logTerminate();
            log.close();
        }
    }

    /** 서버가 늦게 떠도 되도록 1초 간격으로 최대 30번 접속 시도 */
    void connect() throws IOException {
        IOException last = null;
        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                Socket s = new Socket();
                s.setTcpNoDelay(true);
                s.connect(new InetSocketAddress(ClientMain.host, ClientMain.port), 5000);
                socket = s;
                out = s.getOutputStream();
                return;
            } catch (IOException e) {
                last = e;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    throw e;
                }
            }
        }
        throw last;
    }

    /** 서버로 메시지 한 줄을 보낸다 (끝에 줄바꿈을 붙임). 송신 스레드만 부르므로 Lock이 필요 없다. */
    void sendLine(String line) throws IOException {
        out.write((line + "\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    /**
     * 다음 요청을 고르고, 보내기 "전에" pending에 등록한다.
     * (먼저 보내고 나중에 등록하면 응답이 등록보다 먼저 도착해 매칭에 실패할 수 있다)
     *
     * 요청 비율 (명세 참고 권장안)
     *   보유 좌석 없음 : RESERVE 60% / RESERVE_MULTI 40%
     *   보유 좌석 있음 : RESERVE 30% / RESERVE_MULTI 20% / CANCEL 50%
     *   보유 좌석 MANY_SEATS개 이상 : CANCEL 우선
     *
     * @return 보낼 메시지. 연결이 끊겼으면 null
     */
    String planAndRegister(int reqId) {
        lock.lock();
        try {
            if (connectionEnded) {
                return null;
            }
            List<Integer> cancellable = new ArrayList<>();
            for (int s : held) {
                if (!cancelling.contains(s)) {
                    cancellable.add(s);
                }
            }
            int r = rnd.nextInt(100);
            String type;
            if (cancellable.isEmpty()) {
                type = r < 60 ? "RESERVE" : "RESERVE_MULTI";
            } else if (cancellable.size() >= MANY_SEATS || r < 50) {
                type = "CANCEL";
            } else {
                type = r < 80 ? "RESERVE" : "RESERVE_MULTI";
            }

            int[] seats;
            String line;
            switch (type) {
                case "CANCEL" -> {
                    int seat = cancellable.get(rnd.nextInt(cancellable.size()));
                    cancelling.add(seat);
                    seats = new int[] {seat};
                    line = "CANCEL " + reqId + " " + seat;
                }
                case "RESERVE_MULTI" -> {
                    seats = pickDistinctSeats(2 + rnd.nextInt(3)); // 2~4석, 무작위 순서 그대로 보냄
                    line = "RESERVE_MULTI " + reqId + " " + joinSeats(seats);
                }
                default -> {
                    seats = new int[] {pickSeat()};
                    line = "RESERVE " + reqId + " " + seats[0];
                }
            }
            pending.put(reqId, new Pending(type, seats, System.nanoTime()));
            sent++;
            return line;
        } finally {
            lock.unlock();
        }
    }

    /**
     * hotRatio 확률로 인기 좌석(1~hotSeats), 나머지는 그 밖의 좌석. lock 안에서 호출.
     *
     * 단, CANCEL 응답을 아직 못 받은 좌석(cancelling)은 고르지 않는다.
     * 예) CANCEL 72 를 보내고 곧바로 RESERVE_MULTI [72,64] 를 보내면, 서버에서는 둘 다 성공할 수 있는데
     *     두 응답을 서로 다른 Worker가 보내므로 MULTI SUCCESS가 먼저, CANCEL SUCCESS가 나중에 도착할 수 있다.
     *     그러면 Client는 "72 추가 → 72 삭제" 순으로 처리해 실제로는 갖고 있는 좌석을 잃어버린 것으로 착각한다.
     *     취소 중인 좌석을 다시 요청하지 않으면 같은 좌석의 "추가"와 "삭제"가 동시에 진행될 일이 없다.
     */
    int pickSeat() {
        while (true) {
            int seat = rnd.nextDouble() < ClientMain.hotRatio
                    ? 1 + rnd.nextInt(ClientMain.hotSeats)
                    : ClientMain.hotSeats + 1 + rnd.nextInt(100 - ClientMain.hotSeats);
            if (!cancelling.contains(seat)) {
                return seat;
            }
        }
    }

    /**
     * RESERVE_MULTI용으로 서로 다른 좌석 count개를 고른다.
     * 정렬하지 않고 뽑힌 순서 그대로 보낸다 → 서버가 직접 오름차순 정렬(Lock Ordering)하는지 확인하는 용도.
     */
    int[] pickDistinctSeats(int count) {
        Set<Integer> picked = new LinkedHashSet<>();
        while (picked.size() < count) {
            picked.add(pickSeat());
        }
        return picked.stream().mapToInt(Integer::intValue).toArray();
    }

    /**
     * 보낸 요청의 첫 응답을 전부 받을 때까지 Condition Variable에서 기다린다.
     * 연결이 끊기면(connectionEnded) 바로 빠져나온다.
     */
    void awaitAllResponses() throws InterruptedException {
        lock.lock();
        try {
            while (responded < sent && !connectionEnded) {
                progress.await();
            }
        } finally {
            lock.unlock();
        }
    }

    // =====================================================================
    // 수신 스레드
    // =====================================================================

    /**
     * 수신 스레드 본체. 서버 메시지를 한 줄씩 읽어 종류별로 나눈다.
     * RESP → onResp(), NOTIFY → onNotify(), BYE → 수신 종료.
     */
    void receiveLoop() {
        // readLine()이 '\n' 단위로 잘라 주므로 메시지가 붙어 오거나 나뉘어 와도 한 줄씩 정확히 받는다
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] t = line.trim().split("\\s+");
                switch (t[0]) {
                    case "RESP" -> onResp(t, line);
                    case "NOTIFY" -> onNotify(t, line);
                    case "BYE" -> {
                        lock.lock();
                        try {
                            byeReceived = true;
                        } finally {
                            lock.unlock();
                        }
                        return;
                    }
                    default -> log.write("CONNECT", "WARN", "Unknown message from server: \"" + line + "\".");
                }
            }
            log.write("CONNECT", "WARN", "Server closed the connection without termination signal.");
        } catch (IOException e) {
            log.write("CONNECT", "WARN", "Receive stopped: " + e.getMessage());
        } finally {
            markEnded();
        }
    }

    /** RESP <reqId> <result> [reason] */
    void onResp(String[] t, String raw) {
        int reqId;
        try {
            reqId = Integer.parseInt(t[1]);
        } catch (RuntimeException e) {
            log.write("CONNECT", "WARN", "Malformed RESP: \"" + raw + "\".");
            return;
        }
        String result = t.length > 2 ? t[2] : "";
        String reason = t.length > 3 ? t[3] : null;

        Pending p;
        long rtMs;
        lock.lock();
        try {
            p = pending.remove(reqId);
            if (p == null) {
                log.write("CONNECT", "WARN", "RESP for unknown req=" + reqId + " ignored.");
                return;
            }
            long rt = System.nanoTime() - p.sentNanos();
            rtMs = rt / 1_000_000;
            respNanosSum += rt;
            responded++;
            int first = p.seats()[0];
            switch (result) {
                case "SUCCESS" -> {
                    success++;
                    if (p.type().equals("CANCEL")) {
                        cancelling.remove(first);
                        held.remove(first);
                    } else {
                        for (int s : p.seats()) {
                            held.add(s);
                        }
                    }
                }
                case "WAITLISTED" -> waitlisted++;
                default -> {
                    fail++;
                    if (p.type().equals("CANCEL")) {
                        // 취소 실패 = 서버 기준으로 내 좌석이 아님 → 내 상태를 서버에 맞춘다
                        cancelling.remove(first);
                        held.remove(first);
                    }
                }
            }
            if (responded >= sent) {
                progress.signalAll(); // 다 받기를 기다리는 송신 스레드를 깨운다
            }
        } finally {
            lock.unlock();
        }

        String seatText = p.type().equals("RESERVE_MULTI") ? "seats[" + joinSeats(p.seats()) + "]" : "seat#" + p.seats()[0];
        switch (result) {
            case "SUCCESS" -> log.write(p.type(), "SUCCESS", "req=" + reqId + " SUCCESS " + seatText + ". resp_time=" + rtMs + "ms.");
            case "WAITLISTED" -> log.write("WAITLIST", "WARN", "req=" + reqId + " WAITLISTED " + seatText + ". resp_time=" + rtMs + "ms.");
            default -> log.write(p.type(), "FAIL", "req=" + reqId + " FAIL " + seatText + " reason=" + reason + ". resp_time=" + rtMs + "ms.");
        }
    }

    /** NOTIFY <reqId> <seat>: Waitlist에 있던 요청에 좌석이 배정됨 */
    void onNotify(String[] t, String raw) {
        int reqId;
        int seat;
        try {
            reqId = Integer.parseInt(t[1]);
            seat = Integer.parseInt(t[2]);
        } catch (RuntimeException e) {
            log.write("CONNECT", "WARN", "Malformed NOTIFY: \"" + raw + "\".");
            return;
        }
        lock.lock();
        try {
            held.add(seat);
            notified++;
        } finally {
            lock.unlock();
        }
        log.write("NOTIFY", "SUCCESS", "req=" + reqId + " seat#" + seat + " assigned from waitlist.");
    }

    /** 수신이 끝났음(BYE 또는 연결 끊김)을 표시하고, 응답을 기다리며 자고 있는 송신 스레드를 깨운다. */
    void markEnded() {
        lock.lock();
        try {
            connectionEnded = true;
            progress.signalAll();
        } finally {
            lock.unlock();
        }
    }

    // =====================================================================
    // 로그
    // =====================================================================

    /** 요청을 보낸 직후 ClientN.txt에 "req=.. sent .." 를 기록한다. */
    void logSent(int reqId, String line) {
        String[] t = line.split(" ");
        String what = t[0].equals("RESERVE_MULTI") ? "seats[" + t[2] + "]" : "seat#" + t[2];
        log.write(t[0], "INFO", "req=" + reqId + " sent " + what + ".");
    }

    /**
     * 종료 로그. final_held(최종 보유 좌석)는 Verify.java가 서버 최종 좌석 현황과 대조한다.
     * unresolved = WAITLISTED를 받았지만 NOTIFY를 못 받은 수 (종료 시 미해결 대기)
     */
    void logTerminate() {
        lock.lock();
        try {
            String msg = String.format(
                    "sent=%d responded=%d final_held=%s success=%d fail=%d waitlisted=%d notified=%d unresolved=%d avg_resp_ms=%.1f",
                    sent, responded, held.toString().replace(" ", ""), success, fail, waitlisted, notified,
                    waitlisted - notified, responded == 0 ? 0 : respNanosSum / (double) responded / 1e6);
            if (byeReceived) {
                log.write("TERMINATE", "SUCCESS", "Termination signal received. " + msg + ".");
            } else {
                log.write("TERMINATE", "WARN", "Connection ended without termination signal. " + msg + ".");
            }
        } finally {
            lock.unlock();
        }
    }

    /** 지금까지 받은 첫 응답 수. main의 10초 진행률 출력에 쓴다. */
    int respondedSoFar() {
        lock.lock();
        try {
            return responded;
        } finally {
            lock.unlock();
        }
    }

    /** {5,3} → "5,3" (메시지·로그용). */
    static String joinSeats(int[] seats) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < seats.length; i++) {
            sb.append(i > 0 ? "," : "").append(seats[i]);
        }
        return sb.toString();
    }
}
