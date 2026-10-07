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

// Client 1개. 송신 스레드(run)와 수신 스레드(receiveLoop)로 동작
public class Client implements Runnable {
    static final int MANY_SEATS = 5;  // 보유 좌석이 이만큼 되면 취소 위주

    final int id;
    final Log log;
    final Random rnd = new Random();
    Socket socket;
    OutputStream out;

    // 아래 상태는 송신/수신 스레드가 같이 쓰므로 lock으로 보호
    final ReentrantLock lock = new ReentrantLock();
    final Condition progress = lock.newCondition();
    final Map<Integer, Pending> pending = new HashMap<>();  // 응답 기다리는 요청
    final Map<Integer, Integer> waiting = new HashMap<>();  // WAITLISTED를 받은 reqId -> 좌석
    final Set<Integer> notifiedBeforeResponse = new HashSet<>();
    final TreeSet<Integer> held = new TreeSet<>();  // 내 좌석
    final Set<Integer> cancelling = new HashSet<>();  // 취소 응답 기다리는 좌석
    int sent, responded, success, fail, waitlisted, notified, protocolErrors;
    long respNanosSum;
    boolean connectionEnded;
    boolean byeReceived;

    // 보낸 요청 정보
    record Pending(String type, int[] seats, long sentNanos) {
    }

    Client(int id) throws IOException {
        this.id = id;
        this.log = new Log("CLIENT" + id, Paths.get(ClientMain.logDir, "Client" + id + ".txt"));
    }

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
        // 수신 스레드 시작
        Thread receiver = new Thread(this::receiveLoop, "Client" + id + "-recv");
        receiver.start();

        try {
            sendLine("HELLO " + id);
            log.write("CONNECT", "SUCCESS", "Connected to server, HELLO sent.");

            for (int reqId = 1; reqId <= ClientMain.requests; reqId++) {
                String line = planAndRegister(reqId);  // 보내기 전에 pending에 먼저 등록
                if (line == null) {
                    break;
                }
                logSent(reqId, line);
                sendLine(line);
                Thread.sleep(ClientMain.minIntervalMs + rnd.nextInt(ClientMain.maxIntervalMs - ClientMain.minIntervalMs + 1));
            }
            // 다 보냈으면 응답이 다 올 때까지 기다린 뒤 BYE 대기
            awaitAllResponses();
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
            }
            logTerminate();
            log.close();
        }
    }

    // 서버가 늦게 켜질 수 있어서 30번까지 재시도
    void connect() throws IOException {
        IOException last = null;
        for (int attempt = 0; attempt < 30; attempt++) {
            Socket s = new Socket();
            try {
                s.setTcpNoDelay(true);
                s.connect(new InetSocketAddress(ClientMain.host, ClientMain.port), 5000);
                socket = s;
                out = s.getOutputStream();
                return;
            } catch (IOException e) {
                s.close();
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

    void sendLine(String line) throws IOException {
        out.write((line + "\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    // 좌석 없으면 RESERVE 60 / MULTI 40, 있으면 30 / 20 / CANCEL 50
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
                    seats = pickDistinctSeats(2 + rnd.nextInt(3));
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

    // 인기 좌석(1~10) 위주로 고름. 취소 중인 좌석은 제외 (응답 순서가 꼬일 수 있어서)
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

    // 다중 예약용. 정렬 안 하고 보냄
    int[] pickDistinctSeats(int count) {
        Set<Integer> picked = new LinkedHashSet<>();
        while (picked.size() < count) {
            picked.add(pickSeat());
        }
        return picked.stream().mapToInt(Integer::intValue).toArray();
    }

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

    // 서버 메시지 받기 (RESP / NOTIFY / BYE)
    void receiveLoop() {
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

    void onResp(String[] t, String raw) {
        int reqId;
        try {
            reqId = Integer.parseInt(t[1]);
        } catch (RuntimeException e) {
            protocolError("Malformed RESP: \"" + raw + "\".");
            return;
        }
        String result = t.length > 2 ? t[2] : "";
        String reason = t.length > 3 ? t[3] : null;
        if ((!result.equals("SUCCESS") && !result.equals("FAIL") && !result.equals("WAITLISTED"))
                || (result.equals("FAIL") ? t.length < 3 || t.length > 4 : t.length != 3)) {
            protocolError("Malformed RESP: \"" + raw + "\".");
            return;
        }

        Pending p;
        long rtMs;
        lock.lock();
        try {
            p = pending.get(reqId);
            if (p == null) {
                protocolError("RESP for unknown req=" + reqId + " ignored.");
                return;
            }
            if (result.equals("WAITLISTED") && !p.type().equals("RESERVE")) {
                protocolError("WAITLISTED for non-RESERVE req=" + reqId + " ignored.");
                return;
            }
            if (notifiedBeforeResponse.contains(reqId) && !result.equals("WAITLISTED")) {
                protocolError("RESP contradicts earlier NOTIFY for req=" + reqId + " ignored.");
                return;
            }
            pending.remove(reqId);
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
                case "WAITLISTED" -> {
                    waitlisted++;
                    if (!notifiedBeforeResponse.remove(reqId)) {
                        waiting.put(reqId, first);
                    }
                }
                default -> {
                    fail++;
                    if (p.type().equals("CANCEL")) {
                        cancelling.remove(first);
                    }
                }
            }
            // 다 받았으면 송신 스레드 깨움
            if (responded >= sent) {
                progress.signalAll();
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

    // 대기하던 좌석을 받음
    void onNotify(String[] t, String raw) {
        int reqId;
        int seat;
        try {
            reqId = Integer.parseInt(t[1]);
            seat = Integer.parseInt(t[2]);
            if (t.length != 3 || seat < 1 || seat > SeatManager.SEAT_COUNT) {
                throw new IllegalArgumentException();
            }
        } catch (RuntimeException e) {
            protocolError("Malformed NOTIFY: \"" + raw + "\".");
            return;
        }
        lock.lock();
        try {
            Integer expectedSeat = waiting.get(reqId);
            Pending firstResponse = pending.get(reqId);
            if (expectedSeat == null && firstResponse != null && firstResponse.type().equals("RESERVE")) {
                expectedSeat = firstResponse.seats()[0];
            }
            if (expectedSeat == null || expectedSeat != seat || notifiedBeforeResponse.contains(reqId)) {
                protocolError("Unmatched or duplicate NOTIFY: \"" + raw + "\" ignored.");
                return;
            }
            if (waiting.remove(reqId) == null) {
                notifiedBeforeResponse.add(reqId);
            }
            held.add(seat);
            notified++;
        } finally {
            lock.unlock();
        }
        log.write("NOTIFY", "SUCCESS", "req=" + reqId + " seat#" + seat + " assigned from waitlist.");
    }

    void protocolError(String message) {
        lock.lock();
        try {
            protocolErrors++;
        } finally {
            lock.unlock();
        }
        log.write("CONNECT", "WARN", message);
    }

    void markEnded() {
        lock.lock();
        try {
            connectionEnded = true;
            progress.signalAll();
        } finally {
            lock.unlock();
        }
    }

    void logSent(int reqId, String line) {
        String[] t = line.split(" ");
        String what = t[0].equals("RESERVE_MULTI") ? "seats[" + t[2] + "]" : "seat#" + t[2];
        log.write(t[0], "INFO", "req=" + reqId + " sent " + what + ".");
    }

    // 종료 로그 (final_held = 최종 보유 좌석)
    void logTerminate() {
        lock.lock();
        try {
            String msg = String.format(
                    "sent=%d responded=%d final_held=%s success=%d fail=%d waitlisted=%d notified=%d unresolved=%d avg_resp_ms=%.1f protocol_errors=%d",
                    sent, responded, held.toString().replace(" ", ""), success, fail, waitlisted, notified,
                    waiting.size(), responded == 0 ? 0 : respNanosSum / (double) responded / 1e6, protocolErrors);
            if (byeReceived && sent == ClientMain.requests && responded == sent && protocolErrors == 0) {
                log.write("TERMINATE", "SUCCESS", "Termination signal received. " + msg + ".");
            } else {
                log.write("TERMINATE", "WARN", (byeReceived ? "Termination signal received but run incomplete. "
                        : "Connection ended without termination signal. ") + msg + ".");
            }
        } finally {
            lock.unlock();
        }
    }

    int respondedSoFar() {
        lock.lock();
        try {
            return responded;
        } finally {
            lock.unlock();
        }
    }

    static String joinSeats(int[] seats) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < seats.length; i++) {
            sb.append(i > 0 ? "," : "").append(seats[i]);
        }
        return sb.toString();
    }
}
