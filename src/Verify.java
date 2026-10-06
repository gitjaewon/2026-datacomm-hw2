import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 부하 테스트가 끝난 뒤 Server.txt 와 Client1.txt ~ ClientN.txt 를 읽어
 * 이중예약 0건과 최종 좌석 정합성(명세 §4-2)을 확인하는 도구.
 *
 * 실행 예:
 *   java -cp out Verify --log-dir logs --clients 30 --requests 5000
 *
 * 확인 항목
 *   1. 서버 이중예약 카운터 = 0
 *   2. 배정 수 − 해제 수 = 종료 시점 예약된 좌석 수 (= 서버 최종 좌석 현황의 예약 좌석 수)
 *   3. 서버 최종 좌석 현황 = 30개 Client의 최종 보유 좌석 목록 (좌석별 owner, 좌석 수 합)
 *      + 한 좌석이 두 Client의 final_held에 동시에 들어 있지 않음
 *   4. 전체 WAITLISTED 응답 수(Client 합) = 전체 NOTIFY 수신 수(Client 합) + 종료 시 미해결 대기 수(서버)
 *   5. 각 Client가 요청을 모두 보내고 첫 응답을 모두 받았는지, 종료 신호를 받았는지
 * 결과는 화면과 log-dir/VerifyResult.txt 에 쓴다.
 */
public final class Verify {

    private static final Pattern SEAT_OWNER = Pattern.compile("(\\d+)=(EMPTY|Client(\\d+))");
    private static final Pattern KEY_NUM = Pattern.compile("([a-z_]+)=([0-9]+(?:\\.[0-9]+)?)");
    private static final Pattern FINAL_HELD = Pattern.compile("final_held=\\[([0-9,]*)]");

    private final StringBuilder report = new StringBuilder();
    private boolean allPass = true;

    /** 실행 인자(--log-dir, --clients, --requests)를 읽고 검증을 실행한다. */
    public static void main(String[] args) throws IOException {
        String dir = "logs";
        int clients = 30;
        int requests = 5000;
        for (int i = 0; i + 1 < args.length; i += 2) {
            switch (args[i]) {
                case "--log-dir" -> dir = args[i + 1];
                case "--clients" -> clients = Integer.parseInt(args[i + 1]);
                case "--requests" -> requests = Integer.parseInt(args[i + 1]);
                default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
        new Verify().run(Paths.get(dir), clients, requests);
    }

    /** Server.txt와 Client1~N.txt를 읽어 정합성 5개 항목을 판정하고, Readme용 결과표를 출력한다. */
    private void run(Path dir, int clients, int requests) throws IOException {
        // ---------------- Server.txt ----------------
        List<String> serverLines = Files.readAllLines(dir.resolve("Server.txt"), StandardCharsets.UTF_8);
        int[] serverOwner = new int[101]; // 0 = EMPTY
        int seatsFound = 0;
        Map<String, Double> check = Map.of();
        Map<String, Double> pendingLine = Map.of();
        Map<String, Double> metrics = Map.of();
        for (String line : serverLines) {
            if (line.contains("Final seat map")) {
                Matcher m = SEAT_OWNER.matcher(line.substring(line.indexOf(':', line.indexOf("Final seat map")) + 1));
                while (m.find()) {
                    int seat = Integer.parseInt(m.group(1));
                    serverOwner[seat] = m.group(3) == null ? 0 : Integer.parseInt(m.group(3));
                    seatsFound++;
                }
            } else if (line.contains("| DOUBLE_BOOKING_CHECK |")) {
                check = keyValues(line);
            } else if (line.contains("pending_waitlist=")) {
                pendingLine = keyValues(line);
            } else if (line.contains("Metrics:")) {
                metrics = keyValues(line);
            }
        }
        if (seatsFound != 100 || check.isEmpty() || pendingLine.isEmpty()) {
            out("Server.txt has no final report. Check that the server finished with graceful termination.");
            finish(dir);
            return;
        }
        int serverReserved = 0;
        for (int s = 1; s <= 100; s++) {
            if (serverOwner[s] != 0) {
                serverReserved++;
            }
        }

        // ---------------- Client1.txt ~ ClientN.txt ----------------
        int[] clientOwner = new int[101];
        long sumHeld = 0, sumWaitlisted = 0, sumNotified = 0, sumSuccess = 0, sumFail = 0, sumResponded = 0;
        double respWeighted = 0;
        boolean everyClientDone = true;
        boolean noSeatInTwoClients = true;
        for (int id = 1; id <= clients; id++) {
            Path f = dir.resolve("Client" + id + ".txt");
            if (!Files.exists(f)) {
                out("  Client" + id + ".txt not found");
                everyClientDone = false;
                continue;
            }
            String term = null;
            for (String line : Files.readAllLines(f, StandardCharsets.UTF_8)) {
                if (line.contains("| TERMINATE |")) {
                    term = line;
                }
            }
            if (term == null) {
                out("  Client" + id + ": no TERMINATE log");
                everyClientDone = false;
                continue;
            }
            Map<String, Double> kv = keyValues(term);
            int responded = kv.getOrDefault("responded", 0.0).intValue();
            if (kv.getOrDefault("sent", 0.0).intValue() != requests || responded != requests
                    || !term.contains("Termination signal received")) {
                out("  Client" + id + ": missing requests/responses or no termination signal -> " + term);
                everyClientDone = false;
            }
            sumResponded += responded;
            sumSuccess += kv.getOrDefault("success", 0.0).longValue();
            sumFail += kv.getOrDefault("fail", 0.0).longValue();
            sumWaitlisted += kv.getOrDefault("waitlisted", 0.0).longValue();
            sumNotified += kv.getOrDefault("notified", 0.0).longValue();
            respWeighted += kv.getOrDefault("avg_resp_ms", 0.0) * responded;

            Matcher m = FINAL_HELD.matcher(term);
            if (m.find() && !m.group(1).isEmpty()) {
                for (String s : m.group(1).split(",")) {
                    int seat = Integer.parseInt(s);
                    if (clientOwner[seat] != 0) {
                        out("  seat#" + seat + " is in final_held of both Client" + clientOwner[seat] + " and Client" + id);
                        noSeatInTwoClients = false;
                    }
                    clientOwner[seat] = id;
                    sumHeld++;
                }
            }
        }

        // ---------------- 판정 ----------------
        out("===== Integrity check =====");
        long doubleBooking = check.getOrDefault("double_booking", -1.0).longValue();
        long assigned = check.getOrDefault("assigned", -1.0).longValue();
        long released = check.getOrDefault("released", -1.0).longValue();
        long pending = pendingLine.getOrDefault("pending_waitlist", -1.0).longValue();
        long serverWaitlisted = pendingLine.getOrDefault("waitlisted", -1.0).longValue();

        verdict("1. double booking count = 0", doubleBooking == 0,
                "double_booking=" + doubleBooking);
        verdict("2. assigned - released = reserved seats at end", assigned - released == serverReserved,
                assigned + " - " + released + " = " + (assigned - released) + ", reserved " + serverReserved);

        int mismatch = 0;
        for (int s = 1; s <= 100; s++) {
            if (serverOwner[s] != clientOwner[s]) {
                mismatch++;
                if (mismatch <= 10) {
                    out("  seat#" + s + ": server=" + name(serverOwner[s]) + " clients=" + name(clientOwner[s]));
                }
            }
        }
        verdict("3. server final seat map = clients' final held seats (owner per seat)",
                mismatch == 0 && noSeatInTwoClients && sumHeld == serverReserved,
                "mismatched seats " + mismatch + ", clients held " + sumHeld + ", server reserved " + serverReserved);
        verdict("4. WAITLISTED = NOTIFY received + pending waitlist at end",
                sumWaitlisted == sumNotified + pending && sumWaitlisted == serverWaitlisted,
                sumWaitlisted + " = " + sumNotified + " + " + pending + " (server WAITLISTED " + serverWaitlisted + ")");
        verdict("5. every client finished all requests and got termination signal", everyClientDone,
                "first responses " + sumResponded + " / " + (long) clients * requests);

        out("");
        out("Final seat integrity = " + (allPass ? "PASS" : "FAIL"));

        // ---------------- Readme용 결과표 (§4-3 형식) ----------------
        out("");
        out("===== Result metrics (copy into Readme table) =====");
        out(String.format("[Server] throughput               : %.1f req/s", metrics.getOrDefault("throughput", 0.0)));
        out(String.format("[Server] max request queue length : %d", metrics.getOrDefault("max_queue", 0.0).longValue()));
        out(String.format("[Server] double booking           : %d", doubleBooking));
        out(String.format("[Server] deadlock                 : %d", metrics.getOrDefault("deadlock", 0.0).longValue()));
        out(String.format("[Server] avg waitlist wait        : %.3f sec", metrics.getOrDefault("avg_waitlist_wait_sec", 0.0)));
        out(String.format("[Server] lock contention          : %d", metrics.getOrDefault("contention", 0.0).longValue()));
        out(String.format("[Client] total requests answered  : %d", sumResponded));
        out(String.format("[Client] SUCCESS                  : %d (%.1f%%)", sumSuccess, pct(sumSuccess, sumResponded)));
        out(String.format("[Client] FAIL                     : %d (%.1f%%)", sumFail, pct(sumFail, sumResponded)));
        out(String.format("[Client] WAITLISTED               : %d (%.1f%%)", sumWaitlisted, pct(sumWaitlisted, sumResponded)));
        out(String.format("[Client] NOTIFY / pending at end   : %d / %d", sumNotified, pending));
        out(String.format("[Client] avg response time        : %.1f ms", sumResponded == 0 ? 0 : respWeighted / sumResponded));
        out("Final seat integrity             : " + (allPass ? "PASS" : "FAIL"));

        finish(dir);
    }

    /** 로그 한 줄에서 key=숫자 쌍을 모두 뽑는다. (예: "assigned=41213 released=41116") */
    private static Map<String, Double> keyValues(String line) {
        Map<String, Double> map = new HashMap<>();
        Matcher m = KEY_NUM.matcher(line);
        while (m.find()) {
            map.put(m.group(1), Double.parseDouble(m.group(2)));
        }
        return map;
    }

    /** 항목 하나의 PASS/FAIL을 출력한다. 하나라도 FAIL이면 최종 결과도 FAIL. */
    private void verdict(String title, boolean ok, String detail) {
        if (!ok) {
            allPass = false;
        }
        out((ok ? "[PASS] " : "[FAIL] ") + title + "  (" + detail + ")");
    }

    /** 화면에 출력하고, VerifyResult.txt에 쓸 내용으로도 모아 둔다. */
    private void out(String s) {
        System.out.println(s);
        report.append(s).append('\n');
    }

    /** 모아 둔 결과를 log-dir/VerifyResult.txt로 저장한다. */
    private void finish(Path dir) throws IOException {
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(dir.resolve("VerifyResult.txt"),
                StandardCharsets.UTF_8))) {
            w.print(report);
        }
    }

    /** owner 번호 → "ClientN" (0이면 "EMPTY"). */
    private static String name(int owner) {
        return owner == 0 ? "EMPTY" : "Client" + owner;
    }

    /** 백분율 계산 (whole이 0이면 0). */
    private static double pct(long part, long whole) {
        return whole == 0 ? 0 : part * 100.0 / whole;
    }
}
