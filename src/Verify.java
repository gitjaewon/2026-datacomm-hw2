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

// 실행 후 Server.txt, Client1~30.txt로 정합성 확인. 결과는 VerifyResult.txt
public final class Verify {
    private static final Pattern SEAT_OWNER = Pattern.compile("(\\d+)=(EMPTY|Client(\\d+))");
    private static final Pattern KEY_NUM = Pattern.compile("([a-z_]+)=([0-9]+(?:\\.[0-9]+)?)");
    private static final Pattern FINAL_HELD = Pattern.compile("final_held=\\[([0-9,]*)]");

    private final StringBuilder report = new StringBuilder();
    private boolean allPass = true;

    public static void main(String[] args) throws IOException {
        String dir = "logs";
        int clients = 30;
        int requests = 5000;
        for (int i = 0; i + 1 < args.length; i += 2) {
            switch (args[i]) {
                case "--log-dir" -> dir = args[i + 1];
                case "--requests" -> requests = Integer.parseInt(args[i + 1]);
                default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
        new Verify().run(Paths.get(dir), clients, requests);
    }

    private void run(Path dir, int clients, int requests) throws IOException {
        // Server.txt 읽기
        List<String> serverLines = Files.readAllLines(dir.resolve("Server.txt"), StandardCharsets.UTF_8);
        int[] serverOwner = new int[101];
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

        // Client 로그 읽기
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

        // 판정
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

        out("");
        // 결과표
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

    // key=숫자 값 뽑기
    private static Map<String, Double> keyValues(String line) {
        Map<String, Double> map = new HashMap<>();
        Matcher m = KEY_NUM.matcher(line);
        while (m.find()) {
            map.put(m.group(1), Double.parseDouble(m.group(2)));
        }
        return map;
    }

    private void verdict(String title, boolean ok, String detail) {
        if (!ok) {
            allPass = false;
        }
        out((ok ? "[PASS] " : "[FAIL] ") + title + "  (" + detail + ")");
    }

    private void out(String s) {
        System.out.println(s);
        report.append(s).append('\n');
    }

    private void finish(Path dir) throws IOException {
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(dir.resolve("VerifyResult.txt"),
                StandardCharsets.UTF_8))) {
            w.print(report);
        }
    }

    private static String name(int owner) {
        return owner == 0 ? "EMPTY" : "Client" + owner;
    }

    private static double pct(long part, long whole) {
        return whole == 0 ? 0 : part * 100.0 / whole;
    }
}
