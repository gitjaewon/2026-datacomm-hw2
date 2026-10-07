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
    private static final Pattern KEY_NUM = Pattern.compile("([a-z_]+)=(-?[0-9]+(?:\\.[0-9]+)?)");
    private static final Pattern FINAL_HELD = Pattern.compile("final_held=\\[([0-9,]*)]");

    private final StringBuilder report = new StringBuilder();
    private boolean allPass = true;

    public static void main(String[] args) throws IOException {
        String dir = "logs";
        int clients = 30;
        int requests = 5000;
        Arguments.requirePairs(args);
        for (int i = 0; i < args.length; i += 2) {
            switch (args[i]) {
                case "--log-dir" -> dir = args[i + 1];
                case "--requests" -> requests = Arguments.positiveInt(args[i], args[i + 1]);
                default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
        if (!new Verify().run(Paths.get(dir), clients, requests)) {
            System.exit(1);
        }
    }

    boolean run(Path dir, int clients, int requests) throws IOException {
        // Server.txt 읽기
        List<String> serverLines = Files.readAllLines(dir.resolve("Server.txt"), StandardCharsets.UTF_8);
        int[] serverOwner = new int[101];
        int seatsFound = 0;
        boolean[] seatSeen = new boolean[101];
        boolean validSeatMap = true;
        String serverTermination = "";
        Map<String, Double> check = Map.of();
        Map<String, Double> pendingLine = Map.of();
        Map<String, Double> metrics = Map.of();
        for (String line : serverLines) {
            if (line.contains("Final seat map")) {
                Matcher m = SEAT_OWNER.matcher(line.substring(line.indexOf(':', line.indexOf("Final seat map")) + 1));
                while (m.find()) {
                    int seat = Integer.parseInt(m.group(1));
                    int owner = m.group(3) == null ? 0 : Integer.parseInt(m.group(3));
                    if (seat < 1 || seat > 100 || seatSeen[seat] || owner < 0 || owner > clients
                            || (m.group(3) != null && owner == 0)) {
                        validSeatMap = false;
                        continue;
                    }
                    seatSeen[seat] = true;
                    serverOwner[seat] = owner;
                    seatsFound++;
                }
            } else if (line.contains("| DOUBLE_BOOKING_CHECK |")) {
                check = keyValues(line);
            } else if (line.contains("pending_waitlist=")) {
                pendingLine = keyValues(line);
            } else if (line.contains("Metrics:")) {
                metrics = keyValues(line);
            } else if (line.contains("Graceful shutdown.")) {
                serverTermination = line;
            }
        }
        if (!validSeatMap || seatsFound != 100
                || !hasNumbers(check, "double_booking", "assigned", "released", "reserved_now")
                || !hasNumbers(pendingLine, "pending_waitlist", "waitlisted", "handoffs", "notify_sent", "notify_failed")
                || !hasNumbers(metrics, "processed", "elapsed_sec", "throughput", "max_queue", "double_booking", "deadlock",
                        "avg_waitlist_wait_sec", "contention", "success", "fail", "waitlisted", "response_failed", "server_errors")) {
            verdict("Complete and valid server final report", false, "missing/invalid seat map or metrics");
            out("Final seat integrity = FAIL");
            finish(dir);
            return false;
        }
        int serverReserved = 0;
        for (int s = 1; s <= 100; s++) {
            if (serverOwner[s] != 0) {
                serverReserved++;
            }
        }

        // Client 로그 읽기
        int[] clientOwner = new int[101];
        long sumHeld = 0, sumWaitlisted = 0, sumNotified = 0, sumSuccess = 0, sumFail = 0, sumResponded = 0, sumUnresolved = 0;
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
            boolean completeSummary = hasNumbers(kv, "sent", "responded", "success", "fail", "waitlisted",
                    "notified", "unresolved", "avg_resp_ms", "protocol_errors");
            int responded = kv.getOrDefault("responded", 0.0).intValue();
            if (!completeSummary || kv.getOrDefault("sent", 0.0).intValue() != requests || responded != requests
                    || !term.contains("| TERMINATE | SUCCESS | Termination signal received.")
                    || kv.getOrDefault("protocol_errors", -1.0) != 0
                    || responded != kv.getOrDefault("success", -1.0) + kv.getOrDefault("fail", -1.0) + kv.getOrDefault("waitlisted", -1.0)
                    || kv.getOrDefault("waitlisted", -1.0) != kv.getOrDefault("notified", -1.0) + kv.getOrDefault("unresolved", -1.0)) {
                out("  Client" + id + ": missing requests/responses or no termination signal -> " + term);
                everyClientDone = false;
            }
            sumResponded += responded;
            sumSuccess += kv.getOrDefault("success", 0.0).longValue();
            sumFail += kv.getOrDefault("fail", 0.0).longValue();
            sumWaitlisted += kv.getOrDefault("waitlisted", 0.0).longValue();
            sumNotified += kv.getOrDefault("notified", 0.0).longValue();
            sumUnresolved += kv.getOrDefault("unresolved", 0.0).longValue();
            respWeighted += kv.getOrDefault("avg_resp_ms", 0.0) * responded;

            Matcher m = FINAL_HELD.matcher(term);
            if (!m.find()) {
                out("  Client" + id + ": missing or malformed final_held");
                everyClientDone = false;
            } else if (!m.group(1).isEmpty()) {
                for (String s : m.group(1).split(",", -1)) {
                    int seat;
                    try {
                        seat = Integer.parseInt(s);
                    } catch (NumberFormatException e) {
                        everyClientDone = false;
                        continue;
                    }
                    if (seat < 1 || seat > 100) {
                        out("  Client" + id + ": invalid final_held seat " + seat);
                        everyClientDone = false;
                        continue;
                    }
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
        verdict("2. assigned - released = reserved seats at end", assigned - released == serverReserved
                        && check.get("reserved_now").intValue() == serverReserved,
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
                sumWaitlisted == sumNotified + pending && sumWaitlisted == serverWaitlisted && sumUnresolved == pending
                        && pendingLine.get("notify_sent").longValue() == sumNotified
                        && pendingLine.get("handoffs").longValue() == sumNotified
                        && pendingLine.get("notify_failed") == 0,
                sumWaitlisted + " = " + sumNotified + " + " + pending + " (server WAITLISTED " + serverWaitlisted + ")");
        verdict("5. every client finished all requests and got termination signal", everyClientDone,
                "first responses " + sumResponded + " / " + (long) clients * requests);
        verdict("6. server/client response totals agree and server terminated successfully",
                metrics.get("processed").longValue() == (long) clients * requests
                        && metrics.get("processed").longValue() == sumResponded
                        && metrics.get("success").longValue() == sumSuccess
                        && metrics.get("fail").longValue() == sumFail
                        && metrics.get("waitlisted").longValue() == sumWaitlisted
                        && metrics.get("double_booking").longValue() == doubleBooking
                        && metrics.get("response_failed") == 0 && metrics.get("server_errors") == 0
                        && serverTermination.contains("| TERMINATE | SUCCESS |")
                        && serverTermination.contains("Termination signal sent to " + clients + " clients,")
                        && serverTermination.contains("server_checks=PASS"),
                "processed=" + metrics.get("processed").longValue());
        verdict("7. deadlock count = 0", metrics.get("deadlock") == 0,
                "deadlock=" + metrics.get("deadlock").longValue());

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
        return allPass;
    }

    private static boolean hasNumbers(Map<String, Double> values, String... keys) {
        for (String key : keys) {
            Double value = values.get(key);
            if (value == null || !Double.isFinite(value) || value < 0) {
                return false;
            }
        }
        return true;
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
