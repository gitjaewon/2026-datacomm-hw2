import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;

// Client 30개 실행. 실행: java -cp out ClientMain --host <서버IP> --port 5000
public class ClientMain {
    // 고정 설정
    static final int clientCount = 30;
    static final int idStart = 1;
    static int minIntervalMs = 200;
    static int maxIntervalMs = 1000;
    static final int hotSeats = 10;
    static final double hotRatio = 0.7;  // 인기 좌석을 고를 확률

    // 실행 인자 (host, port는 필수)
    static String host;
    static int port = -1;
    static int requests = 5000;
    static String logDir = "logs";

    public static void main(String[] args) throws Exception {
        parseArgs(args);
        Files.createDirectories(Paths.get(logDir));

        Client[] list = new Client[clientCount];
        Thread[] threads = new Thread[clientCount];
        for (int i = 0; i < clientCount; i++) {
            list[i] = new Client(idStart + i);
            threads[i] = new Thread(list[i], "Client" + (idStart + i));
        }
        System.out.printf(Locale.ROOT, "Starting %d clients (Client%d~Client%d) -> %s:%d, %d requests each%n",
                clientCount, idStart, idStart + clientCount - 1, host, port, requests);
        for (Thread t : threads) {
            t.start();
        }

        // 10초마다 진행 상황 출력
        long total = (long) clientCount * requests;
        for (Thread t : threads) {
            while (t.isAlive()) {
                t.join(10_000);
                if (t.isAlive()) {
                    long done = 0;
                    for (Client c : list) {
                        done += c.respondedSoFar();
                    }
                    System.out.printf(Locale.ROOT, "progress: responded %d / %d%n", done, total);
                }
            }
        }
        printSummary(list);
    }

    static void parseArgs(String[] args) {
        Arguments.requirePairs(args);
        for (int i = 0; i < args.length; i += 2) {
            String v = args[i + 1];
            switch (args[i]) {
                case "--host" -> host = v;
                case "--port" -> port = Integer.parseInt(v);
                case "--requests" -> requests = Arguments.positiveInt(args[i], v);
                case "--min-interval-ms" -> minIntervalMs = Arguments.positiveInt(args[i], v);
                case "--max-interval-ms" -> maxIntervalMs = Arguments.positiveInt(args[i], v);
                case "--log-dir" -> logDir = v;
                default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
        Arguments.requireEndpoint(host, port);
        if (maxIntervalMs < minIntervalMs || maxIntervalMs == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Intervals must satisfy 1 <= min <= max < 2147483647.");
        }
    }

    // 30개 합계 출력
    static void printSummary(Client[] list) {
        long sent = 0, responded = 0, success = 0, fail = 0, waitlisted = 0, notified = 0, nanos = 0;
        int noBye = 0;
        for (Client c : list) {
            c.lock.lock();
            try {
                sent += c.sent;
                responded += c.responded;
                success += c.success;
                fail += c.fail;
                waitlisted += c.waitlisted;
                notified += c.notified;
                nanos += c.respNanosSum;
                if (!c.byeReceived) {
                    noBye++;
                }
            } finally {
                c.lock.unlock();
            }
        }
        System.out.println("===== Client summary =====");
        System.out.printf(Locale.ROOT, "requests sent       : %d%n", sent);
        System.out.printf(Locale.ROOT, "first responses     : %d%n", responded);
        System.out.printf(Locale.ROOT, "SUCCESS             : %d (%.1f%%)%n", success, pct(success, responded));
        System.out.printf(Locale.ROOT, "FAIL                : %d (%.1f%%)%n", fail, pct(fail, responded));
        System.out.printf(Locale.ROOT, "WAITLISTED          : %d (%.1f%%)%n", waitlisted, pct(waitlisted, responded));
        System.out.printf(Locale.ROOT, "NOTIFY / unresolved : %d / %d%n", notified, waitlisted - notified);
        System.out.printf(Locale.ROOT, "avg response time   : %.1f ms%n", responded == 0 ? 0 : nanos / (double) responded / 1e6);
        if (noBye > 0) {
            System.out.printf(Locale.ROOT, "WARNING: %d client(s) ended without termination signal%n", noBye);
        }
    }

    static double pct(long part, long whole) {
        return whole == 0 ? 0 : part * 100.0 / whole;
    }
}
