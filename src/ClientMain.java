import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Client 실행 진입점. 한 번 실행으로 Client 여러 개(기본 30개)를 띄우고, 진행률과 합계를 출력한다.
 * Client 1개의 동작(접속·요청 전송·응답 수신)은 Client.java에 있다.
 * Client마다 서버와 독립된 TCP 연결(Socket)을 1개씩 맺는다.
 *
 * 실행 예 (제출용 최종 실행):
 *   java -cp out ClientMain --host <서버IP> --port 5000
 * 개발 중 빠른 테스트:
 *   java -cp out ClientMain --host 127.0.0.1 --port 5000 --requests 200 --min-interval-ms 10 --max-interval-ms 50
 * Client 하나만 따로 띄우기 (예: Client7):
 *   java -cp out ClientMain --host <서버IP> --port 5000 --clients 1 --id-start 7
 *
 * 인자 (괄호 안은 기본값)
 *   --host 서버 주소 (필수)   --port 서버 포트 (필수)   --clients 띄울 수 (30)   --id-start 첫 번호 (1)
 *   --requests Client당 요청 수 (5000)   --min-interval-ms (200)   --max-interval-ms (1000)
 *   --hot-seats 인기 좌석 1~N (10)   --hot-ratio 인기 좌석을 고를 확률 (0.7)   --log-dir (logs)
 */
public class ClientMain {

    // =====================================================================
    // 설정 (실행 인자)
    // =====================================================================
    // host, port는 하드코딩 금지 조건(명세 §0-2) 때문에 기본값이 없다. 인자로 안 주면 실행되지 않는다.
    static String host;
    static int port = -1;
    static int clientCount = 30;
    static int idStart = 1;
    static int requests = 5000;
    static int minIntervalMs = 200;
    static int maxIntervalMs = 1000;
    static int hotSeats = 10;      // 인기 좌석 범위 1 ~ hotSeats
    // 인기 좌석을 고를 확률. CANCEL은 내가 가진 좌석(대부분 비인기 좌석) 중에서 고르기 때문에
    // 0.6이면 전체 요청 좌석 중 인기 좌석 비율이 53% 정도로 50%에 너무 가깝다. 여유 있게 0.7로 둔다.
    // → "요청 좌석의 절반 이상을 일부 좌석에 몰기" 조건 충족 (명세 §1-②)
    static double hotRatio = 0.7;
    static String logDir = "logs";


    // =====================================================================
    // main: Client 30개 실행 → 진행률 출력 → 합계 출력
    // =====================================================================
    public static void main(String[] args) throws Exception {
        parseArgs(args);
        Files.createDirectories(Paths.get(logDir));

        Client[] list = new Client[clientCount];
        Thread[] threads = new Thread[clientCount];
        for (int i = 0; i < clientCount; i++) {
            list[i] = new Client(idStart + i);
            threads[i] = new Thread(list[i], "Client" + (idStart + i));
        }
        System.out.printf("Starting %d clients (Client%d~Client%d) -> %s:%d, %d requests each%n",
                clientCount, idStart, idStart + clientCount - 1, host, port, requests);
        for (Thread t : threads) {
            t.start();
        }

        // 약 50분 걸리는 최종 실행 중 상태를 볼 수 있게 10초마다 진행률 출력
        long total = (long) clientCount * requests;
        for (Thread t : threads) {
            while (t.isAlive()) {
                t.join(10_000);
                if (t.isAlive()) {
                    long done = 0;
                    for (Client c : list) {
                        done += c.respondedSoFar();
                    }
                    System.out.printf("progress: responded %d / %d%n", done, total);
                }
            }
        }
        printSummary(list);
    }

    /**
     * 실행 인자(--host, --port 등)를 읽어 위의 설정값에 넣는다.
     * --host, --port가 없거나 값이 이상하면 예외로 바로 종료한다.
     */
    static void parseArgs(String[] args) {
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Arguments must be --key value pairs.");
        }
        for (int i = 0; i < args.length; i += 2) {
            String v = args[i + 1];
            switch (args[i]) {
                case "--host" -> host = v;
                case "--port" -> port = Integer.parseInt(v);
                case "--clients" -> clientCount = Integer.parseInt(v);
                case "--id-start" -> idStart = Integer.parseInt(v);
                case "--requests" -> requests = Integer.parseInt(v);
                case "--min-interval-ms" -> minIntervalMs = Integer.parseInt(v);
                case "--max-interval-ms" -> maxIntervalMs = Integer.parseInt(v);
                case "--hot-seats" -> hotSeats = Integer.parseInt(v);
                case "--hot-ratio" -> hotRatio = Double.parseDouble(v);
                case "--log-dir" -> logDir = v;
                default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
        if (host == null || port < 0) {
            throw new IllegalArgumentException("--host and --port are required. e.g. --host <server IP> --port 5000");
        }
        if (minIntervalMs < 0 || maxIntervalMs < minIntervalMs || hotSeats < 1 || hotSeats >= 100) {
            throw new IllegalArgumentException("Invalid interval or hot-seats setting.");
        }
    }

    /** 30개 Client 합계 (명세 §4-3 Client 결과 표 형식) */
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
        System.out.printf("requests sent       : %d%n", sent);
        System.out.printf("first responses     : %d%n", responded);
        System.out.printf("SUCCESS             : %d (%.1f%%)%n", success, pct(success, responded));
        System.out.printf("FAIL                : %d (%.1f%%)%n", fail, pct(fail, responded));
        System.out.printf("WAITLISTED          : %d (%.1f%%)%n", waitlisted, pct(waitlisted, responded));
        System.out.printf("NOTIFY / unresolved : %d / %d%n", notified, waitlisted - notified);
        System.out.printf("avg response time   : %.1f ms%n", responded == 0 ? 0 : nanos / (double) responded / 1e6);
        if (noBye > 0) {
            System.out.printf("WARNING: %d client(s) ended without termination signal%n", noBye);
        }
    }

    /** 백분율 계산 (whole이 0이면 0). */
    static double pct(long part, long whole) {
        return whole == 0 ? 0 : part * 100.0 / whole;
    }
}
