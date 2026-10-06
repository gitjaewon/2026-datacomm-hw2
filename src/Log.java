import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 공통 로그 (Server, Client 같이 사용).
 *
 * 형식: [HH:MM:SS.mmm] NODE | EVENT | STATUS | message   (명세 §5)
 *   EVENT  : INIT, CONNECT, RESERVE, RESERVE_MULTI, CANCEL, LOCK, WAITLIST, NOTIFY, POOL, DOUBLE_BOOKING_CHECK, TERMINATE
 *   STATUS : SUCCESS, FAIL, INFO, WARN
 *
 * - 시각은 각 노드의 실제 시각(wall-clock). 시간대는 코드에서 KST(Asia/Seoul)로 고정한다 (ZONE).
 * - 여러 스레드가 동시에 쓰므로 Lock으로 한 줄씩 쓴다.
 * - 로그 전용 스레드는 두지 않는다 (서버 스레드 개수 규칙).
 */
public class Log {

    /**
     * 로그 시각의 시간대. Server와 Client 모두 한국 시간(KST, UTC+09:00)으로 고정한다.
     * JVM 기본 시간대를 따르지 않으므로, 기본값이 UTC인 원격 호스트에서 실행해도 KST로 찍힌다.
     */
    static final ZoneId ZONE = ZoneId.of("Asia/Seoul");

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final String node;
    private final BufferedWriter out;
    private final ReentrantLock lock = new ReentrantLock();

    /** @param node 로그에 찍힐 노드 이름 (예: "SERVER", "CLIENT9") */
    public Log(String node, Path file) throws IOException {
        this.node = node;
        this.out = Files.newBufferedWriter(file, StandardCharsets.UTF_8);
    }

    /** 파일에만 기록 (요청 단위 로그처럼 양이 많은 것) */
    public void write(String event, String status, String message) {
        append(event, status, message, false);
    }

    /**
     * 파일 + 콘솔. 화면에서 꼭 봐야 하는 것만 쓴다:
     * 서버 시작, 전원 접속, 경고(Deadlock 의심·연결 끊김·오류), 최종 결과(이중예약 검사·지표·종료).
     */
    public void console(String event, String status, String message) {
        append(event, status, message, true);
    }

    /** 실제로 한 줄을 만들어 파일에 쓰는 함수. write()와 console()이 공통으로 부른다. */
    private void append(String event, String status, String message, boolean echo) {
        String line;
        lock.lock();
        try {
            // 시각을 Lock 안에서 찍어야 파일의 줄 순서와 시각 순서가 항상 일치한다
            line = "[" + LocalTime.now(ZONE).format(TIME) + "] " + node + " | " + event + " | " + status + " | " + message;
            out.write(line);
            out.write('\n');
            out.flush(); // 비정상 종료돼도 직전까지의 로그는 남도록 매 줄 flush
        } catch (IOException e) {
            System.err.println("Failed to write log: " + e.getMessage());
            return;
        } finally {
            lock.unlock();
        }
        if (echo) {
            System.out.println(line);
        }
    }

    /** 로그 파일을 닫는다 (프로그램 종료 시). */
    public void close() {
        lock.lock();
        try {
            out.close();
        } catch (IOException e) {
            System.err.println("Failed to close log: " + e.getMessage());
        } finally {
            lock.unlock();
        }
    }
}
