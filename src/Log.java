import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.locks.ReentrantLock;

// [HH:MM:SS.mmm] NODE | EVENT | STATUS | message 형식 로그
public class Log {
    static final ZoneId ZONE = ZoneId.of("Asia/Seoul");  // 로그 시간은 KST로 고정

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final String node;
    private final BufferedWriter out;
    private final ReentrantLock lock = new ReentrantLock();

    public Log(String node, Path file) throws IOException {
        this.node = node;
        this.out = Files.newBufferedWriter(file, StandardCharsets.UTF_8);
    }

    public void write(String event, String status, String message) {
        append(event, status, message, false);
    }

    // 파일 + 화면 출력
    public void console(String event, String status, String message) {
        append(event, status, message, true);
    }

    private void append(String event, String status, String message, boolean echo) {
        String line;
        lock.lock();
        try {
            line = "[" + LocalTime.now(ZONE).format(TIME) + "] " + node + " | " + event + " | " + status + " | " + message;
            out.write(line);
            out.write('\n');
            out.flush();  // 비정상 종료 대비
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
