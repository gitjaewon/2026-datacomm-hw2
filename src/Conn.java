import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

// 서버 쪽 연결 1개. 여러 스레드가 보낼 수 있어서 sendLock으로 한 번에 하나씩 보낸다
public class Conn {
    final SocketChannel ch;
    final StringBuilder inbox = new StringBuilder();  // 받은 데이터 버퍼 (Listener만 사용)
    volatile int clientId;  // HELLO 받기 전엔 0
    private final ReentrantLock sendLock = new ReentrantLock();
    private boolean closed;

    Conn(SocketChannel ch) {
        this.ch = ch;
    }

    String name() {
        return "Client" + clientId;
    }

    boolean send(String line) {
        ByteBuffer buf = ByteBuffer.wrap((line + "\n").getBytes(StandardCharsets.UTF_8));
        sendLock.lock();
        try {
            if (closed) {
                return false;
            }
            long waitingSince = 0;
            // 송신 버퍼가 꽉 차면 잠깐 쉬고 재시도, 10초 넘으면 포기
            while (buf.hasRemaining()) {
                if (ch.write(buf) > 0) {
                    waitingSince = 0;
                } else if (waitingSince == 0) {
                    waitingSince = System.nanoTime();
                } else if (System.nanoTime() - waitingSince > 10_000_000_000L) {
                    closed = true;
                    ch.close();
                    return false;
                } else {
                    LockSupport.parkNanos(1_000_000L);
                }
            }
            return true;
        } catch (IOException e) {
            closed = true;
            return false;
        } finally {
            sendLock.unlock();
        }
    }

    void close() {
        sendLock.lock();
        try {
            closed = true;
            ch.close();
        } catch (IOException ignored) {
        } finally {
            sendLock.unlock();
        }
    }
}
