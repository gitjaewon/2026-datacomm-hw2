import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 서버 쪽에서 본 Client 연결 하나 (Listener.accept()에서 연결마다 1개 생성).
 *
 * 수신은 Listener만 한다. 송신은 Worker 10개와 Notifier가 동시에 할 수 있으므로
 * 소켓별 송신 Lock(sendLock)으로 한 번에 한 메시지만 쓰게 해서 메시지가 섞이지 않게 한다.
 */
public class Conn {
    final SocketChannel ch;
    final StringBuilder inbox = new StringBuilder(); // Listener 전용 수신 버퍼
    volatile int clientId;                           // HELLO 전에는 0
    private final ReentrantLock sendLock = new ReentrantLock();
    private boolean closed;

    /** accept()로 받은 연결 소켓을 감싼다. clientId는 HELLO를 받으면 채워진다. */
    Conn(SocketChannel ch) {
        this.ch = ch;
    }

    /** 로그용 이름 "ClientN". */
    String name() {
        return "Client" + clientId;
    }

    /** 한 줄 전송 ('\n'은 여기서 붙임). 끊긴 연결이면 false */
    boolean send(String line) {
        ByteBuffer buf = ByteBuffer.wrap((line + "\n").getBytes(StandardCharsets.UTF_8));
        sendLock.lock();
        try {
            if (closed) {
                return false;
            }
            long waitingSince = 0;
            while (buf.hasRemaining()) {
                // non-blocking 채널이라 송신 버퍼가 꽉 차면 write()가 0을 돌려준다.
                // 응답은 한 줄짜리라 보통 한 번에 나가고, Client가 수신을 멈춘 비정상 상황에서만
                // 1ms씩 양보하며 재시도하다가 10초가 지나면 포기한다.
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

    /** 연결을 닫는다. sendLock을 잡고 닫아서, 다른 스레드가 보내는 도중에 닫히지 않게 한다. */
    void close() {
        sendLock.lock();
        try {
            closed = true;
            ch.close();
        } catch (IOException ignored) {
            // 이미 닫힘
        } finally {
            sendLock.unlock();
        }
    }
}
