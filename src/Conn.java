import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.concurrent.locks.ReentrantLock;

// 서버 쪽 연결 1개. 여러 스레드가 보낼 수 있어서 sendLock으로 한 번에 하나씩 보낸다
public class Conn {
    final SocketChannel ch;
    final StringBuilder inbox = new StringBuilder();  // 받은 데이터 버퍼 (Listener만 사용)
    final BitSet seenRequestIds = new BitSet();  // Listener만 접근
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
        Selector writable = null;
        try {
            if (closed) {
                return false;
            }
            long waitingSince = 0;
            // 송신 버퍼가 꽉 차면 쓰기 가능 이벤트를 기다림 (폴링/추가 스레드 없음)
            while (buf.hasRemaining()) {
                if (ch.write(buf) > 0) {
                    waitingSince = 0;
                } else {
                    if (waitingSince == 0) {
                        waitingSince = System.nanoTime();
                    }
                    long remaining = 10_000_000_000L - (System.nanoTime() - waitingSince);
                    if (remaining <= 0) {
                        throw new SocketTimeoutException("No socket write progress for 10s");
                    }
                    if (writable == null) {
                        writable = Selector.open();
                        ch.register(writable, SelectionKey.OP_WRITE);
                    }
                    writable.select(Math.max(1, remaining / 1_000_000L));
                    writable.selectedKeys().clear();
                }
            }
            return true;
        } catch (IOException e) {
            closed = true;
            try {
                ch.close();
            } catch (IOException ignored) {
            }
            return false;
        } finally {
            if (writable != null) {
                try {
                    writable.close();
                } catch (IOException ignored) {
                }
            }
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
