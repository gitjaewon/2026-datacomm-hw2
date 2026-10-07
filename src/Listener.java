import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

// 접속 수락, 소켓 감시, 메시지 파싱 후 큐에 넣기 (main 스레드에서 실행)
public class Listener {
    static Selector selector;
    static ServerSocketChannel serverChannel;
    static final List<Conn> conns = new ArrayList<>();  // 열린 연결
    static final Set<Integer> activeIds = new HashSet<>();  // HELLO 받은 Client 번호
    static final Set<Integer> registeredIds = new HashSet<>();  // 실행 중 Client 번호 재사용 금지
    static final ByteBuffer readBuf = ByteBuffer.allocate(8192);
    static volatile boolean shutdownRequested;
    static volatile String shutdownReason = "";
    static long nextPoolAt;
    static long lastProcessedSeen = -1;
    static long lastProgressAt;
    static boolean stallReported;

    static void openServerSocket() throws IOException {
        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverChannel.bind(new InetSocketAddress(Server.host, Server.port));
        serverChannel.configureBlocking(false);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    // 종료 요청. select()에서 자고 있으면 깨운다
    static void requestShutdown(String reason) {
        if (shutdownRequested) {
            return;
        }
        shutdownReason = reason;
        shutdownRequested = true;
        selector.wakeup();
    }

    static void listenLoop() throws IOException {
        nextPoolAt = System.currentTimeMillis() + Server.poolSec * 1000L;
        lastProgressAt = System.currentTimeMillis();
        // select는 0.5초마다 깨서 tick()도 실행
        while (!shutdownRequested) {
            selector.select(500);
            Iterator<SelectionKey> it = selector.selectedKeys().iterator();
            while (it.hasNext()) {
                SelectionKey key = it.next();
                it.remove();
                if (!key.isValid()) {
                    continue;
                }
                if (key.isAcceptable()) {
                    accept();
                } else if (key.isReadable()) {
                    read(key);
                }
            }
            tick();
        }
    }

    static void accept() throws IOException {
        SocketChannel ch;
        while ((ch = serverChannel.accept()) != null) {
            ch.configureBlocking(false);
            ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
            Conn conn = new Conn(ch);
            ch.register(selector, SelectionKey.OP_READ, conn);
            conns.add(conn);
        }
    }

    static void read(SelectionKey key) {
        Conn conn = (Conn) key.attachment();
        readBuf.clear();
        int n;
        try {
            n = conn.ch.read(readBuf);
        } catch (IOException e) {
            n = -1;
        }
        if (n < 0) {
            closeConn(key, conn, "connection closed by peer");
            return;
        }
        readBuf.flip();
        conn.inbox.append(StandardCharsets.UTF_8.decode(readBuf));
        // 줄바꿈 단위로 잘라서 처리 (메시지가 붙거나 잘려 와도 되게)
        int nl;
        while ((nl = conn.inbox.indexOf("\n")) >= 0) {
            String line = conn.inbox.substring(0, nl).trim();
            conn.inbox.delete(0, nl + 1);
            handleLine(conn, line);
        }
        if (conn.inbox.length() > 64 * 1024) {
            closeConn(key, conn, "line too long");
        }
    }

    static void handleLine(Conn conn, String line) {
        if (line.isEmpty()) {
            return;
        }
        String[] t = line.split("\\s+");
        String cmd = t[0];

        if (cmd.equals("HELLO")) {
            handleHello(conn, t);
            return;
        }
        if (conn.clientId == 0) {
            Server.log.write("CONNECT", "WARN", "Message before HELLO ignored: \"" + line + "\".");
            return;
        }
        if (!cmd.equals("RESERVE") && !cmd.equals("RESERVE_MULTI") && !cmd.equals("CANCEL")) {
            Server.log.write("CONNECT", "WARN", conn.name() + " unknown message ignored: \"" + line + "\".");
            return;
        }
        int reqId;
        try {
            reqId = Integer.parseInt(t[1]);
        } catch (RuntimeException e) {
            Server.log.write("CONNECT", "WARN", conn.name() + " request without valid reqId ignored: \"" + line + "\".");
            return;
        }
        if (reqId < 1 || reqId > Server.requestsPerClient || conn.seenRequestIds.get(reqId)) {
            Server.log.write("CONNECT", "WARN", conn.name() + " duplicate or out-of-range reqId ignored: " + reqId + ".");
            return;
        }
        conn.seenRequestIds.set(reqId);
        // 좌석이 숫자가 아니면 null로 넘겨서 FAIL 처리
        int[] seatArr = null;
        if (t.length == 3) {
            try {
                seatArr = cmd.equals("RESERVE_MULTI")
                        ? Arrays.stream(t[2].split(",", -1)).mapToInt(Integer::parseInt).toArray()
                        : new int[] {Integer.parseInt(t[2])};
            } catch (NumberFormatException e) {
                seatArr = null;
            }
        }
        enqueue(new RequestQueue.Request(conn, conn.clientId, reqId, cmd, seatArr));
    }

    static void handleHello(Conn conn, String[] t) {
        int id;
        try {
            id = Integer.parseInt(t[1]);
        } catch (RuntimeException e) {
            Server.log.write("CONNECT", "WARN", "HELLO with invalid client id ignored.");
            return;
        }
        if (t.length != 2 || conn.clientId != 0 || id < 1 || id > Server.expectedClients || !registeredIds.add(id)) {
            Server.log.write("CONNECT", "WARN", "HELLO rejected: Client" + id + " is invalid or already connected.");
            return;
        }
        activeIds.add(id);
        conn.clientId = id;
        if (Server.startMillis == 0) {  // 처리량 측정 시작
            Server.startMillis = System.currentTimeMillis();
            Server.startNanos = System.nanoTime();
        }
        Server.log.write("CONNECT", "SUCCESS", "Client" + id + " connected (" + activeIds.size() + "/" + Server.expectedClients + ").");
        if (activeIds.size() == Server.expectedClients) {
            Server.log.console("CONNECT", "SUCCESS", "All clients connected (" + Server.expectedClients + "/" + Server.expectedClients + ").");
        }
    }

    static void enqueue(RequestQueue.Request req) {
        try {
            // 큐가 꽉 차면 자리 날 때까지 대기 (1초마다 tick)
            while (!Server.requestQueue.offer(req, 1000)) {
                if (Server.requestQueue.isClosed()) {
                    Server.log.write("CONNECT", "WARN", "Request after shutdown ignored: Client" + req.clientId() + " req=" + req.reqId() + ".");
                    return;
                }
                tick();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static void tick() {
        long now = System.currentTimeMillis();
        long done = Server.processed.get();
        int queued = Server.requestQueue.size();

        // 큐에 요청이 있는데 deadlockSec 동안 처리가 없으면 deadlock 의심
        if (done != lastProcessedSeen || queued == 0) {
            lastProcessedSeen = done;
            lastProgressAt = now;
            stallReported = false;
        } else if (!stallReported && now - lastProgressAt >= Server.deadlockSec * 1000L) {
            Server.deadlockCount.increment();
            stallReported = true;
            Server.log.console("POOL", "WARN", "No progress for " + Server.deadlockSec + "s while queue=" + queued
                    + ". Possible deadlock (count=" + Server.deadlockCount.sum() + ").");
        }

        // 5초마다 POOL 로그
        if (now >= nextPoolAt) {
            nextPoolAt = now + Server.poolSec * 1000L;
            SeatManager.Snapshot snap = SeatManager.snapshot();
            Server.log.write("POOL", "INFO", String.format(
                    "queue=%d max_queue=%d processed=%d reserved=%d/%d waitlist_total=%d contention=%d seats=%s",
                    queued, Server.requestQueue.maxSize(), done, snap.reservedCount(), SeatManager.SEAT_COUNT,
                    snap.waitlistTotal(), SeatManager.contention.sum(), snap.bitmap()));
        }
    }

    static void closeConn(SelectionKey key, Conn conn, String reason) {
        key.cancel();
        conn.close();
        conns.remove(conn);
        if (conn.clientId == 0) {
            return;
        }
        activeIds.remove(conn.clientId);
        if (shutdownRequested) {
            return;
        }
        Server.log.console("CONNECT", "WARN", conn.name() + " disconnected (" + reason + "). active=" + activeIds.size() + ".");
        // 전부 끊기면 종료
        if (activeIds.isEmpty()) {
            requestShutdown("all clients disconnected before finishing");
        }
    }

    static int broadcastBye() {
        int sent = 0;
        for (Conn c : conns) {
            if (c.clientId != 0 && c.send("BYE")) {
                sent++;
            }
        }
        return sent;
    }

    // BYE 보낸 뒤 Client들이 끊을 때까지 최대 timeoutMs 대기
    static void waitForClientsToClose(long timeoutMs) throws IOException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!conns.isEmpty() && System.currentTimeMillis() < deadline) {
            selector.select(500);
            Iterator<SelectionKey> it = selector.selectedKeys().iterator();
            while (it.hasNext()) {
                SelectionKey key = it.next();
                it.remove();
                if (!key.isValid() || !key.isReadable()) {
                    continue;
                }
                Conn conn = (Conn) key.attachment();
                readBuf.clear();
                int n;
                try {
                    n = conn.ch.read(readBuf);
                } catch (IOException e) {
                    n = -1;
                }
                if (n < 0) {
                    closeConn(key, conn, "closed");
                }
            }
        }
    }

    static void closeAll() throws IOException {
        for (Conn c : conns) {
            c.close();
        }
        conns.clear();
        serverChannel.close();
        selector.close();
    }
}
