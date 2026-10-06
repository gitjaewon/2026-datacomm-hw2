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

/**
 * Listener (1개). 별도 스레드를 만들지 않고 Server.main()의 main 스레드가 이 루프를 직접 돈다.
 *
 * 하는 일 (명세 §1-①)
 *   - Client 접속 수락
 *   - NIO Selector 하나로 소켓 30개를 동시에 감시 (연결마다 스레드를 만들지 않는다)
 *   - 받은 바이트를 줄 단위로 잘라 파싱 → Request Queue에 적재. 좌석은 직접 건드리지 않는다.
 *   - select()를 0.5초마다 깨워서 5초 POOL 로그와 Deadlock 감시도 여기서 한다 (Monitor 스레드 없음)
 *   - 종료 시 BYE 전송, 연결 정리
 */
public class Listener {

    static Selector selector;
    static ServerSocketChannel serverChannel;
    static final List<Conn> conns = new ArrayList<>();     // Listener만 접근
    static final Set<Integer> activeIds = new HashSet<>(); // HELLO를 마친 Client 번호
    static final ByteBuffer readBuf = ByteBuffer.allocate(8192);
    static volatile boolean shutdownRequested;
    static volatile String shutdownReason = "";
    static long nextPoolAt;
    static long lastProcessedSeen = -1;
    static long lastProgressAt;
    static boolean stallReported;

    /**
     * 서버 소켓을 열고(포트 bind) 감시자(Selector)에 "새 접속 알려줘"로 등록한다.
     * 서버 시작 시 1번만 실행된다. 포트는 1개, Client 30명 모두 이 포트로 접속한다.
     */
    static void openServerSocket() throws IOException {
        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverChannel.bind(new InetSocketAddress(Server.host, Server.port));
        serverChannel.configureBlocking(false);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    /** Worker가 마지막 응답을 보낸 뒤 호출. select()에서 자고 있는 Listener를 깨운다. */
    static void requestShutdown(String reason) {
        if (shutdownRequested) {
            return;
        }
        shutdownReason = reason;
        shutdownRequested = true;
        selector.wakeup();
    }

    /**
     * Listener 메인 루프. select()로 최대 0.5초 기다렸다가
     * 새 접속이면 accept(), 데이터가 왔으면 read(), 그다음 tick()(POOL·Deadlock 감시).
     * 종료 요청(requestShutdown)이 오면 빠져나온다.
     */
    static void listenLoop() throws IOException {
        nextPoolAt = System.currentTimeMillis() + Server.poolSec * 1000L;
        lastProgressAt = System.currentTimeMillis();
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

    /**
     * 새 Client 접속을 받아 연결 소켓(Conn)을 만들고 Selector에 "데이터 오면 알려줘"로 등록한다.
     * 접속이 올 때마다 실행된다 (30명이면 연결 소켓 30개).
     */
    static void accept() throws IOException {
        SocketChannel ch;
        while ((ch = serverChannel.accept()) != null) {
            ch.configureBlocking(false);
            ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
            Conn conn = new Conn(ch);
            ch.register(selector, SelectionKey.OP_READ, conn);
            conns.add(conn);
            Server.log.write("CONNECT", "INFO", "New TCP connection accepted, waiting for HELLO. open_connections=" + conns.size() + ".");
        }
    }

    /**
     * TCP는 메시지 경계가 없어서 한 번에 여러 메시지가 붙어 오거나 한 메시지가 나뉘어 올 수 있다.
     * 그래서 받은 내용을 Client별 버퍼(inbox)에 쌓고, '\n'이 나올 때마다 한 줄씩 꺼내 처리한다.
     */
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
        int nl;
        while ((nl = conn.inbox.indexOf("\n")) >= 0) {
            String line = conn.inbox.substring(0, nl).trim(); // trim으로 '\r'도 제거
            conn.inbox.delete(0, nl + 1);
            handleLine(conn, line);
        }
        if (conn.inbox.length() > 64 * 1024) {
            closeConn(key, conn, "line too long"); // 줄바꿈 없이 계속 들어오는 비정상 입력
        }
    }

    /**
     * 메시지 한 줄을 파싱한다. HELLO면 handleHello(), 요청(RESERVE/RESERVE_MULTI/CANCEL)이면
     * Request로 만들어 Request Queue에 넣는다. 형식이 잘못된 줄은 로그만 남기고 버린다.
     */
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
            // 요청 번호가 없으면 응답도 못 보내므로 버린다
            Server.log.write("CONNECT", "WARN", conn.name() + " request without valid reqId ignored: \"" + line + "\".");
            return;
        }
        // 좌석을 숫자로 못 읽으면 seats=null → Worker가 FAIL(BAD_FORMAT)로 응답
        int[] seatArr = null;
        if (t.length == 3) {
            try {
                seatArr = cmd.equals("RESERVE_MULTI")
                        ? Arrays.stream(t[2].split(",")).mapToInt(Integer::parseInt).toArray()
                        : new int[] {Integer.parseInt(t[2])};
            } catch (NumberFormatException e) {
                seatArr = null;
            }
        }
        enqueue(new RequestQueue.Request(conn, conn.clientId, reqId, cmd, seatArr));
    }

    /**
     * HELLO <id> 처리. 이 연결에 Client 번호를 붙이고 접속 수를 센다.
     * 첫 Client 접속 시각을 처리량 계산 시작 시각으로 기록한다.
     */
    static void handleHello(Conn conn, String[] t) {
        int id;
        try {
            id = Integer.parseInt(t[1]);
        } catch (RuntimeException e) {
            Server.log.write("CONNECT", "WARN", "HELLO with invalid client id ignored.");
            return;
        }
        if (conn.clientId != 0 || id <= 0 || !activeIds.add(id)) {
            Server.log.write("CONNECT", "WARN", "HELLO rejected: Client" + id + " is invalid or already connected.");
            return;
        }
        conn.clientId = id;
        if (Server.startMillis == 0) {
            Server.startMillis = System.currentTimeMillis(); // 처리량 계산 시작 = 첫 Client 연결
        }
        Server.log.write("CONNECT", "SUCCESS", "Client" + id + " connected (" + activeIds.size() + "/" + Server.expectedClients + ").");
        if (activeIds.size() == Server.expectedClients) {
            Server.log.console("CONNECT", "SUCCESS", "All clients connected (" + Server.expectedClients + "/" + Server.expectedClients + ").");
        }
    }

    /** 큐가 가득 차면 자리가 날 때까지 기다리되, 1초마다 깨어나 POOL/Deadlock 감시는 계속한다. */
    static void enqueue(RequestQueue.Request req) {
        try {
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

    /** 주기 작업: Deadlock 감시 + 5초마다 POOL 로그 */
    static void tick() {
        long now = System.currentTimeMillis();
        long done = Server.processed.get();
        int queued = Server.requestQueue.size();

        // Deadlock 감시: 큐에 요청이 있는데 deadlockSec(기본 30초) 동안 처리 수가 한 건도 안 늘면 1회로 센다.
        // (처리 수가 늘었거나 큐가 비어 있으면 = 정상)
        if (done != lastProcessedSeen || queued == 0) {
            lastProcessedSeen = done;
            lastProgressAt = now;
            stallReported = false;
        } else if (!stallReported && now - lastProgressAt >= Server.deadlockSec * 1000L) {
            Server.deadlockCount.increment();
            stallReported = true; // 같은 정지 구간을 여러 번 세지 않도록
            Server.log.console("POOL", "WARN", "No progress for " + Server.deadlockSec + "s while queue=" + queued
                    + ". Possible deadlock (count=" + Server.deadlockCount.sum() + ").");
        }

        if (now >= nextPoolAt) {
            nextPoolAt = now + Server.poolSec * 1000L;
            SeatManager.Snapshot snap = SeatManager.snapshot();
            // 5초마다 찍혀서 화면에 내보내면 너무 많다 → 파일에만 기록 (진행 상황은 Server.txt에서 확인)
            Server.log.write("POOL", "INFO", String.format(
                    "queue=%d max_queue=%d processed=%d reserved=%d/%d waitlist_total=%d contention=%d seats=%s",
                    queued, Server.requestQueue.maxSize(), done, snap.reservedCount(), SeatManager.SEAT_COUNT,
                    snap.waitlistTotal(), SeatManager.contention.sum(), snap.bitmap()));
        }
    }

    /** 연결 하나를 닫고 목록에서 뺀다. 실행 도중 모든 Client가 끊기면 종료 절차를 요청한다. */
    static void closeConn(SelectionKey key, Conn conn, String reason) {
        key.cancel();
        conn.close();
        conns.remove(conn);
        if (conn.clientId == 0) {
            return;
        }
        activeIds.remove(conn.clientId);
        if (shutdownRequested) {
            Server.log.write("CONNECT", "INFO", conn.name() + " disconnected after termination signal.");
            return;
        }
        Server.log.console("CONNECT", "WARN", conn.name() + " disconnected (" + reason + "). active=" + activeIds.size() + ".");
        if (activeIds.isEmpty()) {
            // 모든 Client가 중간에 끊기면 150,000건을 영원히 못 채우므로 종료 절차로 넘어간다
            requestShutdown("all clients disconnected before finishing");
        }
    }

    /** 모든 Client에게 종료 신호(BYE)를 보낸다. 보낸 Client 수를 돌려준다. */
    static int broadcastBye() {
        int sent = 0;
        for (Conn c : conns) {
            if (c.clientId != 0 && c.send("BYE")) {
                sent++;
            }
        }
        return sent;
    }

    /** BYE 후 Client들이 스스로 끊을 때까지 최대 timeoutMs 기다린다 (들어오는 데이터는 버림). */
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

    /** 남은 연결 소켓, 서버 소켓, Selector를 모두 닫는다 (종료 마지막 단계). */
    static void closeAll() throws IOException {
        for (Conn c : conns) {
            c.close();
        }
        conns.clear();
        serverChannel.close();
        selector.close();
    }
}
