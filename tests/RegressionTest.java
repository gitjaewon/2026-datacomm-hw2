import java.nio.channels.SocketChannel;
import java.nio.channels.ServerSocketChannel;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

// 테스트에서만 별도 스레드를 사용한다. 실제 서버는 Worker 10 + Listener 1 + Notifier 1.
public class RegressionTest {
    private static int checks;

    public static void main(String[] args) throws Exception {
        Path dir = Path.of(args[0]);
        Files.createDirectories(dir);
        ClientMain.logDir = dir.toString();
        Server.log = new Log("SERVER", dir.resolve("RegressionServer.txt"));
        try (SocketChannel channel = SocketChannel.open()) {
            Conn conn = new Conn(channel);
            seatRules(conn);
            concurrentMulti();
            queueWakeup(conn);
            protocolParsing(conn);
            clientMessages();
            failedResponse();
            socketBackpressure();
        } finally {
            Server.log.close();
        }
        System.out.println("Regression checks passed: " + checks);
    }

    private static void resetSeats() {
        SeatManager.init();
        SeatManager.assigned.reset();
        SeatManager.released.reset();
        SeatManager.handoffs.reset();
        SeatManager.doubleBooking.reset();
        SeatManager.contention.reset();
    }

    private static void seatRules(Conn conn) {
        resetSeats();
        check(SeatManager.reserve(conn, 1, 1, 0).reason.equals("INVALID_SEAT"), "range check");
        check(SeatManager.reserve(conn, 1, 2, 1).result.equals("SUCCESS"), "initial assignment");
        check(SeatManager.reserve(conn, 1, 3, 1).reason.equals("ALREADY_OWNER"), "duplicate owner");
        check(SeatManager.reserve(conn, 2, 7, 1).result.equals("WAITLISTED"), "waitlist registration");
        check(SeatManager.reserve(conn, 3, 9, 1).waitPos == 2, "FIFO position");
        check(SeatManager.reserve(conn, 2, 8, 1).reason.equals("ALREADY_WAITING"), "duplicate waitlist");
        check(SeatManager.cancel(4, 1).reason.equals("NOT_OWNER"), "cancel by another client");
        check(SeatManager.reserveMulti(4, new int[]{2, 1}).reason.equals("TAKEN"), "atomic rejection");
        check(SeatManager.snapshot().owners()[2] == 0, "no partial assignment");
        check(SeatManager.snapshot().waits()[2] == 0, "multi does not join waitlist");
        SeatManager.Result first = SeatManager.cancel(1, 1);
        check(first.notifyJob.entry().clientId() == 2 && first.notifyJob.entry().reqId() == 7,
                "first handoff keeps original request ID");
        check(SeatManager.snapshot().owners()[1] == 2, "immediate handoff without empty gap");
        check(SeatManager.cancel(2, 1).notifyJob.entry().clientId() == 3, "second FIFO handoff");
        SeatManager.cancel(3, 1);
        check(SeatManager.assigned.sum() - SeatManager.released.sum() == 0, "assignment balance");
        check(SeatManager.doubleBooking.sum() == 0, "no double booking");
    }

    private static void concurrentMulti() throws Exception {
        resetSeats();
        var pool = Executors.newFixedThreadPool(10);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<SeatManager.Result>> results = new ArrayList<>();
        try {
            for (int n = 1; n <= 10; n++) {
                int id = n;
                results.add(pool.submit(() -> {
                    start.await();
                    return SeatManager.reserveMulti(id, id % 2 == 0 ? new int[]{5, 3} : new int[]{3, 5});
                }));
            }
            start.countDown();
            int successful = 0;
            for (Future<SeatManager.Result> future : results) {
                SeatManager.Result result = future.get(3, TimeUnit.SECONDS);
                if (result.result.equals("SUCCESS")) {
                    successful++;
                }
                check(result.lockOrder[0] == 3 && result.lockOrder[1] == 5, "ascending lock order");
            }
            check(successful == 1, "one winner for overlapping multi requests");
            SeatManager.Snapshot snapshot = SeatManager.snapshot();
            check(snapshot.owners()[3] == snapshot.owners()[5] && snapshot.reservedCount() == 2,
                    "atomic final owner");
            check(SeatManager.doubleBooking.sum() == 0 && snapshot.waitlistTotal() == 0, "no duplicate or multi waitlist");

            // 유효하지 않은 다중 요청은 다른 스레드가 좌석 lock을 쥐고 있어도 바로 실패해야 한다.
            SeatManager.seats[3].lock.lock();
            try {
                check(pool.submit(() -> SeatManager.reserveMulti(1, new int[]{3, 3}))
                        .get(1, TimeUnit.SECONDS).reason.equals("DUPLICATE_SEAT"), "validate before locking");
            } finally {
                SeatManager.seats[3].lock.unlock();
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private static void queueWakeup(Conn conn) throws Exception {
        RequestQueue queue = new RequestQueue(1);
        RequestQueue.Request request = new RequestQueue.Request(conn, 1, 1, "RESERVE", new int[]{1});
        AtomicReference<RequestQueue.Request> taken = new AtomicReference<>();
        Thread consumer = new Thread(() -> {
            try {
                taken.set(queue.take());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        consumer.start();
        check(queue.offer(request, 1000), "enqueue wakes consumer");
        consumer.join(1000);
        check(!consumer.isAlive() && taken.get() == request, "one dequeue");
        check(queue.offer(request, 1000), "queue accepts work before close");
        queue.close();
        check(queue.take() == request && queue.take() == null, "close drains queued work then exits");
        check(!queue.offer(request, 1000), "closed queue rejects new work");
    }

    private static void protocolParsing(Conn conn) throws Exception {
        Server.requestsPerClient = 10;
        Server.requestQueue = new RequestQueue(10);
        Listener.activeIds.clear();
        Listener.registeredIds.clear();
        Listener.handleHello(conn, new String[]{"HELLO", "31"});
        check(conn.clientId == 0 && Listener.activeIds.isEmpty(), "client ID upper bound");
        Listener.handleHello(conn, new String[]{"HELLO", "1", "extra"});
        check(conn.clientId == 0, "HELLO requires two fields");
        Listener.handleHello(conn, new String[]{"HELLO", "1"});
        Listener.handleLine(conn, "RESERVE_MULTI 1 1,2,");
        check(Server.requestQueue.take().seats() == null, "trailing comma is malformed");
        Listener.handleLine(conn, "RESERVE 1 3");
        Listener.handleLine(conn, "RESERVE 11 3");
        check(Server.requestQueue.size() == 0, "duplicate/out-of-range request IDs do not fill quota");
    }

    private static void register(Client client, int id, String type, int seat) {
        client.pending.put(id, new Client.Pending(type, new int[]{seat}, System.nanoTime()));
        client.sent++;
    }

    private static void resp(Client client, String message) {
        client.onResp(message.split(" "), message);
    }

    private static void notify(Client client, String message) {
        client.onNotify(message.split(" "), message);
    }

    private static void clientMessages() throws Exception {
        Client client = new Client(1);
        try {
            register(client, 1, "RESERVE", 42);
            notify(client, "NOTIFY 1 42");
            check(client.held.contains(42) && client.responded == 0 && client.notified == 1,
                    "early NOTIFY assigns seat but is not a first response");
            resp(client, "RESP 1 WAITLISTED");
            check(client.waitlisted == 1 && client.waiting.isEmpty() && client.notifiedBeforeResponse.isEmpty(),
                    "late WAITLISTED reconciles early NOTIFY");
            notify(client, "NOTIFY 1 42");
            notify(client, "NOTIFY 999 1");
            check(client.notified == 1 && !client.held.contains(1), "duplicate and unknown NOTIFY ignored");

            register(client, 2, "RESERVE", 5);
            resp(client, "RESP 2 WAITLISTED");
            notify(client, "NOTIFY 2 6");
            check(client.waiting.containsKey(2) && !client.held.contains(6), "NOTIFY must match seat and request ID");
            notify(client, "NOTIFY 2 5");
            check(client.waiting.isEmpty() && client.held.contains(5), "normal NOTIFY resolves waitlist");

            register(client, 3, "CANCEL", 42);
            client.cancelling.add(42);
            resp(client, "RESP 3 FAIL SERVER_ERROR");
            check(client.held.contains(42) && !client.cancelling.contains(42), "failed CANCEL preserves ownership");
            register(client, 4, "CANCEL", 42);
            resp(client, "RESP 4 WAITLISTED");
            resp(client, "RESP 4 MAYBE");
            check(client.pending.containsKey(4) && client.responded == 3, "invalid response does not consume request");
            resp(client, "RESP 4 SUCCESS");
            check(!client.held.contains(42), "successful CANCEL removes ownership");
        } finally {
            client.log.close();
        }
    }

    private static void failedResponse() throws Exception {
        resetSeats();
        Server.processed.set(0);
        Server.responseFailed.reset();
        Server.totalExpected = 30;
        Server.requestQueue = new RequestQueue(1);
        try (SocketChannel closed = SocketChannel.open()) {
            closed.close();
            Conn conn = new Conn(closed);
            Server.requestQueue.offer(new RequestQueue.Request(conn, 1, 1, "RESERVE", new int[]{1}), 1000);
            Server.requestQueue.close();
            Worker worker = new Worker(1);
            worker.start();
            worker.join(1000);
            check(!worker.isAlive(), "worker exits after draining closed queue");
            check(Server.processed.get() == 0 && Server.responseFailed.sum() == 1,
                    "failed socket send is not counted as completed response");
        }
    }

    private static void socketBackpressure() throws Exception {
        try (ServerSocketChannel listener = ServerSocketChannel.open()) {
            listener.bind(new InetSocketAddress("127.0.0.1", 0));
            try (SocketChannel peer = SocketChannel.open(listener.getLocalAddress());
                 SocketChannel sender = listener.accept()) {
                sender.configureBlocking(false);
                sender.setOption(StandardSocketOptions.SO_SNDBUF, 1024);
                peer.socket().setSoTimeout(5000);
                Conn conn = new Conn(sender);
                String payload = "x".repeat(1024 * 1024);
                AtomicReference<Boolean> delivered = new AtomicReference<>();
                Thread writer = new Thread(() -> delivered.set(conn.send(payload)));
                writer.start();
                writer.join(50);  // 수신 전 작은 송신 버퍼가 가득 차도록 한다.
                byte[] received = peer.socket().getInputStream().readNBytes(payload.length() + 1);
                writer.join(2000);
                check(!writer.isAlive() && Boolean.TRUE.equals(delivered.get()), "socket write resumes after backpressure");
                check(java.util.Arrays.equals(received, (payload + "\n").getBytes(StandardCharsets.UTF_8)),
                        "partial socket writes preserve complete message");
            }
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
        checks++;
    }
}
