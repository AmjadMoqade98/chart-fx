package de.gsi.microservice.concepts;

import org.zeromq.*;
import org.zeromq.ZMQ.Socket;

/**
 * Quick Router-Dealer Round-trip demonstrator.
 * Broker, Worker and Client are mocked as separate threads.
 *
 * Example output:
 * Setting up test
 * Synchronous round-trip test
 *  3206 calls/second
 * Asynchronous round-trip test
 *  21834 calls/second
 * subscription (SUB) test
 *  689655 calls/second
 * subscription (DEALER) test
 *  30358 calls/second
 * subscription (direct DEALER) test
 *  862068 calls/second
 *
 * N.B. for >200000 calls/second the code seems to depend largely on the broker/parameters
 * (ie. JIT, whether services are identified by single characters etc.)
 */
public class RoundTripAndNotifyTests {
    private static int SAMPLE_SIZE = 10_000;
    private static int SAMPLE_SIZE_PUB = 100_000;
    // private static final String SUB_TOPIC = "x";
    private static final String SUB_TOPIC = "<domain>/<property>?<filter>#<ctx> - a very long topic to test the dependence of pub/sub pairs on topic lengths";
    private static final byte[] SUB_DATA = "D".getBytes(ZMQ.CHARSET); // custom minimal data
    private static final byte[] CLIENT_ID = "C".getBytes(ZMQ.CHARSET); // client name
    private static final byte[] WORKER_ID = "W".getBytes(ZMQ.CHARSET); // worker-service name
    private static final byte[] PUBLISH_ID = "P".getBytes(ZMQ.CHARSET); // publish-service name
    private static final byte[] SUBSCRIBER_ID = "S".getBytes(ZMQ.CHARSET); // subscriber name

    static class Broker implements Runnable {
        @Override
        public void run() {
            try (ZContext ctx = new ZContext()) {
                Socket frontend = ctx.createSocket(SocketType.ROUTER);
                Socket backend = ctx.createSocket(SocketType.ROUTER);
                frontend.setHWM(0);
                backend.setHWM(0);
                frontend.bind("tcp://*:5555");
                backend.bind("tcp://*:5556");

                while (!Thread.currentThread().isInterrupted()) {
                    ZMQ.Poller items = ctx.createPoller(2);
                    items.register(frontend, ZMQ.Poller.POLLIN);
                    items.register(backend, ZMQ.Poller.POLLIN);

                    if (items.poll() == -1)
                        break; // Interrupted

                    if (items.pollin(0)) {
                        ZMsg msg = ZMsg.recvMsg(frontend);
                        if (msg == null)
                            break; // Interrupted
                        ZFrame address = msg.pop();
                        if (address.getData()[0] == CLIENT_ID[0]) {
                            msg.addFirst(new ZFrame(WORKER_ID));
                        } else {
                            msg.addFirst(new ZFrame(PUBLISH_ID));
                        }
                        address.destroy();
                        msg.send(backend);
                    }

                    if (items.pollin(1)) {
                        ZMsg msg = ZMsg.recvMsg(backend);
                        if (msg == null)
                            break; // Interrupted
                        ZFrame address = msg.pop();

                        if (address.getData()[0] == WORKER_ID[0]) {
                            msg.addFirst(new ZFrame(CLIENT_ID));
                        } else {
                            msg.addFirst(new ZFrame(SUBSCRIBER_ID));
                        }
                        msg.send(frontend);
                        address.destroy();
                    }

                    items.close();
                }
            }
        }
    }

    static class Worker implements Runnable {
        @Override
        public void run() {
            try (ZContext ctx = new ZContext()) {
                Socket worker = ctx.createSocket(SocketType.DEALER);
                worker.setHWM(0);
                worker.setIdentity(WORKER_ID);
                worker.connect("tcp://localhost:5556");
                while (!Thread.currentThread().isInterrupted()) {
                    ZMsg msg = ZMsg.recvMsg(worker);
                    msg.send(worker);
                }
            } catch (ZMQException e) {
                // terminate worker
            }
        }
    }

    static class PublishWorker implements Runnable {
        @Override
        public void run() {
            try (ZContext ctx = new ZContext()) {
                Socket worker = ctx.createSocket(SocketType.PUB);
                worker.setHWM(0);
                worker.bind("tcp://localhost:5557");
                // System.err.println("PublishWorker: start publishing");
                while (!Thread.currentThread().isInterrupted()) {
                    worker.send(SUB_TOPIC, ZMQ.SNDMORE);
                    worker.send(SUB_DATA);
                }
            } catch (ZMQException | IllegalStateException e) {
                // terminate pub-Dealer worker
            }
        }
    }

    static class PublishDealerWorker implements Runnable {
        @Override
        public void run() {
            try (ZContext ctx = new ZContext()) {
                Socket worker = ctx.createSocket(SocketType.DEALER);
                worker.setHWM(0);
                worker.setIdentity(PUBLISH_ID);
                //worker.bind("tcp://localhost:5558");
                worker.connect("tcp://localhost:5556");
                while (!Thread.currentThread().isInterrupted()) {
                    ZMsg msg = ZMsg.recvMsg(worker);
                    if ("start".equals(msg.getFirst().getString(ZMQ.CHARSET))) {
                        // System.err.println("dealer (indirect): start pushing");
                        for (int requests = 0; requests < SAMPLE_SIZE_PUB; requests++) {
                            worker.send(SUB_TOPIC, ZMQ.SNDMORE);
                            worker.send(SUB_DATA);
                        }
                    }
                }
            } catch (ZMQException | IllegalStateException e) {
                // terminate publish worker
            }
        }
    }

    static class PublishDirectDealerWorker implements Runnable {
        @Override
        public void run() {
            try (ZContext ctx = new ZContext()) {
                Socket worker = ctx.createSocket(SocketType.DEALER);
                worker.setHWM(0);
                worker.setIdentity(PUBLISH_ID);
                worker.bind("tcp://localhost:5558");
                while (!Thread.currentThread().isInterrupted()) {
                    ZMsg msg = ZMsg.recvMsg(worker);
                    if ("start".equals(msg.getFirst().getString(ZMQ.CHARSET))) {
                        // System.err.println("dealer (direct): start pushing");
                        for (int requests = 0; requests < SAMPLE_SIZE_PUB; requests++) {
                            worker.send(SUB_TOPIC, ZMQ.SNDMORE);
                            worker.send(SUB_DATA);
                        }
                    }
                }
            } catch (ZMQException | IllegalStateException e) {
                // terminate publish worker
            }
        }
    }

    static class Client implements Runnable {
        @Override
        public void run() {
            try (ZContext ctx = new ZContext()) {
                Socket client = ctx.createSocket(SocketType.DEALER);
                client.setHWM(0);
                client.setIdentity(CLIENT_ID);
                client.connect("tcp://localhost:5555");

                Socket subClient = ctx.createSocket(SocketType.SUB);
                subClient.setHWM(0);
                subClient.connect("tcp://localhost:5557");
                System.out.println("Setting up test");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                long start;

                System.out.println("Synchronous round-trip test");
                start = System.currentTimeMillis();

                for (int requests = 0; requests < SAMPLE_SIZE; requests++) {
                    ZMsg req = new ZMsg();
                    req.addString("hello");
                    req.send(client);
                    ZMsg.recvMsg(client).destroy();
                }

                long now = System.currentTimeMillis();
                System.out.printf(
                        " %d calls/second\n", (1000 * SAMPLE_SIZE) / (now - start));

                System.out.println("Asynchronous round-trip test");
                start = System.currentTimeMillis();

                for (int requests = 0; requests < SAMPLE_SIZE; requests++) {
                    ZMsg req = new ZMsg();
                    req.addString("hello");
                    req.send(client);
                }

                for (int requests = 0;
                        requests < SAMPLE_SIZE && !Thread.currentThread().isInterrupted();
                        requests++) {
                    ZMsg.recvMsg(client).destroy();
                }

                long now2 = System.currentTimeMillis();
                System.out.printf(" %d calls/second\n", (1000 * SAMPLE_SIZE) / (now2 - start));

                System.out.println("subscription (SUB) test");
                subClient.subscribe(SUB_TOPIC.getBytes(ZMQ.CHARSET));
                // first loop to empty potential queues/HWM
                for (int requests = 0; requests < SAMPLE_SIZE_PUB; requests++) {
                    ZMsg req = ZMsg.recvMsg(subClient);
                    req.destroy();
                }
                // start actual subscription loop
                start = System.currentTimeMillis();
                for (int requests = 0; requests < SAMPLE_SIZE_PUB; requests++) {
                    ZMsg req = ZMsg.recvMsg(subClient);
                    req.destroy();
                }
                long now3 = System.currentTimeMillis();
                subClient.unsubscribe(SUB_TOPIC.getBytes(ZMQ.CHARSET));
                System.out.printf(" %d calls/second\n", (1000 * SAMPLE_SIZE_PUB) / (now3 - start));

                System.out.println("subscription (DEALER) test");
                client.disconnect("tcp://localhost:5555");
                client.setIdentity(SUBSCRIBER_ID);
                client.connect("tcp://localhost:5555");
                ZMsg.newStringMsg("start").send(client);
                start = System.currentTimeMillis();
                for (int requests = 0; requests < SAMPLE_SIZE_PUB; requests++) {
                    ZMsg req = ZMsg.recvMsg(client);
                    req.destroy();
                }
                long now4 = System.currentTimeMillis();
                System.out.printf(" %d calls/second\n", (1000 * SAMPLE_SIZE_PUB) / (now4 - start));

                System.out.println("subscription (direct DEALER) test");
                client.disconnect("tcp://localhost:5555");
                client.connect("tcp://localhost:5558");
                ZMsg.newStringMsg("start").send(client);
                start = System.currentTimeMillis();
                for (int requests = 0; requests < SAMPLE_SIZE_PUB; requests++) {
                    ZMsg req = ZMsg.recvMsg(client);
                    req.destroy();
                }
                long now5 = System.currentTimeMillis();
                System.out.printf(" %d calls/second\n", (1000 * SAMPLE_SIZE_PUB) / (now5 - start));

            } catch (ZMQException e) {
                System.out.println("terminate client");
            }
        }
    }

    public static void main(String[] args) {
        if (args.length == 1) {
            SAMPLE_SIZE = Integer.parseInt(args[0]);
            SAMPLE_SIZE_PUB = 10 * SAMPLE_SIZE;
        }

        Thread brokerThread = new Thread(new Broker());
        Thread workerThread = new Thread(new Worker());
        Thread publishThread = new Thread(new PublishWorker());
        Thread pubDealerThread = new Thread(new PublishDealerWorker());
        Thread directDealerThread = new Thread(new PublishDirectDealerWorker());

        Thread clientThread = new Thread(new Client());

        brokerThread.setDaemon(true);
        workerThread.setDaemon(true);
        publishThread.setDaemon(true);
        pubDealerThread.setDaemon(true);
        directDealerThread.setDaemon(true);

        brokerThread.start();
        workerThread.start();
        publishThread.start();
        pubDealerThread.start();
        directDealerThread.start();

        clientThread.start();

        try {
            clientThread.join();
            workerThread.interrupt();
            brokerThread.interrupt();
            publishThread.interrupt();
            pubDealerThread.interrupt();
            directDealerThread.interrupt();
            Thread.sleep(200); // give them some time
        } catch (InterruptedException e) {
            // finishes tests
        }
    }
}
