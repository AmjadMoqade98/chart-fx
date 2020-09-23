package de.gsi.microservice.concepts.majordomo;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import de.gsi.microservice.rbac.BasicRbacRole;

/**
*  Majordomo Protocol broker
*  A minimal implementation of http://rfc.zeromq.org/spec:7 and spec:8
*/
public class MajordomoBroker {
    // We'd normally pull these from config data
    private static final String INTERNAL_SERVICE_PREFIX = "mmi.";
    private static final int HEARTBEAT_LIVENESS = 3; // 3-5 is reasonable
    private static final int HEARTBEAT_INTERVAL = 2500; // msecs
    private static final int HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS;

    // ---------------------------------------------------------------------

    /**
     * This defines a single service.
     */
    private static class Service {
        public final String name; // Service name
        Deque<ZMsg> requests; // List of client requests
        Deque<Worker> waiting; // List of waiting workers

        public Service(String name) {
            this.name = name;
            this.requests = new ArrayDeque<>();
            this.waiting = new ArrayDeque<>();
        }
    }

    /**
     * This defines one worker, idle or active.
     */
    private static class Worker {
        final String identity; // Identity of worker
        final ZFrame address; // Address frame to route to
        final ZMQ.Socket socket; // Socket worker is connected to
        Service service; // Owning service, if known
        long expiry; // Expires at unless heartbeat

        public Worker(final ZMQ.Socket socket, final String identity, final ZFrame address) {
            this.socket = socket;
            this.address = address;
            this.identity = identity;
            this.expiry = System.currentTimeMillis() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS;
        }
    }

    // ---------------------------------------------------------------------

    private final ZContext ctx; // Our context
    private final ZMQ.Socket internalSocket;

    private final List<ZMQ.Socket> routerSockets = new ArrayList<>(); // Sockets for clients & public external workers
    private final Map<String, ZMQ.Socket> clientSockets = new HashMap<>();
    private long heartbeatAt; // When to send HEARTBEAT
    private final Map<String, Service> services = new HashMap<>(); // known services
    private final Map<String, Worker> workers = new HashMap<>(); // known workers
    private final Deque<Worker> waiting = new ArrayDeque<>(); // idle workers

    private boolean verbose; // Print activity to stdout
    private final Formatter log = new Formatter(System.out);

    // ---------------------------------------------------------------------

    /**
     * Main method - create and start new broker.
     */
    public static void main(String[] args) {
        final boolean verbose = args.length > 0 && "-v".equals(args[0]);
        MajordomoBroker broker = new MajordomoBroker(verbose);
        // Can be called multiple times with different endpoints
        broker.bind("tcp://*:5555");
        broker.bind("tcp://*:5556");

        // simple internalSock echo
        MajordomoWorker workerSession = new MajordomoWorker(broker.getContext(), "inproc.echo", verbose, BasicRbacRole.ADMIN);
        workerSession.setDaemon(true);
        workerSession.registerHandler(input -> input); //  output = input : echo service is complex :-)
        workerSession.start();

        broker.mediate();
    }

    /**
     * Initialize broker state.
     */
    public MajordomoBroker(boolean verbose) {
        this.verbose = verbose;
        this.heartbeatAt = System.currentTimeMillis() + HEARTBEAT_INTERVAL;
        this.ctx = new ZContext();
        this.internalSocket = ctx.createSocket(SocketType.ROUTER);
        internalSocket.bind("inproc://broker");
    }

    // ---------------------------------------------------------------------

    /**
     * main broker work happens here
     */
    public void mediate() {
        while (!Thread.currentThread().isInterrupted()) {
            ZMQ.Poller items = ctx.createPoller(routerSockets.size() + 1);
            items.register(internalSocket, ZMQ.Poller.POLLIN);
            for (ZMQ.Socket routerSocket : routerSockets) {
                items.register(routerSocket, ZMQ.Poller.POLLIN);
            }

            if (items.poll(HEARTBEAT_INTERVAL) == -1) {
                break; // interrupted
            }

            if (items.pollin(0)) { // data arrived on internal worker socket
                if (handleReceivedMessage(internalSocket)) {
                    break;
                }
            }

            int socketCount = 1;
            for (ZMQ.Socket routerSocket : routerSockets) {
                if (items.pollin(socketCount++)) { // data arrived on external router socket
                    if (handleReceivedMessage(routerSocket)) {
                        break;
                    }
                }
            }

            items.close();
            purgeWorkers();
            sendHeartbeats();
        }
        destroy(); // interrupted
    }

    private boolean handleReceivedMessage(final ZMQ.Socket receiveSocket) {
        final ZMsg msg = ZMsg.recvMsg(receiveSocket);
        if (msg == null) {
            return true; // interrupted
        }
        //ZMsg msgCopy = msg.duplicate();

        if (verbose) {
            log.format("I: received message:\n");
            msg.dump(log.out());
        }

        ZFrame sender = msg.pop();
        ZFrame empty = msg.pop();
        ZFrame header = msg.pop();

        if (MajordomoProtocol.C_CLIENT.frameEquals(header)) {
            processClient(receiveSocket, sender, msg);
        } else if (MajordomoProtocol.W_WORKER.frameEquals(header)) {
            processWorker(receiveSocket, sender, msg);
        } else {
            log.format("E: invalid message:\n");
            msg.dump(log.out());
            msg.destroy();
        }

        sender.destroy();
        empty.destroy();
        header.destroy();
        return false;
    }

    /**
     * Disconnect all workers, destroy context.
     */
    private void destroy() {
        Worker[] deleteList = workers.values().toArray(new Worker[0]);
        for (Worker worker : deleteList) {
            deleteWorker(worker, true);
        }
        ctx.destroy();
    }

    /**
     * Process a request coming from a client.
     */
    private void processClient(final ZMQ.Socket receiveSocket, ZFrame sender, ZMsg msg) {
        assert (msg.size() >= 2); // Service name + body
        ZFrame serviceFrame = msg.pop();
        // Set reply return address to client sender
        clientSockets.put(sender.duplicate().toString(), receiveSocket);
        msg.wrap(sender.duplicate());
        if (serviceFrame.toString().startsWith(INTERNAL_SERVICE_PREFIX)) {
            serviceInternal(serviceFrame, msg);
        } else {
            dispatch(requireService(serviceFrame), msg);
        }
        serviceFrame.destroy();
    }

    /**
     * Process message sent to us by a worker.
     */
    private void processWorker(final ZMQ.Socket receiveSocket, final ZFrame sender, final ZMsg msg) {
        assert (msg.size() >= 1); // At least, command

        ZFrame command = msg.pop();

        boolean workerReady = workers.containsKey(sender.strhex());

        Worker worker = requireWorker(receiveSocket, sender);

        if (MajordomoProtocol.W_READY.frameEquals(command)) {
            // Not first command in session || Reserved service name
            if (workerReady || sender.toString().startsWith(INTERNAL_SERVICE_PREFIX))
                deleteWorker(worker, true);
            else {
                // Attach worker to service and mark as idle
                ZFrame serviceFrame = msg.pop();
                worker.service = requireService(serviceFrame);
                workerWaiting(worker);
                serviceFrame.destroy();
            }
        } else if (MajordomoProtocol.W_REPLY.frameEquals(command)) {
            if (workerReady) {
                // Remove & save client return envelope and insert the
                // protocol header and service name, then rewrap envelope.
                final ZFrame client = msg.unwrap();
                final ZMQ.Socket socket = clientSockets.get(client.toString());
                if (socket != null) {
                    msg.addFirst(worker.service.name);
                    msg.addFirst(MajordomoProtocol.C_CLIENT.newFrame());
                    msg.wrap(client);
                    msg.send(socket);
                }
                workerWaiting(worker);
            } else {
                deleteWorker(worker, true);
            }
        } else if (MajordomoProtocol.W_HEARTBEAT.frameEquals(command)) {
            if (workerReady) {
                worker.expiry = System.currentTimeMillis() + HEARTBEAT_EXPIRY;
            } else {
                deleteWorker(worker, true);
            }
        } else if (MajordomoProtocol.W_DISCONNECT.frameEquals(command))
            deleteWorker(worker, false);
        else {
            log.format("E: invalid message:\n");
            msg.dump(log.out());
        }
        msg.destroy();
    }

    /**
     * Deletes worker from all data structures, and destroys worker.
     */
    private void deleteWorker(Worker worker, boolean disconnect) {
        assert (worker != null);
        if (disconnect) {
            sendToWorker(worker, MajordomoProtocol.W_DISCONNECT, null, null);
        }
        if (worker.service != null) {
            worker.service.waiting.remove(worker);
        }
        workers.remove(worker.identity);
        worker.address.destroy();
    }

    /**
     * Finds the worker (creates if necessary).
     */
    private Worker requireWorker(final ZMQ.Socket socket, ZFrame address) {
        assert (address != null);
        return workers.computeIfAbsent(address.strhex(), identity -> {
            if (verbose) {
                log.format("I: registering new worker: %s\n", identity);
            }
            return new Worker(socket, identity, address.duplicate());
        });
    }

    /**
     * Locates the service (creates if necessary).
     */
    private Service requireService(ZFrame serviceFrame) {
        assert (serviceFrame != null);
        String name = serviceFrame.toString();
        Service service = services.get(name);
        if (service == null) {
            service = new Service(name);
            services.put(name, service);
        }
        return service;
    }

    /**
     * Bind broker to endpoint, can call this multiple times. We use a single
     * socket for both clients and workers.
     */
    private void bind(String endpoint) {
        final ZMQ.Socket routerSocket = ctx.createSocket(SocketType.ROUTER);
        routerSocket.bind(endpoint);
        routerSockets.add(routerSocket);
        log.format("I: MajordomoProtocol broker/0.1.1 is active at %s\n", endpoint);
    }

    public ZContext getContext() {
        return ctx;
    }

    public ZMQ.Socket getInternalSocket() {
        return internalSocket;
    }

    /**
     *
     * @return unmodifiable list of registered external sockets
     */
    public List<ZMQ.Socket> getRouterSockets() {
        return Collections.unmodifiableList(routerSockets);
    }

    /**
     * Handle internal service according to 8/MMI specification
     */
    private void serviceInternal(ZFrame serviceFrame, ZMsg msg) {
        String returnCode = "501";
        if ("mmi.service".equals(serviceFrame.toString())) {
            String name = msg.peekLast().toString();
            returnCode = services.containsKey(name) ? "200" : "400";
            msg.peekLast().reset(returnCode.getBytes(StandardCharsets.UTF_8));
        } else if ("mmi.echo".equals(serviceFrame.toString())) {
            msg.peekLast().reset(msg.peekLast().getData());
        } else {
            msg.peekLast().reset(returnCode.getBytes(StandardCharsets.UTF_8));
        }
        // Remove & save client return envelope and insert the
        // protocol header and service name, then rewrap envelope.
        final ZFrame client = msg.unwrap();
        final ZMQ.Socket clientSocket = clientSockets.get(client.toString());
        if (clientSocket != null) {
            msg.addFirst(serviceFrame.duplicate());
            msg.addFirst(MajordomoProtocol.C_CLIENT.newFrame());
            msg.wrap(client);
            msg.send(clientSocket);
        }
    }

    /**
     * Send heartbeats to idle workers if it's time
     */
    public synchronized void sendHeartbeats() {
        // Send heartbeats to idle workers if it's time
        if (System.currentTimeMillis() >= heartbeatAt) {
            for (Worker worker : waiting) {
                sendToWorker(worker, MajordomoProtocol.W_HEARTBEAT, null, null);
            }
            heartbeatAt = System.currentTimeMillis() + HEARTBEAT_INTERVAL;
        }
    }

    /**
     * Look for &amp; kill expired workers. Workers are oldest to most recent, so we
     * stop at the first alive worker.
     */
    public synchronized void purgeWorkers() {
        for (Worker w = waiting.peekFirst(); w != null
                                             && w.expiry < System.currentTimeMillis();
                w = waiting.peekFirst()) {
            log.format("I: deleting expired worker: %s - service: %s\n", w.identity, w.service == null ? "(unknown)" : w.service.name);
            deleteWorker(waiting.pollFirst(), false);
        }
    }

    /**
     * This worker is now waiting for work.
     */
    public synchronized void workerWaiting(Worker worker) {
        // Queue to broker and service waiting lists
        waiting.addLast(worker);
        worker.service.waiting.addLast(worker);
        worker.expiry = System.currentTimeMillis() + HEARTBEAT_EXPIRY;
        dispatch(worker.service, null);
    }

    /**
     * Dispatch requests to waiting workers as possible
     */
    private void dispatch(Service service, ZMsg msg) {
        assert (service != null);
        if (msg != null) { // Queue message if any
            service.requests.offerLast(msg);
        }
        purgeWorkers();
        while (!service.waiting.isEmpty() && !service.requests.isEmpty()) {
            msg = service.requests.pop();
            Worker worker = service.waiting.pop();
            waiting.remove(worker);
            sendToWorker(worker, MajordomoProtocol.W_REQUEST, null, msg);
            msg.destroy();
        }
    }

    /**
     * Send message to worker. If message is provided, sends that message. Does
     * not destroy the message, this is the caller's job.
     */
    public void sendToWorker(Worker worker, MajordomoProtocol command, String option, ZMsg msgp) {
        ZMsg msg = msgp == null ? new ZMsg() : msgp.duplicate();

        // Stack protocol envelope to start of message
        if (option != null)
            msg.addFirst(new ZFrame(option));
        msg.addFirst(command.newFrame());
        msg.addFirst(MajordomoProtocol.W_WORKER.newFrame());

        // Stack routing envelope to start of message
        msg.wrap(worker.address.duplicate());
        if (verbose) {
            log.format("I: sending %s to worker - service: %s\n", command, worker.service == null ? " (unknown)" : worker.service.name);
            msg.dump(log.out());
        }
        msg.send(worker.socket);
    }
}
