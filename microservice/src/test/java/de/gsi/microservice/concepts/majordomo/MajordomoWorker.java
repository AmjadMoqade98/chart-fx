package de.gsi.microservice.concepts.majordomo;

import java.lang.management.ManagementFactory;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import de.gsi.microservice.rbac.BasicRbacRole;
import de.gsi.microservice.rbac.RbacRole;
import de.gsi.microservice.rbac.RbacToken;

/**
* Majordomo Protocol Client API, Java version Implements the MajordomoProtocol/Worker spec at
* http://rfc.zeromq.org/spec:7.
 *
 * Changes:
 * * role-based priority queues
 * * multiple threads
 *
*/
public class MajordomoWorker extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(MajordomoWorker.class);
    private static final int HEARTBEAT_LIVENESS = 3; // 3-5 is reasonable
    private final String uniqueID;
    private final boolean verbose; // Print activity to stdout
    private final Formatter log = new Formatter(System.out);

    private final String brokerAddress;
    private final ZContext ctx;
    private final String service;
    private final long timeout = 2500;
    private final SortedSet<RbacRole<?>> rbacRoles;
    private final Map<RbacRole<?>, Queue<ZMsg>> requestQueues = new HashMap<>();
    private final Queue<ZMsg> replyQueue = new ArrayDeque<>();
    private ZMQ.Socket worker; // Socket to broker
    private long heartbeatAt; // When to send HEARTBEAT
    private int liveness; // How many attempts left
    private int heartbeat = 2500; // Heartbeat delay, msecs
    private int reconnect = 2500; // Reconnect delay, msecs
    private RequestHandler requestHandler;

    public MajordomoWorker(String brokerAddress, String serviceName, boolean verbose, RbacRole<?>... rbacRoles) {
        this(null, brokerAddress, serviceName, verbose, rbacRoles);
    }

    public MajordomoWorker(ZContext ctx, String serviceName, boolean verbose, RbacRole<?>... rbacRoles) {
        this(ctx, "inproc://broker", serviceName, verbose, rbacRoles);
    }

    protected MajordomoWorker(ZContext ctx, String brokerAddress, String serviceName, boolean verbose, RbacRole<?>... rbacRoles) {
        assert (brokerAddress != null);
        assert (serviceName != null);
        this.brokerAddress = brokerAddress;
        this.service = serviceName;
        this.verbose = verbose;
        this.ctx = Objects.requireNonNullElseGet(ctx, ZContext::new);

        // initialise RBAC role-based priority queues
        this.rbacRoles = Collections.unmodifiableSortedSet(new TreeSet<>(Set.of(rbacRoles)));
        this.rbacRoles.forEach(role -> requestQueues.put(role, new ArrayDeque<>()));
        requestQueues.put(BasicRbacRole.NULL, new ArrayDeque<>()); // add default queue

        uniqueID = "PID=" + ManagementFactory.getRuntimeMXBean().getName() + "-TID=" + this.getId();
        LOGGER.atInfo().addArgument(serviceName).addArgument(uniqueID).log("created new serviceName '{}' worker - uniqueID: {}");

        reconnectToBroker();
    }

    /**
     * Send message to broker If no msg is provided, creates one internally
     *
     * @param command the MajordomoProtocol command
     * @param option option
     * @param msg message to be sent to Majordomo broker
     */
    void sendToBroker(MajordomoProtocol command, String option, ZMsg msg) {
        msg = msg != null ? msg.duplicate() : new ZMsg();

        // Stack protocol envelope to start of message
        if (option != null) {
            msg.addFirst(new ZFrame(option));
        }

        msg.addFirst(command.newFrame());
        msg.addFirst(MajordomoProtocol.W_WORKER.newFrame());
        msg.addFirst(new ZFrame(ZMQ.MESSAGE_SEPARATOR));

        if (verbose) {
            log.format("I: sending %s to broker\n", command);
            msg.dump(log.out());
        }
        msg.send(worker);
    }

    /**
     * Connect or reconnect to broker
     */
    void reconnectToBroker() {
        if (worker != null) {
            worker.close();
        }
        worker = ctx.createSocket(SocketType.DEALER);
        worker.connect(brokerAddress);
        if (verbose)
            log.format("I: connecting to broker at %s\n", brokerAddress);

        // Register service with broker
        sendToBroker(MajordomoProtocol.W_READY, service, null);

        // If liveness hits zero, queue is considered disconnected
        liveness = HEARTBEAT_LIVENESS;
        heartbeatAt = System.currentTimeMillis() + heartbeat;
    }

    public void destroy() {
        ctx.destroy();
    }

    public RequestHandler getRequestHandler() {
        return requestHandler;
    }

    // ==============   getters and setters =================
    public int getHeartbeat() {
        return heartbeat;
    }

    public void setHeartbeat(int heartbeat) {
        this.heartbeat = heartbeat;
    }

    public int getReconnect() {
        return reconnect;
    }

    public void setReconnect(int reconnect) {
        this.reconnect = reconnect;
    }

    protected void handleRequestsFromBoker() {
        // find queue based on RBAC role
        Queue<ZMsg> queue = requestQueues.get(BasicRbacRole.NULL); // default queue
        for (RbacRole<?> role : rbacRoles) {
            if (requestQueues.get(role).isEmpty()) {
                continue;
            }
            queue = requestQueues.get(role); // matched non-empty queue
        }
        if (queue.isEmpty()) {
            return;
        }
        final ZMsg msg = queue.poll();
        assert (msg != null);

        // remove and check for empty frame
        ZFrame empty = msg.pop();
        assert (empty.getData().length == 0);
        empty.destroy();

        // remove and check whether we can/should handle this request
        ZFrame header = msg.pop();
        assert (MajordomoProtocol.W_WORKER.frameEquals(header));
        header.destroy();

        // remove command
        ZFrame command = msg.pop();
        if (MajordomoProtocol.W_REQUEST.frameEquals(command)) {
            // We should pop and save as many addresses as there are
            // up to a null part, but for now, just save one
            final ZFrame replyTo = msg.unwrap();

            ZMsg reply = new ZMsg();
            if (requestHandler != null) {
                reply = requestHandler.handle(msg);
            }
            assert (reply != null);

            reply.wrap(replyTo);
            if (!replyQueue.offer(reply)) {
                throw new IllegalStateException("output queue is full, size = " + replyQueue.size());
            }
            command.destroy();
            return;
        } else if (MajordomoProtocol.W_HEARTBEAT.frameEquals(command)) {
            // Do nothing for heartbeats
        } else if (MajordomoProtocol.W_DISCONNECT.frameEquals(command)) {
            reconnectToBroker();
        } else {
            log.format("E: invalid input message: \n");
            msg.dump(log.out());
        }
        command.destroy();
        msg.destroy();
    }

    /**
     * Send reply, if any, to broker and wait for next request.
     */
    protected void handleReceive() {
        while (!Thread.currentThread().isInterrupted()) {
            // Poll socket for a reply, with timeout
            ZMQ.Poller items = ctx.createPoller(1);
            items.register(worker, ZMQ.Poller.POLLIN);
            if (items.poll(timeout) == -1) {
                break; // Interrupted
            }

            if (items.pollin(0)) {
                ZMsg msg = ZMsg.recvMsg(worker);
                if (msg == null) {
                    break; // Interrupted
                }
                if (verbose) {
                    log.format("I: received message from broker: \n");
                    msg.dump(log.out());
                }
                liveness = HEARTBEAT_LIVENESS;
                // Don't try to handle errors, just assert noisily
                assert (msg.size() >= 3);

                // find proper queue
                final Queue<ZMsg> queue;
                if (msg.size() >= 7) {
                    final RbacToken rbacToken = RbacToken.from(msg.peekLast().getData());
                    final Queue<ZMsg> roleBasedQueue = requestQueues.get(rbacToken.getRole());
                    queue = roleBasedQueue == null ? requestQueues.get(BasicRbacRole.NULL) : roleBasedQueue;
                } else {
                    queue = requestQueues.get(BasicRbacRole.NULL);
                }
                assert (queue != null);
                queue.offer(msg);

                handleRequestsFromBoker();

            } else if (--liveness == 0) {
                if (verbose) {
                    log.format("W: disconnected from broker - retrying\n");
                }
                try {
                    Thread.sleep(reconnect);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Restore the interrupted status
                    break;
                }
                reconnectToBroker();
            }
            // Send HEARTBEAT if it's time
            if (System.currentTimeMillis() > heartbeatAt) {
                sendToBroker(MajordomoProtocol.W_HEARTBEAT, null, null);
                heartbeatAt = System.currentTimeMillis() + heartbeat;
            }
            items.close();

            // send reply if any
            while (!replyQueue.isEmpty()) {
                final ZMsg reply = replyQueue.poll();
                sendToBroker(MajordomoProtocol.W_REPLY, null, reply);
                reply.destroy();
            }
        }
        if (Thread.currentThread().isInterrupted()) {
            log.format("W: interrupt received, killing worker\n");
        }
    }

    public void registerHandler(final RequestHandler handler) {
        this.requestHandler = handler;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            handleReceive();
        }
        destroy();
    }

    public String getUniqueID() {
        return uniqueID;
    }

    public interface RequestHandler {
        ZMsg handle(ZMsg input);
    }
}
