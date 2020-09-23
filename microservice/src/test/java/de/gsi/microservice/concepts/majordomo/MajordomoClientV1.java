package de.gsi.microservice.concepts.majordomo;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Formatter;
import java.util.concurrent.atomic.AtomicInteger;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import de.gsi.microservice.rbac.RbacToken;

/**
* Majordomo Protocol Client API, Java version Implements the MajordomoProtocol/Worker spec at
* http://rfc.zeromq.org/spec:7.
*
*/
public class MajordomoClientV1 {
    private static final AtomicInteger CLIENT_V1_INSTANCE = new AtomicInteger();
    private final String uniqueID;
    private final byte[] uniqueIdBytes;
    private String broker;
    private ZContext ctx;
    private ZMQ.Socket clientSocket;
    private long timeout = 2500;
    private int retries = 3;
    private boolean verbose;
    private Formatter log = new Formatter(System.out);

    public MajordomoClientV1(String broker, String clientName, boolean verbose) {
        this.broker = broker;
        this.verbose = verbose;
        ctx = new ZContext();

        uniqueID = clientName + "PID=" + ManagementFactory.getRuntimeMXBean().getName() + "-InstanceID=" + CLIENT_V1_INSTANCE.getAndIncrement();
        uniqueIdBytes = uniqueID.getBytes(ZMQ.CHARSET);

        reconnectToBroker();
    }

    /**
     * Connect or reconnect to broker
     */
    void reconnectToBroker() {
        if (clientSocket != null) {
            clientSocket.close();
        }
        clientSocket = ctx.createSocket(SocketType.REQ);
        clientSocket.setIdentity(uniqueIdBytes);
        clientSocket.connect(broker);
        if (verbose)
            log.format("I: connecting to broker at %s\n", broker);
    }

    public void destroy() {
        ctx.destroy();
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public String getUniqueID() {
        return uniqueID;
    }

    /**
     * Send request to broker and get reply by hook or crook takes ownership of
     * request message and destroys it when sent. Returns the reply message or
     * NULL if there was no reply.
     *
     * @param service service name
     * @param request to be send to broker/worker
     * @return reply message or NULL if there was no reply
     */
    public ZMsg send(final String service, final ZMsg request, final RbacToken... rbacToken) {
        return send(service.getBytes(StandardCharsets.UTF_8), request, rbacToken);
    }

    /**
     * Send request to broker and get reply by hook or crook takes ownership of
     * request message and destroys it when sent. Returns the reply message or
     * NULL if there was no reply.
     *
     * @param service service name (UTF-8 bytes)
     * @param request to be send to broker/worker
     * @return reply message or NULL if there was no reply
     */
    public ZMsg send(final byte[] service, final ZMsg request, final RbacToken... rbacToken) {
        if (rbacToken != null && rbacToken.length > 0 && rbacToken[0] != null) {
            request.addLast(rbacToken[0].toString());
        }
        request.push(service);
        request.push(MajordomoProtocol.C_CLIENT.getFrameData());
        if (verbose) {
            log.format("I: send request to '%s' service: \n", new ZFrame(service).toString());
            request.dump(log.out());
        }
        ZMsg reply = null;

        int retriesLeft = retries;
        while (retriesLeft > 0 && !Thread.currentThread().isInterrupted()) {
            if (!request.duplicate().send(clientSocket)) {
                throw new IllegalStateException("could not send request " + request);
            }

            // Poll socket for a reply, with timeout
            ZMQ.Poller items = ctx.createPoller(1);
            items.register(clientSocket, ZMQ.Poller.POLLIN);
            if (items.poll(timeout) == -1)
                break; // Interrupted

            if (items.pollin(0)) {
                ZMsg msg = ZMsg.recvMsg(clientSocket);
                if (verbose) {
                    log.format("I: received reply: \n");
                    msg.dump(log.out());
                }
                // Don't try to handle errors, just assert noisily
                assert (msg.size() >= 3);

                ZFrame header = msg.pop();
                assert (MajordomoProtocol.C_CLIENT.isEquals(header.getData()));
                header.destroy();

                ZFrame replyService = msg.pop();
                assert (Arrays.equals(service, replyService.getData()));
                replyService.destroy();

                reply = msg;
                break;
            } else {
                items.unregister(clientSocket);
                if (--retriesLeft == 0) {
                    log.format("W: permanent error, abandoning\n");
                    break;
                }
                log.format("W: no reply, reconnecting\n");
                reconnectToBroker();
            }
            items.close();
        }
        request.destroy();
        return reply;
    }
}
