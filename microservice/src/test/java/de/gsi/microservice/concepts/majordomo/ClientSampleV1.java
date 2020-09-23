package de.gsi.microservice.concepts.majordomo;

import java.nio.charset.StandardCharsets;

import org.zeromq.ZMsg;

/**
* Majordomo Protocol client example. Uses the mdcli API to hide all MajordomoProtocol aspects
*/
public class ClientSampleV1 {
    public static void main(String[] args) {
        boolean verbose = (args.length > 0 && "-v".equals(args[0]));
        MajordomoClientV1 clientSession = new MajordomoClientV1("tcp://localhost:5555", "customClientName", verbose);
        final byte[] serviceBytes = "mmi.echo".getBytes(StandardCharsets.UTF_8);
        int count;
        long start = System.currentTimeMillis();
        for (count = 0; count < 10000; count++) {
            ZMsg request = new ZMsg();
            request.addString("Hello world  - V1");
            //ZMsg reply = clientSession.send("echo", request, new RbacToken(BasicRbacRole.ADMIN, "HASHCODE"));
            // ZMsg reply = clientSession.send("inproc.echo", request, new RbacToken(BasicRbacRole.ADMIN, "HASHCODE"));
            //ZMsg reply = clientSession.send("echo", request);
            ZMsg reply = clientSession.send("mmi.echo", request);
            if (reply != null) {
                reply.destroy();
            } else {
                break; // Interrupt or failure
            }
            if (count % 1000 == 0) {
                System.err.println("client iteration = " + count);
            }
        }

        long mark1 = System.currentTimeMillis();
        double diff2 = 1e-3 * (mark1 - start);
        System.err.printf("%d requests/replies processed in %d ms -> %f op/s\n",
                count, mark1 - start, count / diff2);
        clientSession.destroy();
    }
}
