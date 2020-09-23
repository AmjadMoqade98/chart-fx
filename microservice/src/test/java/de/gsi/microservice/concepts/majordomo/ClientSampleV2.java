package de.gsi.microservice.concepts.majordomo;

import org.zeromq.ZMsg;

/**
 * Majordomo Protocol client example, asynchronous. Uses the mdcli API to hide
 * all MajordomoProtocol aspects
 */

public class ClientSampleV2 {
    public static void main(String[] args) {
        boolean verbose = (args.length > 0 && "-v".equals(args[0]));
        MajordomoClientV2 clientSession = new MajordomoClientV2("tcp://localhost:5555", verbose);

        int count;
        long start = System.currentTimeMillis();
        for (count = 0; count < 100000; count++) {
            ZMsg request = new ZMsg();
            request.addString("Hello world - V2");
            clientSession.send("echo", request);
        }
        long mark1 = System.currentTimeMillis();
        double diff1 = 1e-3 * (mark1 - start);
        System.err.printf("%d requests processed in %d ms -> %f op/s\n", count,
                mark1 - start, count / diff1);

        for (count = 0; count < 100000; count++) {
            //            if (count < 10 || count % 1000 == 0 || count >= (100000 - 10)) {
            //                System.err.println("receive message " + count);
            //            }
            ZMsg reply = clientSession.recv();
            if (reply != null) {
                reply.destroy();
            } else {
                break; // Interrupt or failure
            }
        }
        long mark2 = System.currentTimeMillis();
        double diff2 = 1e-3 * (mark2 - start);
        System.err.printf("%d requests/replies processed in %d ms -> %f op/s\n",
                count, mark2 - start, count / diff2);
        clientSession.destroy();
    }
}
