package de.gsi.microservice.concepts.majordomo;

import de.gsi.microservice.rbac.BasicRbacRole;

/**
* Majordomo Protocol worker example. Uses the mdwrk API to hide all MajordomoProtocol aspects
*
*/
public class SimpleEchoServiceWorker {
    public static void main(String[] args) {
        boolean verbose = (args.length > 0 && "-v".equals(args[0]));
        MajordomoWorker workerSession = new MajordomoWorker("tcp://localhost:5556", "echo", verbose, BasicRbacRole.ADMIN);

        workerSession.registerHandler(input -> input); //  output = input : echo service is complex :-)

        workerSession.start();
    }
}
