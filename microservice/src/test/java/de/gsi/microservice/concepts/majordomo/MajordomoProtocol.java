package de.gsi.microservice.concepts.majordomo;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.zeromq.ZFrame;

/**
 * Majordomo Protocol definitions, Java version
 */
public enum MajordomoProtocol {
    /**
     * This is the version of MajordomoProtocol/Client we implement
     */
    C_CLIENT("MDPC01"),

    /**
     * This is the version of MajordomoProtocol/Worker we implement
     */
    W_WORKER("MDPW01"),

    // MajordomoProtocol/Server commands, as byte values
    W_READY(1),
    W_REQUEST(2),
    W_REPLY(3),
    W_HEARTBEAT(4),
    W_DISCONNECT(5);

    private final byte[] data;

    MajordomoProtocol(final String value) {
        this.data = value.getBytes(StandardCharsets.UTF_8);
    }

    MajordomoProtocol(final int value) { //watch for ints>255, will be truncated
        this.data = new byte[] { (byte) (value & 0xFF) };
    }

    public ZFrame newFrame() {
        return new ZFrame(data);
    }

    public byte[] getFrameData() {
        return data;
    }

    public boolean frameEquals(ZFrame frame) {
        return Arrays.equals(data, frame.getData());
    }

    public boolean isEquals(final byte[] other) {
        return Arrays.equals(this.data, other);
    }
}
