package com.core.platform.bus.mold;

/**
 * Constants used by MOLD components.
 */
public class MoldConstants {

    /**
     * The maximum size of a UDP packet that applications can write to.
     */
    public static final int MTU_SIZE = 1472;
    /**
     * The maximum size of message in a MoldUDP64 packet.
     */
    public static final int MAX_MESSAGE_SIZE = 1450;
    /**
     * The size of the MoldUdp64 packet header.
     */
    public static final int HEADER_SIZE = 20;
    /**
     * The offset to the session field in the MoldUdp64 packet header.
     */
    public static final int SESSION_OFFSET = 0;
    /**
     * The length of the session field in the MoldUdp64 packet header.
     */
    public static final int SESSION_LENGTH = 10;
    /**
     * The offset to the sequence number field in the MoldUdp64 packet header.
     */
    public static final int SEQ_NUM_OFFSET = 10;
    /**
     * The offset to the num messages field in the MoldUdp64 packet header.
     */
    public static final int NUM_MESSAGES_OFFSET = 18;
}
