package com.core.infrastructure.io;

/**
 * Constants used with the WebSocket protocol.
 */
public class WebSocketConstants {

    public static final short FLAG_FIN = 0x80;
    public static final byte OPCODE_MASK = 0xF;
    public static final byte OPCODE_CONTINUATION_FRAME = 0x0;
    public static final byte OPCODE_TEXT_FRAME = 0x1;
    public static final byte OPCODE_BINARY_FRAME = 0x2;
    public static final byte OPCODE_CONNECTION_CLOSE = 0x8;
    public static final byte OPCODE_PING = 0x9;
    public static final byte OPCODE_PONG = 0xA;

    public static final short FLAG_MASK = 0x80;
    public static final byte PAYLOAD_LENGTH_MASK = 0x7F;
    public static final byte EXTENDED_16_BYTES = 126;
    public static final byte EXTENDED_64_BYTES = 127;
    public static final int MAX_16_BYTES_LENGTH = 65535;
}
