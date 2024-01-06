package com.core.platform.io;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.SocketChannel;
import com.core.infrastructure.io.WebSocketConstants;
import com.core.infrastructure.time.Scheduler;
import org.agrona.DirectBuffer;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * The WebSocket server client is used to read/write from/to a WebSocket client.
 *
 * <p>The server client is initialized using the {@link #init(SocketChannel) init} method with an existing
 * {@link SocketChannel} object.
 *
 * <p>The WebSocket protocol is summarized in {@link JsonWebSocketClient} and the full specification is available here:
 * https://datatracker.ietf.org/doc/html/rfc6455
 */
public class JsonWebSocketServerClient extends AbstractJsonWebSocket implements Encodable {

    private static final DirectBuffer SEC_WEBSOCKET_KEY = BufferUtils.fromAsciiString("Sec-WebSocket-Key");
    private static final String MAGIC_STRING = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    private static final MessageDigest SHA1;
    protected static final DirectBuffer PING;
    protected static final DirectBuffer PONG;

    static {
        try {
            SHA1 = MessageDigest.getInstance("SHA-1");

            var ping = BufferUtils.allocate(2);
            ping.putByte(0, (byte) ((WebSocketConstants.FLAG_FIN + WebSocketConstants.OPCODE_PING) & 0xFF));
            ping.putByte(1, (byte) 0);
            PING = ping;

            var pong = BufferUtils.allocate(2);
            pong.putByte(0, (byte) ((WebSocketConstants.FLAG_FIN + WebSocketConstants.OPCODE_PONG) & 0xFF));
            pong.putByte(1, (byte) 0);
            PONG = pong;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private final DirectBuffer temp;
    private SocketChannel channel;
    private boolean connectionUpgradeComplete;

    /**
     * Creates an empty {@code JsonWebSocketServerClient} with a default read buffer size of 1kb and a write buffer size
     * of 32kb.
     *
     * @param scheduler the scheduler
     */
    public JsonWebSocketServerClient(Scheduler scheduler) {
        super(scheduler, 1024, 32 * 1024);

        temp = BufferUtils.emptyBuffer();
    }

    @Override
    public boolean isConnected() {
        return channel != null && channel.isConnected();
    }

    @Override
    public Exception getConnectionFailedException() {
        return connectionFailedException;
    }

    @Override
    public String getConnectionFailedReason() {
        return connectionFailedReason;
    }

    @Override
    protected SocketChannel getChannel() {
        return channel;
    }

    @Override
    protected DirectBuffer getPingPacket() {
        return PING;
    }

    @Override
    protected DirectBuffer getPongPacket() {
        return PONG;
    }

    /**
     * Initializes the client with the specified socket channel.
     *
     * @param channel the socket channel
     * @throws IOException if an I/O error occurs
     */
    public void init(SocketChannel channel) throws IOException {
        this.channel = channel;
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE);
        channel.configureBlocking(false);
        channel.setReadListener(cachedRead);
    }

    @Override
    public void close() {
        try {
            if (channel == null) {
                return;
            }

            if (ioListener != null) {
                ioListener.onConnectionEvent("WebSocket closed");
            }

            enablePing(false);
            enableUnsolicitedPong(false);

            readListener = null;
            connectionFailedListener = null;
            ioListener = null;

            readBufferLength = 0;
            connectionUpgradeComplete = false;

            connectionFailedReason = null;
            connectionFailedException = null;

            var ch = channel;
            channel = null;
            ch.close();
        } catch (IOException ignored) {
            // ignore
        }
    }

    @Override
    public ObjectEncoder json() {
        return jsonEncoder.start(writeBuffer, 10);
    }

    @Override
    protected void onJsonWrite() {
        try {
            if (isConnected()) {
                // write length
                var payloadLength = jsonEncoder.getEncodedLength();

                int headerStart;
                int headerLength;
                if (payloadLength <= 125) {
                    headerStart = 8;
                    headerLength = 2;
                    writeBuffer.putByte(8,
                            (byte) ((WebSocketConstants.FLAG_FIN + WebSocketConstants.OPCODE_TEXT_FRAME) & 0xFF));
                    writeBuffer.putByte(9, (byte) payloadLength);
                } else if (payloadLength <= 65535) {
                    headerStart = 6;
                    headerLength = 4;
                    writeBuffer.putByte(6,
                            (byte) ((WebSocketConstants.FLAG_FIN + WebSocketConstants.OPCODE_TEXT_FRAME) & 0xFF));
                    writeBuffer.putByte(7, WebSocketConstants.EXTENDED_16_BYTES);
                    writeBuffer.putChar(8, (char) payloadLength, ByteOrder.BIG_ENDIAN);
                } else {
                    headerStart = 0;
                    headerLength = 10;
                    writeBuffer.putByte(0,
                            (byte) ((WebSocketConstants.FLAG_FIN + WebSocketConstants.OPCODE_TEXT_FRAME) & 0xFF));
                    writeBuffer.putByte(1, WebSocketConstants.EXTENDED_64_BYTES);
                    writeBuffer.putLong(2, payloadLength, ByteOrder.BIG_ENDIAN);
                }

                if (ioListener != null) {
                    ioListener.onWriteEvent(writeBuffer,  10, payloadLength);
                }

                var totalLength = headerLength + payloadLength;
                var bytesWritten = channel.write(writeBuffer, headerStart, totalLength);
                if (bytesWritten != totalLength) {
                    connectionFailedReason = "could not write full packet to socket";
                    onConnectionFailed();
                }
            }
        } catch (IOException e) {
            connectionFailedReason = "error writing to socket";
            connectionFailedException = e;
            onConnectionFailed();
        }
    }

    @Override
    protected void onRead() {
        try {
            var length = channel.read(readBuffer, readBufferLength, readBuffer.capacity() - readBufferLength);
            if (length == -1) {
                // end of file
                onConnectionFailed();
                return;
            }
            readBufferLength += length;

            if (!connectionUpgradeComplete) {
                handleConnectionUpgrade();
                return;
            }

            var offset = 0;
            while (isConnected() && readBufferLength - offset >= 2) {
                var headerLength = 0;

                var firstByte = readBuffer.getByte(offset + headerLength++);
                var finFlag = firstByte & WebSocketConstants.FLAG_FIN;
                if (finFlag == 0) {
                    connectionFailedReason = "cannot read frames that are not FIN";
                    onConnectionFailed();
                    return;
                }
                final var opCode = firstByte & WebSocketConstants.OPCODE_MASK;

                var secondByte = readBuffer.getByte(offset + headerLength++);
                final var maskFlag = (secondByte & WebSocketConstants.FLAG_MASK) != 0;

                var payloadLength = secondByte & WebSocketConstants.PAYLOAD_LENGTH_MASK;
                if (payloadLength == WebSocketConstants.EXTENDED_16_BYTES) {
                    if (readBufferLength < offset + headerLength + Short.BYTES) {
                        break;
                    }
                    payloadLength = ((int) readBuffer.getShort(offset + headerLength, ByteOrder.BIG_ENDIAN)) & 0xFFFF;
                    headerLength += Short.BYTES;
                } else if (payloadLength == WebSocketConstants.EXTENDED_64_BYTES) {
                    if (readBufferLength < offset + headerLength + Long.BYTES) {
                        break;
                    }
                    payloadLength = (int) readBuffer.getLong(offset + headerLength, ByteOrder.BIG_ENDIAN);
                    headerLength += Long.BYTES;
                }
                if (payloadLength < 0) {
                    connectionFailedReason = "negative payload length";
                    onConnectionFailed();
                    return;
                }

                var maskLength = maskFlag ? Integer.BYTES : 0;
                if (readBufferLength < offset + headerLength + maskLength + payloadLength) {
                    break;
                }

                if (maskFlag) {
                    readBuffer.getBytes(offset + headerLength, mask);
                    headerLength += 4;
                    for (var i = 0; i < payloadLength; i++) {
                        var index = offset + headerLength + i;
                        var readByte = (byte) (readBuffer.getByte(index) ^ mask[i & 0x3]);
                        readBuffer.putByte(index, readByte);
                    }
                }

                if (opCode == WebSocketConstants.OPCODE_TEXT_FRAME) {
                    if (ioListener != null) {
                        ioListener.onReadEvent(readBuffer, offset + headerLength, payloadLength);
                    }

                    var result = json.parse(readBuffer, offset + headerLength, payloadLength);
                    if (result.isError()) {
                        connectionFailedReason = result.getErrorReason();
                        onConnectionFailed();
                        return;
                    }

                    if (readListener != null) {
                        readListener.accept(result.getRoot());
                    }
                } else if (opCode == WebSocketConstants.OPCODE_PING) {
                    if (ioListener != null) {
                        ioListener.onReadEvent("WebSocket ping");
                        ioListener.onWriteEvent("WebSocket pong");
                    }
                    getChannel().write(PONG);
                } else if (opCode == WebSocketConstants.OPCODE_CONNECTION_CLOSE) {
                    onConnectionFailed();
                    return;
                } else if (opCode != WebSocketConstants.OPCODE_PONG) {
                    connectionFailedReason = "unhandled opcode: " + opCode;
                    onConnectionFailed();
                    return;
                }

                offset += headerLength + payloadLength;
            }

            if (isConnected()) {
                compact(offset);
            }
        } catch (IOException e) {
            connectionFailedReason = "I/O error on read/write";
            connectionFailedException = e;
            onConnectionFailed();
        }
    }

    private void handleConnectionUpgrade() throws IOException {
        if (ioListener != null) {
            ioListener.onReadEvent(readBuffer, 0, readBufferLength);
        }

        var startOfLine = -1;
        var startOfSecWebSocketValue = -1;
        for (var i = 0; i < readBufferLength; i++) {
            var b =  readBuffer.getByte(i);
            if (b == '\r') {
                startOfLine = i + 2;
                if (startOfSecWebSocketValue != -1) {
                    temp.wrap(readBuffer, startOfSecWebSocketValue, i - startOfSecWebSocketValue);

                    var key = Base64.getEncoder().encodeToString(
                            SHA1.digest((temp.getStringWithoutLengthAscii(0, temp.capacity()) + MAGIC_STRING
                            ).getBytes(StandardCharsets.UTF_8)));

                    var length = writeBuffer.putStringWithoutLengthAscii(
                            0,
                            "HTTP/1.1 101 Switching Protocols\r\n"
                                    + "Connection: Upgrade\r\n"
                                    + "Upgrade: websocket\r\n"
                                    + "Sec-WebSocket-Accept: ");
                    length += writeBuffer.putStringWithoutLengthAscii(length, key);
                    length += writeBuffer.putStringWithoutLengthAscii(length, "\r\n\r\n");

                    if (ioListener != null) {
                        ioListener.onWriteEvent(writeBuffer, 0, length);
                    }

                    var bytesWritten = channel.write(writeBuffer, 0, length);
                    if (bytesWritten != length) {
                        connectionFailedReason = "could not write full packet to socket";
                        onConnectionFailed();
                        return;
                    }
                    enablePing(enablePing);

                    // kill the remaining buffer
                    readBufferLength = 0;
                    connectionUpgradeComplete = true;
                    return;
                }
            } else if (b == ':') {
                if (startOfLine != -1) {
                    temp.wrap(readBuffer, startOfLine, i - startOfLine);
                    if (SEC_WEBSOCKET_KEY.equals(temp)) {
                        startOfSecWebSocketValue = i + 1;
                    }
                }
            } else if (startOfSecWebSocketValue != -1 && Character.isWhitespace(b)) {
                startOfSecWebSocketValue = i + 1;
            }
        }

        if (ioListener != null) {
            connectionFailedReason = "WebSocket handshake failed";
            onConnectionFailed();
        }
    }

    private void compact(int offset) {
        BufferUtils.compact(readBuffer, offset, readBufferLength - offset);
        readBufferLength -= offset;
    }

    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("connected").bool(channel != null && channel.isConnected());

        if (connectionFailedReason != null) {
            encoder.string("connectionFailedReason").string(connectionFailedReason);
        }

        encoder.closeMap();
    }

    @Override
    public String toString() {
        return toEncodedString();
    }
}
