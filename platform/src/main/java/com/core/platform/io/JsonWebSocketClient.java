package com.core.platform.io;

import com.core.infrastructure.Json;
import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.compression.Decompressor;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.InetAddressUtils;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.io.SocketChannel;
import com.core.infrastructure.io.SslSocketChannel;
import com.core.infrastructure.io.WebSocketConstants;
import com.core.infrastructure.time.Scheduler;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * The WebSocket client is used to connect and read/write from/to a WebSocket server.
 *
 * <p>The WebSocket protocol is summarized below and the full specification is available here:
 * https://datatracker.ietf.org/doc/html/rfc6455
 *
 * <p>To connect to the server, invoke {@link #connect(String, String)} connect}.
 * The socket connection, SSL handshake, and WebSocket connection upgrade will be completed by this object.
 *
 * <p>The initial connection is an HTTP upgrade request.
 * <pre>
 *       GET &lt;HTTP method&gt; HTTP/1.1
 *       Host: &lt;server hostname&gt;
 *       Origin: http://www.test.com
 *       Upgrade: websocket
 *       Connection: Upgrade
 *       Pragma: no-cache
 *       Cache-Control: no-cache
 *       Sec-WebSocket-Version: 13
 *       Sec-WebSocket-Key: &lt;random UUID&gt;
 * </pre>
 * The {@code Sec-WebSocket-Key} is a random UUID.
 *
 * <p>On connection, the listener specified in {@link #setConnectedListener(Runnable) setConnectedListener} is invoked.
 *
 * <p>Any errors in reading, writing, or connecting will invoke the listener specified in
 * {@link #setConnectionFailedListener(Runnable) setConnectionFailedListener}.
 *
 * <p>Once connected, WebSocket frames can be sent and received.
 * WebSocket frames have the following format.
 * <pre>
 *       0                   1                   2                   3
 *       0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *      +-+-+-+-+-------+-+-------------+-------------------------------+
 *      |F|R|R|R| opcode|M| Payload len |    Extended payload length    |
 *      |I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
 *      |N|V|V|V|       |S|             |   (if payload len==126/127)   |
 *      | |1|2|3|       |K|             |                               |
 *      +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
 *      |     Extended payload length continued, if payload len == 127  |
 *      + - - - - - - - - - - - - - - - +-------------------------------+
 *      |                               |Masking-key, if MASK set to 1  |
 *      +-------------------------------+-------------------------------+
 *      | Masking-key (continued)       |          Payload Data         |
 *      +-------------------------------- - - - - - - - - - - - - - - - +
 *      :                     Payload Data continued ...                :
 *      + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
 *      |                     Payload Data continued ...                |
 *      +---------------------------------------------------------------+
 * </pre>
 *
 * <p>JSON frames can be sent with {@link #json()}.
 * Text, binary, and continuation frames are received and dispatched to the listener specified with
 * {@link #setReadListener(Consumer) setReadListener}.
 */
public class JsonWebSocketClient extends AbstractJsonWebSocket implements SslProtocolClient, Encodable {

    private static final long DEFAULT_RECONNECT_TIMEOUT = TimeUnit.SECONDS.toNanos(5);
    protected static final DirectBuffer PING;
    protected static final DirectBuffer PONG;

    static {
        var ping = BufferUtils.allocate(2);
        ping.putByte(0, (byte) ((WebSocketConstants.FLAG_FIN + WebSocketConstants.OPCODE_PING) & 0xFF));
        ping.putByte(1, (byte) WebSocketConstants.FLAG_MASK);
        PING = ping;

        var pong = BufferUtils.allocate(2);
        pong.putByte(0, (byte) ((WebSocketConstants.FLAG_FIN + WebSocketConstants.OPCODE_PONG) & 0xFF));
        pong.putByte(1, (byte) WebSocketConstants.FLAG_MASK);
        PONG = pong;
    }

    private static final Random RANDOM = new Random();

    private final Selector selector;
    private final Runnable cachedHandshakeComplete;
    private final Runnable cachedConnect;
    private final Runnable cachedConnectionFailed;
    private final Runnable cachedRead;
    private final DirectBuffer tempWrapper;

    private MutableDirectBuffer payloadData;
    private final MutableDirectBuffer tempUuid;

    private String address;
    private String method;

    private Runnable connectListener;
    private boolean upgradingConnectionComplete;

    protected Decompressor decompressor;
    private MutableDirectBuffer decompressedData;

    private final Runnable cachedReconnect;
    private long reconnectTimeout;
    private long reconnectTaskId;
    private boolean reconnect;
    private SslSocketChannel channel;

    /**
     * Creates a {@code JsonWebSocketClient} with the specified {@code selector} used to create SSL sockets and
     * {@code scheduler} for scheduling reconnect tasks.
     *
     * <p>The default read buffer size is 32kb and write buffer size is 1kb.
     *
     * @param selector the selector
     * @param scheduler the scheduler
     */
    public JsonWebSocketClient(Selector selector, Scheduler scheduler) {
        super(scheduler, 32 * 1024, 1024);
        this.selector = selector;

        reconnectTimeout = DEFAULT_RECONNECT_TIMEOUT;

        cachedConnect = this::onConnect;
        cachedHandshakeComplete = this::onHandshakeComplete;
        cachedConnectionFailed = this::onConnectionFailed;
        cachedRead = this::onRead;
        cachedReconnect = this::reconnect;

        payloadData = BufferUtils.allocate(1024);
        tempUuid = BufferUtils.allocate(2 * Long.BYTES);
        tempWrapper = BufferUtils.emptyBuffer();
    }

    @Override
    public boolean isConnected() {
        return upgradingConnectionComplete;
    }

    public Exception getConnectionFailedException() {
        return channel.getConnectionFailedException() == null
                ? connectionFailedException : channel.getConnectionFailedException();
    }

    public String getConnectionFailedReason() {
        return channel.getConnectionFailedReason() == null
                ? connectionFailedReason : channel.getConnectionFailedReason();
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

    @Override
    public void setWriteBufferSize(int size) {
        super.setWriteBufferSize(size);
        payloadData = BufferUtils.allocateDirect(size);
    }

    @Override
    public void setConnectedListener(Runnable listener) {
        connectListener = listener;
    }

    @Override
    public void setConnectTimeout(long connectTimeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setReconnectTimeout(long timeout) {
        reconnectTimeout = timeout;
    }

    public void setDecompressor(Decompressor decompressor) {
        this.decompressor = decompressor;
        decompressedData = BufferUtils.allocateDirect(4 * readBuffer.capacity());
    }

    @Override
    public void disableReconnect() {
        reconnect = false;
    }

    @Override
    public void enableReconnect() {
        reconnect = true;
    }

    /**
     * Connects to the specified WebSocket {@code host} with the specified {@code method}.
     *
     * @param host the WebSocket host
     * @param method the WebSocket method
     */
    public void connect(String host, String method) {
        try {
            if (channel == null) {
                this.address = host;
                this.method = method;

                reconnectTaskId = scheduler.cancel(reconnectTaskId);

                if (ioListener != null) {
                    var buf = ioListener.borrowBuffer();
                    var length = buf.putStringWithoutLengthAscii(0, "WebSocket connect: host=");
                    length += buf.putStringWithoutLengthAscii(length, host);
                    length += buf.putStringWithoutLengthAscii(length, ", method=");
                    length += buf.putStringWithoutLengthAscii(length, method);
                    ioListener.onConnectionEvent(buf, 0, length);
                }

                channel = selector.createSslSocketChannel();
                channel.configureBlocking(false);
                channel.setReadListener(cachedRead);
                channel.setConnectListener(cachedConnect);
                channel.setHandshakeCompleteListener(cachedHandshakeComplete);
                channel.setConnectionFailedListener(cachedConnectionFailed);
                channel.connect(host);
            }
        } catch (Exception e) {
            connectionFailedReason = "error creating channel";
            connectionFailedException = e;
            onConnectionFailed();
        }
    }

    private void reconnect() {
        connect(address, method);
    }

    /**
     * Closes the WebSocket.
     *
     * <p>If a reconnect timer is specified with {@link #setReconnectTimeout(long) setReconnectTimeout} then the client
     * will attempt to reconnect to the host after the specified timeout.
     */
    @Override
    public void close() {
        try {
            if (channel == null) {
                return;
            }

            if (ioListener != null) {
                ioListener.onConnectionEvent("WebSocket closed");
            }

            reconnectTaskId = scheduler.cancel(reconnectTaskId);

            if (reconnect) {
                if (ioListener != null) {
                    var buf = ioListener.borrowBuffer();
                    var length = buf.putStringWithoutLengthAscii(0, "WebSocket reconnect in ");
                    length += buf.putIntAscii(length, (int) TimeUnit.NANOSECONDS.toSeconds(reconnectTimeout));
                    length += buf.putStringWithoutLengthAscii(length, "s");
                    ioListener.onConnectionEvent(buf, 0, length);
                }
                reconnectTaskId = scheduler.scheduleIn(
                        reconnectTaskId, reconnectTimeout, cachedReconnect, "SslSocketChannelImpl:reconnect", 0);
            }

            readBufferLength = 0;
            writeBufferLength = 0;

            upgradingConnectionComplete = false;
            connectionFailedReason = null;
            connectionFailedException = null;

            var ch = channel;
            channel = null;
            ch.close();
        } catch (IOException e) {
            connectionFailedReason = "I/O error closing socket";
            connectionFailedException = e;
            onConnectionFailed();
        }
    }

    @Override
    protected void onJsonWrite() {
        var payloadLength = jsonEncoder.getEncodedLength();
        writePayloadData(payloadLength);
    }

    private void writePayloadData(int payloadLength) {
        try {
            // write opcode
            writeBuffer.putByte(
                    writeBufferLength++,
                    (byte) ((WebSocketConstants.FLAG_FIN + WebSocketConstants.OPCODE_TEXT_FRAME) & 0xFF));

            // write payloadLength
            if (payloadLength < WebSocketConstants.EXTENDED_16_BYTES) {
                writeBuffer.putByte(
                        writeBufferLength++, (byte) ((WebSocketConstants.FLAG_MASK + payloadLength) & 0xFF));
            } else if (payloadLength <= WebSocketConstants.MAX_16_BYTES_LENGTH) {
                writeBuffer.putByte(
                        writeBufferLength++,
                        (byte) ((WebSocketConstants.FLAG_MASK + WebSocketConstants.EXTENDED_16_BYTES) & 0xFF));
                writeBuffer.putShort(writeBufferLength, (short) payloadLength, ByteOrder.BIG_ENDIAN);
                writeBufferLength += Short.BYTES;
            } else {
                writeBuffer.putByte(
                        writeBufferLength++,
                        (byte) ((WebSocketConstants.FLAG_MASK + WebSocketConstants.EXTENDED_64_BYTES) & 0xFF));
                writeBuffer.putLong(writeBufferLength, payloadLength, ByteOrder.BIG_ENDIAN);
                writeBufferLength += Long.BYTES;
            }

            // write mask
            var maskInt = RANDOM.nextInt();
            mask[0] = (byte) ((maskInt >>> 0) & 0xFF);
            mask[1] = (byte) ((maskInt >>> 8) & 0xFF);
            mask[2] = (byte) ((maskInt >>> 16) & 0xFF);
            mask[3] = (byte) ((maskInt >>> 24) & 0xFF);
            writeBuffer.putBytes(writeBufferLength, mask);
            writeBufferLength += Integer.BYTES;

            // write and mask payload
            for (var i = 0; i < payloadLength; i++) {
                writeBuffer.putByte(writeBufferLength++, (byte) (payloadData.getByte(i) ^ mask[i % mask.length]));
            }

            if (isConnected()) {
                if (ioListener != null) {
                    ioListener.onWriteEvent(payloadData, 0, payloadLength);
                }
                var bytesWritten = channel.write(writeBuffer, 0, writeBufferLength);
                writeBufferLength -= bytesWritten;
            }
        } catch (IOException e) {
            connectionFailedReason = "error writing to socket";
            connectionFailedException = e;
            onConnectionFailed();
        }
    }

    @Override
    public ObjectEncoder json() {
        return jsonEncoder.start(payloadData, 0);
    }

    private void onConnect() {
        if (ioListener != null) {
            ioListener.onConnectionEvent("WebSocket connected");
        }
        // we need to let SSL take care of the rest
    }

    protected boolean preParse(DirectBuffer buffer) {
        return false;
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
        } catch (IOException e) {
            connectionFailedReason = "I/O read error";
            connectionFailedException = e;
            onConnectionFailed();
            return;
        }

        if (!upgradingConnectionComplete) {
            verifyUpgradeStatus();
            if (!upgradingConnectionComplete) {
                return;
            }
        }

        var offset = 0;
        while (readBufferLength - offset >= 2) {
            var headerLength = 0;
            final var opCode = readBuffer.getByte(offset + headerLength++) & WebSocketConstants.OPCODE_MASK;
            var payloadLengthByte = readBuffer.getByte(offset + headerLength++);

            var payloadLength = payloadLengthByte & WebSocketConstants.PAYLOAD_LENGTH_MASK;
            if (payloadLength == WebSocketConstants.EXTENDED_16_BYTES) {
                if (readBufferLength < offset + headerLength + Short.BYTES) {
                    compact(offset);
                    return;
                }
                payloadLength = ((int) readBuffer.getShort(offset + headerLength, ByteOrder.BIG_ENDIAN)) & 0xFFFF;
                headerLength += Short.BYTES;
            } else if (payloadLength == WebSocketConstants.EXTENDED_64_BYTES) {
                if (readBufferLength < offset + headerLength + Long.BYTES) {
                    compact(offset);
                    return;
                }
                payloadLength = (int) readBuffer.getLong(offset + headerLength, ByteOrder.BIG_ENDIAN);
                headerLength += Long.BYTES;
            }

            if (payloadLength < 0) {
                connectionFailedReason = "negative payload length";
                onConnectionFailed();
                return;
            }
            if (readBufferLength < offset + headerLength + payloadLength) {
                // not enough bytes in the packet
                compact(offset);
                return;
            }

            offset += headerLength;

            if (opCode == WebSocketConstants.OPCODE_TEXT_FRAME) {
                Json.ParseResult result;

                if (decompressor == null) {
                    if (ioListener != null) {
                        ioListener.onReadEvent(readBuffer, offset, payloadLength);
                    }
                    tempWrapper.wrap(readBuffer, offset, payloadLength);
                    if (preParse(tempWrapper)) {
                        offset += payloadLength;
                        continue;
                    }
                    result = json.parse(readBuffer, offset, payloadLength);
                } else {
                    try {
                        var bytesDecompressed = decompressor.decompress(readBuffer, decompressedData);
                        if (ioListener != null) {
                            ioListener.onReadEvent(decompressedData, 0, bytesDecompressed);
                        }
                        result = json.parse(decompressedData, 0, bytesDecompressed);
                    } catch (Exception e) {
                        connectionFailedReason = "error decompressing data";
                        connectionFailedException = e;
                        onConnectionFailed();
                        return;
                    }
                }

                if (result.isError()) {
                    connectionFailedReason = result.getErrorReason();
                    onConnectionFailed();
                    return;
                }

                if (readListener != null) {
                    readListener.accept(result.getRoot());
                }
            } else if (opCode == WebSocketConstants.OPCODE_PING) {
                try {
                    if (ioListener != null) {
                        ioListener.onReadEvent("WebSocket ping");
                        ioListener.onWriteEvent("WebSocket pong");
                    }
                    channel.write(PONG);
                } catch (IOException e) {
                    connectionFailedReason = "I/O error on write";
                    connectionFailedException = e;
                    onConnectionFailed();
                    return;
                }
            } else if (opCode == WebSocketConstants.OPCODE_CONNECTION_CLOSE) {
                onConnectionFailed();
                return;
            } else if (opCode != WebSocketConstants.OPCODE_PONG) {
                connectionFailedReason = "unhandled opcode: " + opCode;
                onConnectionFailed();
                return;
            }

            offset += payloadLength;
        }

        if (isConnected()) {
            compact(offset);
        }
    }

    private void verifyUpgradeStatus() {
        byte byte1;
        byte byte2 = 0;
        byte byte3 = 0;
        byte byte4 = 0;

        for (var i = 0; i < readBufferLength; i++) {
            byte1 = byte2;
            byte2 = byte3;
            byte3 = byte4;
            byte4 = readBuffer.getByte(i);

            if (byte1 == '\r' && byte2 == '\n' && byte3 == '\r' && byte4 == '\n') {
                upgradingConnectionComplete = true;

                if (ioListener != null) {
                    ioListener.onConnectionEvent("WebSocket upgrade complete");
                }

                var offset = i + 1;
                compact(offset);

                enablePing(enablePing);
                enableUnsolicitedPong(enableUnsolicitedPong);

                if (connectListener != null) {
                    connectListener.run();

                    try {
                        if (writeBufferLength > 0) {
                            var bytesWritten = channel.write(writeBuffer, 0, writeBufferLength);
                            writeBufferLength -= bytesWritten;
                        }
                    } catch (IOException e) {
                        connectionFailedReason = "error writing to socket";
                        connectionFailedException = e;
                        onConnectionFailed();
                    }
                }
                return;
            }
        }
    }

    private void compact(int offset) {
        BufferUtils.compact(readBuffer, offset, readBufferLength - offset);
        readBufferLength -= offset;
    }

    private void onHandshakeComplete() {
        try {
            if (ioListener != null) {
                ioListener.onConnectionEvent("WebSocket SSL handshake complete");
            }
            var length = 0;
            length += writeBuffer.putStringWithoutLengthAscii(length, "GET ");
            length += writeBuffer.putStringWithoutLengthAscii(length, method);
            length += writeBuffer.putStringWithoutLengthAscii(length, " HTTP/1.1\r\n");

            var host = InetAddressUtils.toHost(address);
            length += writeBuffer.putStringWithoutLengthAscii(length, "Host: ");
            length += writeBuffer.putStringWithoutLengthAscii(length, host);
            length += writeBuffer.putStringWithoutLengthAscii(length, "\r\n");

            length += writeBuffer.putStringWithoutLengthAscii(length,
                    """
                            Origin: http://www.test.com\r
                            Upgrade: websocket\r
                            Connection: Upgrade\r
                            Pragma: no-cache\r
                            Cache-Control: no-cache\r                       
                            Sec-WebSocket-Version: 13\r
                            Sec-WebSocket-Key:\s""");
            tempUuid.putLong(0, RANDOM.nextLong());
            tempUuid.putLong(Long.BYTES, RANDOM.nextLong());
            var key = Base64.getEncoder().encode(tempUuid.byteArray());
            writeBuffer.putBytes(length, key, 0, key.length);
            length += key.length;
            length += writeBuffer.putStringWithoutLengthAscii(length, "\r\n\r\n");

            if (ioListener != null) {
                ioListener.onWriteEvent(writeBuffer, 0, length);
            }
            channel.write(writeBuffer, 0, length);
        } catch (IOException e) {
            connectionFailedReason = "I/O write error on WebSocket handshake";
            connectionFailedException = e;
            onConnectionFailed();
        }
    }

    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("host").string(address)
                .string("connected").bool(channel != null && channel.isConnected())
                .string("webSocketUpgrade").bool(isConnected());

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
