package com.core.infrastructure.io;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.ByteBufferPool;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.time.Scheduler;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.net.SocketOption;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.TimeUnit;

class SslSocketChannelImpl implements SslSocketChannel, NioSelector.SelectableChannel, Encodable {

    private static final long SSL_TIMEOUT = TimeUnit.MILLISECONDS.toNanos(1);

    private final SSLContext context;
    private final Selector selector;
    private final ByteBufferPool bufferPool;
    private final Scheduler scheduler;

    private SocketChannel channel;
    private SelectionKey selectionKey;

    private String remoteAddress;
    private String localAddress;
    private Runnable readListener;
    private Runnable writeListener;
    private Runnable connectListener;
    private Runnable connectionFailedListener;
    private Runnable handshakeCompleteListener;
    private IoListener ioListener;

    private final Runnable cachedHandshake;
    private SSLEngine engine;
    private SSLEngineResult.HandshakeStatus handshakeStatus;
    private long handshakeTaskId;

    private String reason;
    private Exception exception;

    /**
     * Will contain this peer's application data in plaintext, that will be later encrypted using
     * {@link SSLEngine#wrap(ByteBuffer, ByteBuffer)} and sent to the other peer.
     * This buffer can typically be of any size, as long as it is large enough to contain this peer's outgoing
     * messages.
     * If this peer tries to send a message bigger than buffer's capacity a {@link BufferOverflowException}
     * will be thrown.
     */
    private ByteBuffer myAppData;
    /**
     * Will contain this peer's encrypted data, that will be generated after
     * {@link SSLEngine#wrap(ByteBuffer, ByteBuffer)} is applied on {@link #myAppData}.
     * It should be initialized using {@link SSLSession#getPacketBufferSize()}, which returns the size up to which,
     * SSL/TLS packets will be generated from the engine under a session.
     * All SSLEngine network buffers should be sized at least this large to avoid insufficient space problems when
     * performing wrap and unwrap calls.
     */
    private ByteBuffer myNetData;
    /**
     * Will contain the other peer's (decrypted) application data.
     * It must be large enough to hold the application data from any peer.
     * Can be initialized with {@link SSLSession#getApplicationBufferSize()} for an estimation of the other peer's
     * application data and should be enlarged if this size is not enough.
     */
    private ByteBuffer peerAppData;
    /**
     * Will contain the other peer's encrypted data.
     * The SSL/TLS protocols specify that implementations should produce packets containing at most 16 KB of
     * plaintext, so a buffer sized to this value should normally cause no capacity problems.
     * However, some implementations violate the specification and generate large records up to 32 KB.
     * If the {@link SSLEngine#unwrap(ByteBuffer, ByteBuffer)} detects large inbound packets, the buffer sizes
     * returned by SSLSession will be updated dynamically, so the this peer should check for overflow conditions and
     * enlarge the buffer using the session's (updated) buffer size.
     */
    private ByteBuffer peerNetData;

    SslSocketChannelImpl(
            Selector selector, SocketChannel channel, SSLContext context,
            Scheduler scheduler, ByteBufferPool bufferPool) {
        this.selector = selector;
        this.channel = channel;
        this.context = context;
        this.scheduler = scheduler;
        this.bufferPool = bufferPool;

        cachedHandshake = this::doSslHandshake;
    }

    @Override
    public String getConnectionFailedReason() {
        return reason;
    }

    @Override
    public Exception getConnectionFailedException() {
        return exception;
    }

    @Override
    public boolean isConnected() {
        return channel.isConnected();
    }

    @Override
    public boolean isConnectionPending() {
        return channel.isConnectionPending();
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public boolean isBlocking() {
        return channel.isBlocking();
    }

    @Override
    public String getLocalAddress() {
        try {
            if (localAddress == null) {
                localAddress = InetAddressUtils.toAddress(channel.getLocalAddress());
            }
            return localAddress;
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public String getRemoteAddress() {
        try {
            if (remoteAddress == null) {
                remoteAddress = InetAddressUtils.toAddress(channel.getRemoteAddress());
            }
            return remoteAddress;
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void setReadListener(Runnable listener) {
        this.readListener = listener;

        if (selectionKey.isValid()) {
            if (readListener == null) {
                selectionKey.interestOpsAnd(~SelectionKey.OP_READ);
            } else {
                selectionKey.interestOpsOr(SelectionKey.OP_READ);
            }
        }
    }

    @Override
    public void setWriteListener(Runnable listener) {
        writeListener = listener;

        if (selectionKey.isValid()) {
            if (writeListener == null) {
                selectionKey.interestOpsAnd(~SelectionKey.OP_WRITE);
            } else {
                selectionKey.interestOpsOr(SelectionKey.OP_WRITE);
            }
        }
    }

    @Override
    public void setConnectListener(Runnable listener) {
        this.connectListener = listener;
    }

    @Override
    public void setHandshakeCompleteListener(Runnable listener) {
        handshakeCompleteListener = listener;
    }

    @Override
    public void setConnectionFailedListener(Runnable listener) {
        connectionFailedListener = listener;
    }

    @Override
    public void setIoListener(IoListener listener) {
        ioListener = listener;
    }

    @Override
    public <T> void setOption(SocketOption<T> name, T value) throws IOException {
        channel.setOption(name, value);
    }

    @Override
    public <T> T getOption(SocketOption<T> name) throws IOException {
        return channel.getOption(name);
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return channel.supportedOptions();
    }

    @Override
    public void configureBlocking(boolean block) throws IOException {
        channel.configureBlocking(block);
        if (!block) {
            selectionKey = channel.register(selector, SelectionKey.OP_CONNECT, this);
        }
    }

    @Override
    public void bind(String address) throws IOException {
        if (address == null) {
            channel.bind(null);
        } else {
            var hostPort = InetAddressUtils.toHostPort(address);
            channel.bind(hostPort);
        }
    }

    @Override
    public boolean finishConnect() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        if (ioListener != null) {
            ioListener.onConnectionEvent("closed");
        }

        if (myAppData != null) {
            bufferPool.returnBuffer(myAppData);
            bufferPool.returnBuffer(myNetData);
            bufferPool.returnBuffer(peerAppData);
            bufferPool.returnBuffer(peerNetData);
            myAppData = null;
            myNetData = null;
            peerAppData = null;
            peerNetData = null;
        }

        readListener = null;
        writeListener = null;
        handshakeCompleteListener = null;
        connectionFailedListener = null;

        handshakeTaskId = scheduler.cancel(handshakeTaskId);
        handshakeStatus = null;

        final var en = engine;
        engine = null;
        final var chan = channel;
        // free up these resources for the GC
        channel = null;

        if (en != null) {
            en.closeOutbound();
            try {
                en.closeInbound();
            } catch (IOException ignored) {
                // quiet fail
            }
        }

        if (chan != null) {
            chan.close();
        }
    }

    @Override
    public boolean connect(String address) throws IOException {
        if (ioListener != null) {
            var buf = ioListener.borrowBuffer();
            var length = buf.putStringWithoutLengthAscii(0, "Socket connect: ");
            length += buf.putStringWithoutLengthAscii(length, address);
            ioListener.onConnectionEvent(buf, 0, length);
        }

        if (connectListener != null) {
            connectListener.run();
        }

        var hostPort = InetAddressUtils.toHostPort(address);
        engine = context.createSSLEngine(hostPort.getHostName(), hostPort.getPort());
        engine.setUseClientMode(true);

        var session = engine.getSession();
        myAppData = bufferPool.borrowBuffer(session.getApplicationBufferSize());
        myNetData = bufferPool.borrowBuffer(session.getPacketBufferSize());
        peerAppData = bufferPool.borrowBuffer(session.getApplicationBufferSize());
        peerNetData = bufferPool.borrowBuffer(session.getPacketBufferSize());

        var inetSocketAddress = InetAddressUtils.toHostPort(address);
        return channel.connect(inetSocketAddress);
    }

    private void onConnectionFailed() {
        if (ioListener != null) {
            ioListener.onConnectionFailed(reason, exception);
        }

        if (connectionFailedListener != null) {
            connectionFailedListener.run();
        }
    }

    @Override
    public int write(ByteBuffer writeBuffer) throws IOException {
        myNetData.clear();
        var bytesToSend = writeBuffer.remaining();

        while (writeBuffer.hasRemaining()) {
            var result = engine.wrap(writeBuffer, myNetData);
            var status = result.getStatus();
            if (status == SSLEngineResult.Status.OK) {
                myNetData.flip();
                channel.write(myNetData);
                myNetData.compact();
            } else {
                throw new IOException("error in SSL wrapping: " + status);
            }
        }

        return bytesToSend;
    }

    @Override
    public int write(DirectBuffer buffer, int index, int length) throws IOException {
        if (ioListener != null) {
            ioListener.onWriteEvent(buffer, index, length);
        }

        return write(BufferUtils.byteBuffer(buffer, index, length));
    }

    @Override
    public int write(DirectBuffer buffer) throws IOException {
        if (ioListener != null) {
            ioListener.onWriteEvent(buffer, 0, buffer.capacity());
        }

        return write(BufferUtils.byteBuffer(buffer, 0, buffer.capacity()));
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        var bytes = 0L;
        for (var i = offset; i < offset + length; i++) {
            bytes += write(srcs[i]);
        }
        return bytes;
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, srcs.length);
    }

    @Override
    public int read(ByteBuffer readBuffer) throws IOException {
        var bytesRead = channel.read(peerNetData);

        if (bytesRead <= 0) {
            return bytesRead;
        } else {
            var position = readBuffer.position();
            var lastPosition = position;
            peerNetData.flip();
            while (peerNetData.hasRemaining()) {
                var result = engine.unwrap(peerNetData, readBuffer);
                var status = result.getStatus();

                if (status == SSLEngineResult.Status.CLOSED) {
                    close();
                    return -1;
                } else if (status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                    handleBufferUnderflow();
                    break;
                } else if (status == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                    throw new IOException("buffer overflow");
                } else if (status == SSLEngineResult.Status.OK) {
                    if (lastPosition == readBuffer.position()) {
                        break;
                    }
                    lastPosition = readBuffer.position();
                }
            }

            peerNetData.compact();
            return readBuffer.position() - position;
        }
    }

    @Override
    public int read(MutableDirectBuffer buffer, int index, int length) throws IOException {
        var bytesRead = read(BufferUtils.byteBuffer(buffer, index, length));

        if (ioListener != null) {
            ioListener.onReadEvent(buffer, index, bytesRead);
        }

        return bytesRead;
    }

    @Override
    public int read(MutableDirectBuffer buffer) throws IOException {
        var bytesRead = read(BufferUtils.byteBuffer(buffer, 0, buffer.capacity()));

        if (ioListener != null) {
            ioListener.onReadEvent(buffer, 0, bytesRead);
        }

        return bytesRead;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        var bytes = 0L;
        for (var i = offset; i < offset + length; i++) {
            bytes += read(dsts[i]);
        }
        return bytes;
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return read(dsts, 0, dsts.length);
    }

    @Override
    public boolean isHandshakeComplete() {
        return handshakeStatus == SSLEngineResult.HandshakeStatus.FINISHED;
    }

    @Override
    public boolean isHandshaking() {
        return handshakeStatus != null && handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED;
    }

    @Override
    public void onRead() {
        if (readListener != null && isConnected()) {
            readListener.run();
        }
    }

    @Override
    public void onWrite() {
        if (writeListener != null) {
            writeListener.run();
        }
    }

    @Override
    public void onAccept() {
        // does not accept
    }

    @Override
    public void onConnect() {
        try {
            if (channel.finishConnect()) {
                engine.beginHandshake();
                handshakeStatus = engine.getHandshakeStatus();
                doSslHandshake();
            } else {
                reason = "finishConnect returned false";
                onConnectionFailed();
            }
        } catch (IOException e) {
            reason = "I/O on finish connection or begin handshake";
            exception = e;
            onConnectionFailed();
        }
    }

    private void doSslHandshake() {
        try {
            handshakeTaskId = 0;

            if (isHandshaking()) {
                handshakeStatus = engine.getHandshakeStatus();

                if (handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
                        && handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED) {
                    switch (handshakeStatus) {
                        case NEED_UNWRAP:
                            onNeedUnwrap();
                            break;
                        case NEED_WRAP:
                            onNeedWrap();
                            break;
                        case NEED_TASK:
                            onNeedTask();
                            break;
                        default:
                            throw new IllegalStateException("invalid SSL status: " + handshakeStatus);
                    }
                }
            }

            if (isHandshakeComplete()) {
                if (handshakeCompleteListener != null) {
                    handshakeCompleteListener.run();
                }
            } else if (isHandshaking()) {
                handshakeTaskId = scheduler.scheduleIn(
                        handshakeTaskId, SSL_TIMEOUT, cachedHandshake, "SslSocketChannelImpl:doSslHandshake", 0);
            }
        } catch (Exception e) {
            reason = "error in SSL handshake";
            exception = e;
            onConnectionFailed();
        }
    }

    @SuppressWarnings("PMD.EmptyCatchBlock")
    private void onNeedUnwrap() throws IOException {
        SSLEngineResult result;
        if (channel.read(peerNetData) < 0) {
            if (engine.isInboundDone() && engine.isOutboundDone()) {
                return;
            }
            try {
                engine.closeInbound();
            } catch (SSLException e) {
                // do nothing
            }
            engine.closeOutbound();
            // After closeOutbound the engine will be set to WRAP state, in order to try to send a close
            // message to the client.
            handshakeStatus = engine.getHandshakeStatus();
            return;
        }

        try {
            peerNetData.flip();
            result = engine.unwrap(peerNetData, peerAppData);
            peerNetData.compact();
            handshakeStatus = result.getHandshakeStatus();
        } catch (SSLException sslException) {
            engine.closeOutbound();
            handshakeStatus = engine.getHandshakeStatus();
            return;
        }

        switch (result.getStatus()) {
            case OK:
                break;
            case BUFFER_OVERFLOW:
                // Will occur when peerAppData's capacity is smaller than the data derived from
                // peerNetData's unwrap.
                peerAppData = enlargeBuffer(
                        peerAppData, engine.getSession().getApplicationBufferSize());
                break;
            case BUFFER_UNDERFLOW:
                // Will occur either when no data was read from the peer or when the peerNetData buffer
                // was too small to hold all peer's data.
                peerNetData.flip();
                handleBufferUnderflow();
                peerNetData.compact();
                break;
            case CLOSED:
                if (engine.isOutboundDone()) {
                    return;
                } else {
                    engine.closeOutbound();
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                }
            default:
                throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
        }
    }

    private void onNeedWrap() throws IOException {
        SSLEngineResult result;
        try {
            myNetData.clear();
            result = engine.wrap(myAppData, myNetData);
            handshakeStatus = result.getHandshakeStatus();
        } catch (SSLException sslException) {
            engine.closeOutbound();
            handshakeStatus = engine.getHandshakeStatus();
            return;
        }

        switch (result.getStatus()) {
            case OK:
                myNetData.flip();
                while (myNetData.hasRemaining()) {
                    channel.write(myNetData);
                }
                break;
            case BUFFER_OVERFLOW:
                // Will occur if there is not enough space in myNetData buffer to write all the data
                // that would be generated by the method wrap.
                // Since myNetData is set to session's packet size we should not get to this point
                // because SSLEngine is supposed to produce messages smaller or equal to that, but a
                // general handling would be the following:
                myNetData = enlargeBuffer(myNetData, engine.getSession().getPacketBufferSize());
                break;
            case CLOSED:
                try {
                    myNetData.flip();
                    while (myNetData.hasRemaining()) {
                        channel.write(myNetData);
                    }
                    // At this point the handshake status will probably be NEED_UNWRAP so we make sure that peerNetData
                    // is clear to read.
                    peerNetData.clear();
                } catch (Exception e) {
                    handshakeStatus = engine.getHandshakeStatus();
                }
                break;
            default:
                throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
        }
    }

    private void onNeedTask() {
        Runnable task;
        while ((task = engine.getDelegatedTask()) != null) {
            task.run();
        }
        handshakeStatus = engine.getHandshakeStatus();
    }

    private void handleBufferUnderflow() {
        var netSize = engine.getSession().getPacketBufferSize();
        if (netSize > peerNetData.capacity()) {
            var newBuffer = bufferPool.borrowBuffer(netSize);
            newBuffer.put(peerNetData);
            bufferPool.returnBuffer(peerNetData);
            peerNetData = newBuffer;
        }
    }

    private ByteBuffer enlargeBuffer(ByteBuffer buffer, int newCapacity) {
        bufferPool.returnBuffer(buffer);
        return bufferPool.borrowBuffer(Math.max(newCapacity, 2 * buffer.capacity()));
    }

    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("remoteAddress").string(getRemoteAddress())
                .string("localAddress").string(getLocalAddress())
                .string("open").bool(isOpen())
                .string("connected").bool(isConnected())
                .string("handshakeComplete").bool(isHandshakeComplete())
                .closeMap();
    }
}
