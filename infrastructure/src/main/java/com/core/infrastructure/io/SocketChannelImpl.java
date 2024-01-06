package com.core.infrastructure.io;

import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;

import java.io.IOException;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Set;

class SocketChannelImpl implements SocketChannel, NioSelector.SelectableChannel, Encodable {

    private final java.nio.channels.SocketChannel channel;
    private final Selector selector;
    private SelectionKey selectionKey;
    private String remoteAddress;
    private String localAddress;
    private Runnable readListener;
    private Runnable writeListener;
    private Runnable connectListener;

    SocketChannelImpl(Selector selector, java.nio.channels.SocketChannel channel) {
        this.selector = selector;
        this.channel = channel;
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void close() throws IOException {
        channel.close();
        readListener = null;
        writeListener = null;
        connectListener = null;
        localAddress = null;
        remoteAddress = null;
    }

    @Override
    public void configureBlocking(boolean block) throws IOException {
        channel.configureBlocking(block);
        if (!block) {
            selectionKey = channel.register(selector, 0, this);
        }
    }

    @Override
    public boolean isBlocking() {
        return channel.isBlocking();
    }

    @Override
    public int read(ByteBuffer buffer) throws IOException {
        return channel.read(buffer);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return channel.read(dsts, offset, length);
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return channel.read(dsts);
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
    public boolean connect(String address) throws IOException {
        var inetSocketAddress = InetAddressUtils.toHostPort(address);
        return channel.connect(inetSocketAddress);
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
    public boolean finishConnect() throws IOException {
        return channel.finishConnect();
    }

    @Override
    public int write(ByteBuffer buffer) throws IOException {
        return channel.write(buffer);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return channel.write(srcs, offset, length);
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return channel.write(srcs);
    }

    @Override
    public void setReadListener(Runnable readListener) {
        this.readListener = readListener;
        if (selectionKey.isValid()) {
            if (readListener == null) {
                selectionKey.interestOpsAnd(~SelectionKey.OP_READ);
            } else {
                selectionKey.interestOpsOr(SelectionKey.OP_READ);
            }
        }
    }

    @Override
    public void setWriteListener(Runnable writeListener) {
        this.writeListener = writeListener;
        if (selectionKey.isValid()) {
            if (writeListener == null) {
                selectionKey.interestOpsAnd(~SelectionKey.OP_WRITE);
            } else {
                selectionKey.interestOpsOr(SelectionKey.OP_WRITE);
            }
        }
    }

    @Override
    public void setConnectListener(Runnable connectListener) {
        this.connectListener = connectListener;
        if (selectionKey.isValid()) {
            if (connectListener == null) {
                selectionKey.interestOpsAnd(~SelectionKey.OP_CONNECT);
            } else {
                selectionKey.interestOpsOr(SelectionKey.OP_CONNECT);
            }
        }
    }

    @Override
    public void onRead() {
        readListener.run();
    }

    @Override
    public void onWrite() {
        writeListener.run();
    }

    @Override
    public void onAccept() {
        // does not accept
    }

    @Override
    public void onConnect() {
        connectListener.run();
    }

    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("remoteAddress").string(getRemoteAddress())
                .string("localAddress").string(getLocalAddress())
                .string("open").bool(isOpen())
                .string("connected").bool(isConnected())
                .closeMap();
    }

    @Override
    public String toString() {
        return toEncodedString();
    }
}
