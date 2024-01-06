package com.core.infrastructure.io;

import java.io.IOException;
import java.net.SocketOption;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Set;

class ServerSocketChannelImpl implements ServerSocketChannel, NioSelector.SelectableChannel {

    private final Selector selector;
    private final java.nio.channels.ServerSocketChannel channel;
    private SelectionKey selectionKey;
    private String localAddress;
    private Runnable acceptListener;

    ServerSocketChannelImpl(Selector selector, java.nio.channels.ServerSocketChannel channel) throws IOException {
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
        localAddress = null;
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
    public void bind(String address) throws IOException {
        if (address == null) {
            channel.bind(null);
        } else {
            var hostPort = InetAddressUtils.toHostPort(address);
            channel.bind(hostPort);
        }
    }

    @Override
    public SocketChannel accept() throws IOException {
        var tcpSocket = channel.accept();
        return new SocketChannelImpl(selector, tcpSocket);
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
    public void setAcceptListener(Runnable acceptListener) {
        this.acceptListener = acceptListener;
        if (selectionKey.isValid()) {
            if (acceptListener == null) {
                selectionKey.interestOpsAnd(~SelectionKey.OP_ACCEPT);
            } else {
                selectionKey.interestOpsOr(SelectionKey.OP_ACCEPT);
            }
        }
    }

    @Override
    public void onRead() {
        // does not read
    }

    @Override
    public void onWrite() {
        // does not write
    }

    @Override
    public void onAccept() {
        acceptListener.run();
    }

    @Override
    public void onConnect() {
        // does not connect
    }
}
