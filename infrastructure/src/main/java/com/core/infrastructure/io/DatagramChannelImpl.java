package com.core.infrastructure.io;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Objects;
import java.util.Set;

class DatagramChannelImpl implements DatagramChannel, NioSelector.SelectableChannel, Encodable {

    private final java.nio.channels.DatagramChannel channel;
    private final Selector selector;
    private SelectionKey selectionKey;
    private String remoteAddress;
    private String localAddress;
    private String lastSentAddress;
    private SocketAddress lastSentSocketAddress;
    private SocketAddress lastReceiveAddress;
    private Runnable readListener;
    private Runnable writeListener;

    DatagramChannelImpl(Selector selector, java.nio.channels.DatagramChannel channel) {
        this.selector = selector;
        this.channel = channel;
    }

    @Command
    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void close() throws IOException {
        channel.close();
        readListener = null;
        writeListener = null;
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

    @Command
    @Override
    public boolean isBlocking() {
        return channel.isBlocking();
    }

    @Override
    public int receive(MutableDirectBuffer buffer, int index, int length) throws IOException {
        var byteBuffer = BufferUtils.byteBuffer(buffer, index, length);
        lastReceiveAddress = channel.receive(byteBuffer);
        return byteBuffer.position() - index;
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
    public int read(ByteBuffer dst) throws IOException {
        return channel.read(dst);
    }

    @Override
    public int send(DirectBuffer buffer, int index, int length, String address) throws IOException {
        if (!address.equals(lastSentAddress)) {
            // different address, rebuild InetSocketAddress
            lastSentSocketAddress = InetAddressUtils.toHostPort(address);
            lastSentAddress = address;
        }

        var srcBuffer = BufferUtils.byteBuffer(buffer, index, length);
        return channel.send(srcBuffer, lastSentSocketAddress);
    }

    @Override
    public int reply(DirectBuffer buffer, int index, int length) throws IOException {
        if (lastReceiveAddress == null) {
            throw new IOException("must receive a packet before replying");
        }

        var srcBuffer = BufferUtils.byteBuffer(buffer, index, length);
        return channel.send(srcBuffer, lastReceiveAddress);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return channel.write(src);
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
    public void connect(String address) throws IOException {
        var inetSocketAddress = InetAddressUtils.toHostPort(address);
        channel.connect(inetSocketAddress);
    }

    @Override
    public void disconnect() throws IOException {
        channel.disconnect();
        remoteAddress = null;
    }

    @Override
    public void bind(String address) throws IOException {
        if (address == null) {
            channel.bind(null);
        } else {
            var inetSocketAddress = InetAddressUtils.toPort(address);
            channel.bind(new InetSocketAddress(inetSocketAddress));
        }
    }

    @Override
    public void join(String address) throws IOException {
        Objects.requireNonNull(address);
        var parsedNetworkInterface = InetAddressUtils.toInterface(address);
        var networkInterface = NetworkInterface.getByName(parsedNetworkInterface);
        var parsedHost = InetAddressUtils.toHost(address);
        var host = InetAddress.getByName(parsedHost);
        channel.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);
        if (networkInterface.isLoopback()) {
            channel.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true);
        }
        channel.join(host, networkInterface);
    }

    @Command
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

    @Command
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

    @Command
    @Override
    public String getLastReceiveAddress() {
        return InetAddressUtils.toAddress(lastReceiveAddress);
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
    public boolean isConnected() {
        return channel.isConnected();
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
        // does not connect
    }

    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("remoteAddress").string(getRemoteAddress())
                .string("localAddress").string(getLocalAddress())
                .string("lastRecvAddress").string(getLastReceiveAddress())
                .string("open").bool(isOpen())
                .string("connected").bool(isConnected())
                .closeMap();
    }

    @Override
    public String toString() {
        return toEncodedString();
    }
}
