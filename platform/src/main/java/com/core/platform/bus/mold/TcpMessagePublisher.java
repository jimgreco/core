package com.core.platform.bus.mold;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.collections.ObjectPool;
import com.core.infrastructure.collections.Resettable;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.io.ServerSocketChannel;
import com.core.infrastructure.io.SocketChannel;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.time.Scheduler;
import com.core.infrastructure.time.Time;
import com.core.platform.activation.Activatable;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

// TODO: account for write blocked
class TcpMessagePublisher implements Activatable, Encodable {

    private static final int WRITE_BUFFER_SIZE = 100 * 1024;
    private static final long HEARTBEAT_CHECK_TIME = TimeUnit.SECONDS.toNanos(1);
    private static final long HEARTBEAT_TIMEOUT = TimeUnit.SECONDS.toNanos(10);
    private static final long CATCHUP_DELAY = TimeUnit.MILLISECONDS.toNanos(1);

    private final Selector selector;
    private final Time time;
    private final Scheduler scheduler;
    private final Log log;

    private final MoldSession moldSession;
    private final Activator activator;
    private final MessageStore messageStore;

    @Property
    private final String address;

    private final ObjectPool<SocketHolder> socketHolderPool;
    private final List<SocketHolder> socketHolders;

    private final MutableDirectBuffer messageReadBuffer;
    private final MutableDirectBuffer heartbeatReadBuffer;
    private final MutableDirectBuffer heartbeatWriteBuffer;
    private final MutableDirectBuffer messageWrapper;
    private final int[] messageLengths;

    private final Runnable cachedRewind;

    private MutableDirectBuffer messageBuffer;
    private ServerSocketChannel serverSocketChannel;
    private long onHeartbeatCheckTaskId;
    private long rewindTaskId;

    TcpMessagePublisher(
            Selector selector,
            Time time,
            Scheduler scheduler,
            LogFactory logFactory,
            ActivatorFactory activatorFactory,
            MoldSession moldSession,
            MessageStore messageStore,
            String address) {
        this.selector = Objects.requireNonNull(selector, "selectService is null");
        this.time = Objects.requireNonNull(time, "time is null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(activatorFactory, "activationManager is null");
        this.moldSession = Objects.requireNonNull(moldSession, "moldSession is null");
        this.messageStore = Objects.requireNonNull(messageStore, "messageStore is null");
        this.address = Objects.requireNonNull(address, "address is null");

        log = logFactory.create(TcpMessagePublisher.class);
        socketHolders = new CoreList<>();
        socketHolderPool = new ObjectPool<>(SocketHolder::new);
        cachedRewind = this::onRewind;

        messageLengths = new int[1];
        messageWrapper = BufferUtils.mutableEmptyBuffer();
        messageReadBuffer = BufferUtils.allocate(WRITE_BUFFER_SIZE);
        heartbeatReadBuffer = BufferUtils.allocate(1024);
        heartbeatWriteBuffer = BufferUtils.allocate(Short.BYTES + MoldConstants.SESSION_LENGTH + Long.BYTES);

        activator = activatorFactory.createActivator(
                "TcpEventPublisher:" + address, this, moldSession);
    }

    private void onHeartbeatCheck() {
        var timestamp = time.nanos();
        var holders = socketHolders.iterator();
        SocketHolder holder;
        while (holders.hasNext()) {
            holder = holders.next();
            if (timestamp > holder.lastHeartbeatTime + HEARTBEAT_TIMEOUT) {
                log.warn().append("heartbeat timeout, disconnecting: ").append(holder.socket.getRemoteAddress())
                        .commit();
                holders.remove();
                socketHolderPool.returnObject(holder);
            } else {
                try {
                    holder.writeHeartbeat();
                } catch (IOException e) {
                    log.warn().append("I/O exception writing heartbeat, disconnecting: ").append(e).commit();
                    holders.remove();
                    socketHolderPool.returnObject(holder);
                }
            }
        }
    }

    @Override
    public void activate() {
        try {
            log.info().append("binding message publisher: session=").append(moldSession.getSessionName())
                    .append(", address=").append(address)
                    .commit();

            var sessionName = moldSession.getSessionName();
            messageStore.open(sessionName);

            serverSocketChannel = selector.createServerSocketChannel();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            serverSocketChannel.setAcceptListener(this::onClientAccept);
            serverSocketChannel.bind(address);

            activator.ready();

            onHeartbeatCheckTaskId = scheduler.scheduleEvery(
                    onHeartbeatCheckTaskId, HEARTBEAT_CHECK_TIME, this::onHeartbeatCheck,
                    "TcpEventPublisher:onHeartbeatCheck", 0);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void onClientAccept() {
        try {
            var socket = serverSocketChannel.accept();

            log.info().append("accepting client connection: ").append(socket.getRemoteAddress()).commit();
            var holder = socketHolderPool.borrowObject();
            holder.init(socket);
        } catch (IOException e) {
            log.warn().append("could not accept client connection: ").append(e).commit();
        }
    }

    private void onRewind() {
        rewindTaskId = 0;
        var holders = socketHolders.iterator();
        SocketHolder holder;
        var scheduleRewind = false;

        while (holders.hasNext()) {
            holder = holders.next();
            try {
                if (holder.seqNum != -1 && holder.seqNum < moldSession.getNextSequenceNumber()
                        && holder.writeBufferPosition == 0) {
                    var lengthRead = 0;
                    var messagesRead = 0;
                    for (var i = holder.seqNum; i < moldSession.getNextSequenceNumber(); i++) {
                        if (lengthRead >= messageReadBuffer.capacity() - MoldConstants.MAX_MESSAGE_SIZE - Short.BYTES) {
                            break;
                        }
                        lengthRead += messageStore.read(messageReadBuffer, lengthRead, i);
                        messagesRead++;
                    }

                    var lengthWritten = holder.socket.write(messageReadBuffer, 0, lengthRead);

                    if (lengthWritten < lengthRead) {
                        var remainingBytes = lengthRead - lengthWritten;
                        log.warn().append("write blocked on rewind: bytesWritten=").append(lengthWritten)
                                .append(", bytesRemaining=").append(remainingBytes)
                                .append(", startSeqNum=").append(holder.seqNum)
                                .append(", nextSeqNum=").append(holder.seqNum + messagesRead)
                                .commit();

                        holder.writeBuffer.putBytes(0, messageReadBuffer, lengthWritten, remainingBytes);
                        holder.writeBufferPosition = remainingBytes;
                        holder.seqNum += messagesRead;
                        holder.socket.setWriteListener(holder.cachedOnWrite);
                    } else {
                        holder.seqNum += messagesRead;

                        if (holder.seqNum < moldSession.getNextSequenceNumber()) {
                            scheduleRewind = true;
                        } else {
                            log.info().append("rewind complete: address=").append(holder.socket.getRemoteAddress())
                                    .append(", nextSeqNum=").append(moldSession.getNextSequenceNumber())
                                    .commit();
                        }
                    }
                }
            } catch (IOException e) {
                log.warn().append("I/O exception writing to the client, disconnecting: address=")
                        .append(holder.socket.getRemoteAddress())
                        .append(", exception=").append(e).commit();
                holders.remove();
                socketHolderPool.returnObject(holder);
            }
        }

        if (scheduleRewind && rewindTaskId == 0) {
            rewindTaskId = scheduler.scheduleIn(
                    rewindTaskId, CATCHUP_DELAY, cachedRewind, "onCatchup", 0);
        }
    }

    @Override
    public void deactivate() {
        onHeartbeatCheckTaskId = scheduler.cancel(onHeartbeatCheckTaskId);
        rewindTaskId = scheduler.cancel(rewindTaskId);

        activator.notReady("closed");

        log.info().append("closing message publisher: session=").append(moldSession.getSessionName())
                .append(", address=").append(address)
                .commit();

        var holders = socketHolders.iterator();
        while (holders.hasNext()) {
            var holder = holders.next();
            holders.remove();
            socketHolderPool.returnObject(holder);
        }

        try {
            serverSocketChannel.close();
            serverSocketChannel = null;
        } catch (IOException e) {
            log.warn().append("could not close server socket: ").append(e).commit();
            throw new IllegalStateException(e);
        }
    }

    /**
     * Returns a buffer for writing messages to the MoldUDP64 packet.
     *
     * @return a buffer for writing messages to the MoldUdp64 packet
     */
    public MutableDirectBuffer acquire() {
        messageBuffer = messageStore.acquire();
        messageWrapper.wrap(messageBuffer, Short.BYTES, messageBuffer.capacity() - Short.BYTES);
        return messageWrapper;
    }

    /**
     * Commits the message to the MoldUDP64 packet.
     *
     * @param length the number of bytes in the message
     */
    public void commit(int length) {
        var messageLength = Short.BYTES + length;

        try {
            if (messageLength > messageBuffer.capacity()) {
                throw new IllegalStateException("invalid length, greater than 1450 bytes: " + length);
            }
            messageBuffer.putShort(0, (short) length);

            messageLengths[0] = messageLength;
            messageStore.commit(messageLengths, 0, 1);

            moldSession.setNextSequenceNumber(moldSession.getNextSequenceNumber() + 1);
        } catch (IOException e) {
            log.warn().append("error recording message, closing server: session=").append(moldSession.getSessionName())
                    .append(", nextSeqNum=").append(moldSession.getNextSequenceNumber())
                    .commit();
            activator.stop();
            return;
        }

        // send to all listeners
        var holders = socketHolders.iterator();
        SocketHolder holder;

        while (holders.hasNext()) {
            holder = holders.next();
            try {
                if (holder.seqNum != -1 && holder.seqNum + 1 == moldSession.getNextSequenceNumber()
                        && holder.writeBufferPosition == 0) {
                    var lengthWritten = holder.socket.write(messageBuffer, 0, messageLength);

                    if (lengthWritten < messageLength) {
                        var remainingBytes = messageLength - lengthWritten;
                        log.warn().append("write blocked on new message: bytesWritten=").append(lengthWritten)
                                .append(", bytesRemaining=").append(remainingBytes)
                                .append(", startSeqNum=").append(holder.seqNum)
                                .commit();
                        holder.seqNum++;

                        holder.writeBuffer.putBytes(0, messageBuffer, lengthWritten, remainingBytes);
                        holder.writeBufferPosition = remainingBytes;
                        holder.socket.setWriteListener(holder.cachedOnWrite);
                    } else {
                        holder.seqNum++;
                    }
                }
            } catch (IOException e) {
                log.warn().append("I/O exception writing to the client, disconnecting: ").append(e).commit();
                holders.remove();
                socketHolderPool.returnObject(holder);
            }
        }
    }

    @Command(path = "status", readOnly = true)
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("activator").object(activator)
                .string("session").object(moldSession)
                .string("address").string(address)
                .string("messageStore").object(messageStore)
                .closeMap();
    }

    @Override
    public String toString() {
        return toEncodedString();
    }

    private class SocketHolder implements Resettable {

        final Runnable cachedOnRead;
        final Runnable cachedOnWrite;
        final MutableDirectBuffer writeBuffer;
        int writeBufferPosition;
        SocketChannel socket;
        long seqNum;
        long lastHeartbeatTime;

        SocketHolder() {
            cachedOnRead = this::onRead;
            cachedOnWrite = this::onWrite;
            writeBuffer = BufferUtils.allocate(WRITE_BUFFER_SIZE);
        }

        @Override
        public void reset() {
            try {
                socket.close();
            } catch (IOException e) {
                log.warn().append("I/O exception closing client, disconnecting: ").append(e).commit();
            }
            socket = null;
            seqNum = -1;
            lastHeartbeatTime = -1;
            writeBufferPosition = 0;
        }

        void init(SocketChannel socketChannel) {
            try {
                socket = socketChannel;
                socketHolders.add(this);

                seqNum = -1;
                lastHeartbeatTime = time.nanos();

                socket.configureBlocking(false);
                socket.setOption(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE);
                socket.setReadListener(cachedOnRead);
                writeHeartbeat();
            } catch (IOException e) {
                log.warn().append("could initiate connection: address=").append(socketChannel.getRemoteAddress())
                        .append(", exception=").append(e)
                        .commit();
                socketHolders.remove(this);
                socketHolderPool.returnObject(this);
            }
        }

        private void writeHeartbeat() throws IOException {
            if (writeBufferPosition == 0) {
                heartbeatWriteBuffer.putShort(0, (short) -2);
                heartbeatWriteBuffer.putBytes(
                        Short.BYTES, moldSession.getSessionName(), 0, MoldConstants.SESSION_LENGTH);
                heartbeatWriteBuffer.putLong(Short.BYTES + MoldConstants.SESSION_LENGTH,
                        moldSession.getNextSequenceNumber());

                var lengthWritten = socket.write(heartbeatWriteBuffer);

                if (lengthWritten < heartbeatWriteBuffer.capacity()) {
                    var remainingBytes = heartbeatWriteBuffer.capacity() - lengthWritten;
                    log.warn().append("write blocked on heartbeat: bytesWritten=").append(lengthWritten)
                            .append(", bytesRemaining=").append(remainingBytes)
                            .commit();

                    writeBuffer.putBytes(0, heartbeatWriteBuffer, lengthWritten, remainingBytes);
                    writeBufferPosition = remainingBytes;
                    socket.setWriteListener(cachedOnWrite);
                }
            }
        }

        private void onWrite() {
            try {
                if (writeBufferPosition > 0) {
                    var bytesWritten = socket.write(writeBuffer, 0, writeBufferPosition);
                    writeBufferPosition -= bytesWritten;
                }
                if (writeBufferPosition == 0) {
                    // wrote everything
                    socket.setWriteListener(null);

                    if (seqNum < moldSession.getNextSequenceNumber() && rewindTaskId == 0) {
                        rewindTaskId = scheduler.scheduleIn(
                                rewindTaskId, CATCHUP_DELAY, cachedRewind, "onCatchup", 0);
                    }
                }
            } catch (IOException e) {
                log.warn().append("I/O exception writing to socket, disconnecting: address=")
                        .append(socket.getRemoteAddress())
                        .append(", address=").append(e)
                        .commit();
                socketHolders.remove(this);
                socketHolderPool.returnObject(this);
            }
        }

        private void onRead() {
            try {
                var bytesRead = socket.read(heartbeatReadBuffer);
                lastHeartbeatTime = time.nanos();

                if (bytesRead == -1) {
                    // client closed connection
                    log.info().append("end of stream, disconnecting: ").append(socket.getRemoteAddress())
                            .commit();
                    socketHolders.remove(this);
                    socketHolderPool.returnObject(this);
                } else if (seqNum == -1) {
                    if (bytesRead < Long.BYTES) {
                        log.warn().append("client did not send sequence number, disconnecting: ")
                                .append(socket.getRemoteAddress())
                                .commit();
                        socketHolders.remove(this);
                        socketHolderPool.returnObject(this);
                    } else {
                        seqNum = heartbeatReadBuffer.getLong(0);

                        if (seqNum < moldSession.getNextSequenceNumber()) {
                            log.info().append("rewinding client: address=").append(socket.getRemoteAddress())
                                    .append(", fromSeqNum=").append(seqNum)
                                    .append(", nextSeqNum=").append(moldSession.getNextSequenceNumber())
                                    .commit();
                            rewindTaskId = scheduler.scheduleIn(
                                    rewindTaskId, CATCHUP_DELAY, cachedRewind, "onCatchup", 0);
                        }
                    }
                }
            } catch (IOException e) {
                log.warn().append("I/O exception reading from socket, disconnecting: address=")
                        .append(socket.getRemoteAddress())
                        .append(", address=").append(e)
                        .commit();
                socketHolders.remove(this);
                socketHolderPool.returnObject(this);
            }
        }
    }
}
