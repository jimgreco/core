package com.core.platform.bus.mold;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Directory;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.io.SocketChannel;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.metrics.MetricFactory;
import com.core.infrastructure.time.Scheduler;
import com.core.infrastructure.time.Time;
import com.core.platform.activation.Activatable;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

class TcpMessageReceiver implements Activatable, Encodable {

    private static final long HEARTBEAT_TIMEOUT = TimeUnit.SECONDS.toNanos(10);
    private static final long CHECK_HEARTBEAT_TIMEOUT = TimeUnit.SECONDS.toNanos(1);
    private static final int HEARTBEAT_LENGTH = Short.BYTES + MoldConstants.SESSION_LENGTH + Long.BYTES;

    private final Selector selector;
    private final Log log;
    private final Scheduler scheduler;
    private final Time time;

    private final MoldSession moldSession;
    @Directory(path = ".")
    private final Activator activator;

    private final MutableDirectBuffer packetBuffer;
    private final DirectBuffer wrapper;
    private final DirectBuffer heartbeat;
    private final MutableDirectBuffer seqNumBuffer;
    private int readBufferOffset;

    @Property(write = true)
    private String address;
    @Property
    private long nextSeqNum;
    private long lastMessageTime;
    private long heartbeatTaskId;

    private SocketChannel socketChannel;
    private Consumer<DirectBuffer> messageListener;

    TcpMessageReceiver(
            Selector selector,
            Time time,
            Scheduler scheduler,
            LogFactory logFactory,
            MetricFactory metricFactory,
            ActivatorFactory activatorFactory,
            MoldSession moldSession,
            String name,
            String address) {
        this.selector = Objects.requireNonNull(selector, "selectService is null");
        this.time = Objects.requireNonNull(time, "time is null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(metricFactory, "metricFactory is null");
        Objects.requireNonNull(activatorFactory, "activationManager is null");
        this.moldSession = Objects.requireNonNull(moldSession, "moldSession is null");
        Objects.requireNonNull(name, "name is null");
        this.address = Objects.requireNonNull(address, "address is null");

        log = logFactory.create(getClass());
        packetBuffer = BufferUtils.allocateDirect(100_000);
        wrapper = BufferUtils.emptyBuffer();
        heartbeat = BufferUtils.fromAsciiString("h");
        seqNumBuffer = BufferUtils.allocate(Long.BYTES);

        nextSeqNum = 1;

        activator = activatorFactory.createActivator(name, this);

        metricFactory.registerGaugeMetric(
                "Mold_Session_NextSeqNum",
                moldSession::getNextSequenceNumber,
                "address", address);
    }

    @Override
    public void activate() {
        try {
            if (messageListener == null) {
                throw new IllegalStateException("messageListener not set");
            }

            connect();

            heartbeatTaskId = scheduler.scheduleEvery(
                    heartbeatTaskId, CHECK_HEARTBEAT_TIMEOUT, this::heartbeat,
                    "TcpEventReceiver:heartbeat", 0);

            activator.notReady("opened");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void deactivate() {
        heartbeatTaskId = scheduler.cancel(heartbeatTaskId);

        activator.notReady("closed");

        disconnect();
    }

    private void connect() throws IOException {
        log.info().append("connecting to the message server: ").append(address).commit();

        readBufferOffset = 0;
        lastMessageTime = time.nanos();

        socketChannel = selector.createSocketChannel();
        socketChannel.configureBlocking(false);
        socketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        socketChannel.setOption(StandardSocketOptions.SO_REUSEPORT, true);
        socketChannel.setReadListener(this::processMessages);
        socketChannel.setConnectListener(() -> {
            try {
                if (socketChannel.finishConnect()) {
                    seqNumBuffer.putLong(0, nextSeqNum);
                    socketChannel.write(seqNumBuffer);
                } else {
                    log.info().append("error finishing the socket connection: ").append(address).commit();
                    disconnect();
                }
            } catch (IOException e) {
                log.info().append("I/O exception finishing the socket connection: address=").append(address)
                        .append(", exception=").append(e)
                        .commit();
            }
        });
        socketChannel.connect(address);
    }

    private void heartbeat() {
        var currentTime = time.nanos();
        if (socketChannel == null) {
            try {
                connect();
            } catch (IOException e) {
                log.info().append("error connecting: address=").append(address)
                        .append(", exception=").append(e)
                        .commit();
                disconnect();
            }
        } else if (currentTime > lastMessageTime + HEARTBEAT_TIMEOUT) {
            log.info().append("heartbeat timeout, disconnecting: ").append(address).commit();
            disconnect();
        } else if (socketChannel.isConnected()) {
            try {
                socketChannel.write(heartbeat);
            } catch (IOException e) {
                log.info().append("error sending heartbeat, disconnecting: address=").append(address)
                        .append(", exception=").append(e)
                        .commit();
                disconnect();
            }
        }
    }

    @Command
    private void disconnect() {
        try {
            if (socketChannel != null) {
                socketChannel.close();
            }
        } catch (IOException e) {
            log.info().append("I/O exception closing socket channel: address=").append(address)
                    .append(", exception=").append(e)
                    .commit();
        }
        socketChannel = null;
    }

    /**
     * Sets the listener for messages that are published in sequence order.
     *
     * @param messageListener the listener for messages that are published in sequence order
     */
    public void setMessageListener(Consumer<DirectBuffer> messageListener) {
        this.messageListener = Objects.requireNonNull(messageListener);
    }

    private void processMessages() {
        try {
            // read packet
            var bytesRead = socketChannel.read(
                    packetBuffer, readBufferOffset, packetBuffer.capacity() - readBufferOffset);
            if (bytesRead == -1) {
                log.warn().append("server disconnect: ").append(address).commit();
                disconnect();
                return;
            }
            readBufferOffset += bytesRead;
            lastMessageTime = time.nanos();

            var offset = 0;

            while (offset + Short.BYTES < readBufferOffset) {
                var messageLength = packetBuffer.getShort(offset);
                if (messageLength == -2) {
                    // message length of -2 signifies a heartbeat with the next sequence number
                    if (offset + HEARTBEAT_LENGTH > readBufferOffset) {
                        break;
                    }

                    wrapper.wrap(packetBuffer, offset + Short.BYTES, MoldConstants.SESSION_LENGTH);
                    if (moldSession.getSessionName() == null) {
                        moldSession.setSessionName(BufferUtils.copy(wrapper));
                        log.info().append("new MoldUDP64 session: received=").append(moldSession.getSessionName())
                                .commit();
                    } else if (!moldSession.getSessionName().equals(wrapper)) {
                        log.warn().append("invalid session, disconnecting: expected=")
                                .append(moldSession.getSessionName())
                                .append(", received=").append(wrapper)
                                .commit();
                        disconnect();
                        return;
                    }

                    var nextSeqNum = packetBuffer.getLong(offset + Short.BYTES + MoldConstants.SESSION_LENGTH);
                    moldSession.setNextSequenceNumber(nextSeqNum);

                    offset += HEARTBEAT_LENGTH;
                } else {
                    if (offset + Short.BYTES + messageLength > readBufferOffset) {
                        break;
                    }

                    nextSeqNum++;
                    wrapper.wrap(packetBuffer, offset + Short.BYTES, messageLength);
                    messageListener.accept(wrapper);

                    offset += Short.BYTES + messageLength;
                }
            }
            BufferUtils.compact(packetBuffer, offset, readBufferOffset - offset);
            readBufferOffset -= offset;

            if (!activator.isReady() && nextSeqNum >= moldSession.getNextSequenceNumber()) {
                activator.ready();
            }
        } catch (IOException e) {
            log.warn().append("error processing message, disconnecting: ").append(e).commit();
            disconnect();
        }
    }

    @Command(path = "status", readOnly = true)
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("activator").object(activator)
                .string("session").object(moldSession)
                .string("address").object(address)
                .string("socket").object(socketChannel)
                .string("nextSeqNum").number(nextSeqNum)
                .closeMap();
    }

    @Override
    public String toString() {
        return toEncodedString();
    }
}
