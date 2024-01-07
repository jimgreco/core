package com.core.platform.bus.mold;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Directory;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.DatagramChannel;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.metrics.MetricFactory;
import com.core.infrastructure.time.Scheduler;
import com.core.platform.activation.Activatable;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

class MoldEventReceiver implements Activatable, Encodable {

    private static final long DISCOVERY_TIMEOUT = TimeUnit.SECONDS.toNanos(1);
    private static final long REWIND_TIMEOUT = TimeUnit.SECONDS.toNanos(1);

    private final Selector selector;
    @Property
    private final String eventChannelAddress;
    @Property
    private final String discoveryChannelAddress;
    private final MoldSession moldSession;
    private final Log log;
    @Directory(path = ".")
    private final Activator activator;
    private final Scheduler scheduler;
    private final List<String> rewindAddresses;
    private final MutableDirectBuffer packetBuffer;
    private final DirectBuffer packetBufferWrapper;
    private final Runnable onDiscoveryTimeout;
    private final Runnable onRewindTimeout;
    private DatagramChannel eventChannelSocket;
    private DatagramChannel discoveryChannelSocket;
    private DatagramChannel rewindSocket;
    private Consumer<DirectBuffer>[] eventListeners;
    private long rewindTaskId;
    private long discoveryTaskId;
    private boolean firstRewind;
    @Property
    private long nextSeqNum;

    @SuppressWarnings("unchecked")
    MoldEventReceiver(
            String busName,
            String name,
            Selector selector,
            Scheduler scheduler,
            LogFactory logFactory,
            MetricFactory metricFactory,
            ActivatorFactory activatorFactory,
            MoldSession moldSession,
            String eventChannelAddress,
            String discoveryChannelAddress) {
        this.selector = Objects.requireNonNull(selector, "selectService is null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(metricFactory, "metricFactory is null");
        Objects.requireNonNull(activatorFactory, "activationManager is null");
        this.moldSession = Objects.requireNonNull(moldSession, "moldSession is null");
        this.eventChannelAddress = Objects.requireNonNull(eventChannelAddress, "eventChannelAddress is null");
        this.discoveryChannelAddress = Objects.requireNonNull(
                discoveryChannelAddress, "discoveryChannelAddress is null");

        nextSeqNum = 1;
        log = logFactory.create(getClass());
        onDiscoveryTimeout = this::onDiscoveryTimeout;
        onRewindTimeout = this::onRewindTimeout;
        rewindAddresses = new CoreList<>();
        packetBuffer = BufferUtils.allocateDirect(MoldConstants.MTU_SIZE);
        packetBufferWrapper = BufferUtils.emptyBuffer();
        activator = activatorFactory.createActivator(name, this);
        eventListeners = new Consumer[0];

        metricFactory.registerGaugeMetric(
                "Mold_Session_NextSeqNum",
                moldSession::getNextSequenceNumber,
                "address", eventChannelAddress, "bus", busName);
        metricFactory.registerGaugeMetric(
                "Mold_EventReceiver_NextSeqNum",
                () -> nextSeqNum,
                "address", eventChannelAddress, "bus", busName);
    }

    @Override
    public void activate() {
        try {
            if (eventListeners.length == 0) {
                throw new IllegalStateException("eventListener not set");
            }
            firstRewind = true;

            log.info().append("joining and binding the event channel socket: ").append(eventChannelAddress).commit();
            eventChannelSocket = selector.createDatagramChannel();
            eventChannelSocket.configureBlocking(false);
            eventChannelSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            eventChannelSocket.setOption(StandardSocketOptions.SO_REUSEPORT, true);
            eventChannelSocket.join(eventChannelAddress);
            eventChannelSocket.bind(eventChannelAddress);
            eventChannelSocket.setReadListener(this::onEventChannelPacket);

            log.info().append("joining and binding the discovery channel socket: ").append(discoveryChannelAddress).commit();
            discoveryChannelSocket = selector.createDatagramChannel();
            discoveryChannelSocket.configureBlocking(false);
            discoveryChannelSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            discoveryChannelSocket.setOption(StandardSocketOptions.SO_REUSEPORT, true);
            discoveryChannelSocket.join(discoveryChannelAddress);
            discoveryChannelSocket.bind(discoveryChannelAddress);
            discoveryChannelSocket.setReadListener(this::onDiscoveryChannelPacket);

            rewindSocket = selector.createDatagramChannel();
            rewindSocket.configureBlocking(false);
            rewindSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            rewindSocket.setOption(StandardSocketOptions.SO_REUSEPORT, true);
            rewindSocket.bind(null);
            rewindSocket.setReadListener(this::onRewindPacket);
            log.info().append("bound the rewind socket: ").append(rewindSocket.getLocalAddress()).commit();

            activator.notReady("opened");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void deactivate() {
        try {
            activator.notReady("closed");

            firstRewind = false;
            if (eventChannelSocket != null) {
                log.info().append("closed the event channel socket: ").append(eventChannelAddress).commit();
                eventChannelSocket.close();
                eventChannelSocket = null;
            }
            if (discoveryChannelSocket != null) {
                log.info().append("closed the discovery channel socket: ").append(discoveryChannelAddress).commit();
                discoveryChannelSocket.close();
                discoveryChannelSocket = null;
            }
            if (rewindSocket != null) {
                log.info().append("closed the rewind socket: ").append(rewindSocket).commit();
                rewindSocket.close();
                rewindSocket = null;
            }
        } catch (IOException e) {
            throw new IllegalStateException("could not open sockets", e);
        }
    }

    void addEventListener(Consumer<DirectBuffer> eventListener) {
        this.eventListeners = Arrays.copyOf(eventListeners, eventListeners.length + 1);
        eventListeners[eventListeners.length - 1] = eventListener;
    }

    private void onDiscoveryChannelPacket() {
        try {
            var bytesRead = discoveryChannelSocket.receive(packetBuffer);
            if (bytesRead > 1) {
                // this is a rewind address
                rewindAddresses.add(BufferUtils.toAsciiString(packetBuffer, 0, bytesRead));
            }
        } catch (IOException e) {
            log.warn().append("error processing discovery channel packet, closing sockets: ").append(e).commit();
            activator.stop();
        }
    }

    private void onEventChannelPacket() {
        try {
            processMoldPacket(eventChannelSocket);

            if (isBehind()) {
                processBehind();
            } else if (firstRewind) {
                firstRewind = false;
                activator.ready();
            }
        } catch (IOException e) {
            log.warn().append("error processing event channel packet, closing sockets: ").append(e).commit();
            activator.stop();
        }
    }

    private void onRewindPacket() {
        try {
            rewindTaskId = scheduler.cancel(rewindTaskId);
            processMoldPacket(rewindSocket);

            if (isBehind()) {
                processBehind();
            } else {
                log.info().append("caught up: session=").append(moldSession.getSessionName())
                        .append(", seqNum=").append(nextSeqNum)
                        .append(", discoveryChannelAddress=").append(discoveryChannelAddress)
                        .commit();
                activator.ready();
                rewindSocket.disconnect();
            }
        } catch (IOException e) {
            log.warn().append("error processing rewind packet, closing sockets: ").append(e).commit();
            activator.stop();
        }
    }

    private void processMoldPacket(DatagramChannel socket) throws IOException {
        // read packet
        var bytesRead = socket.receive(packetBuffer);
        if (bytesRead < MoldConstants.HEADER_SIZE) {
            log.warn().append("dropping MoldUDP64 packet with less than 20 bytes: received=").append(bytesRead).commit();
            return;
        }

        // parse session
        if (moldSession.getSessionName() == null) {
            moldSession.setSessionName(BufferUtils.copy(packetBuffer, 0, MoldConstants.SESSION_LENGTH));
            log.info().append("new MoldUDP64 session: received=").append(moldSession.getSessionName()).commit();
        }
        packetBufferWrapper.wrap(packetBuffer, 0, MoldConstants.SESSION_LENGTH);
        if (!moldSession.getSessionName().equals(packetBufferWrapper)) {
            log.warn().append("dropping MoldUDP64 packet with different session: received=").append(packetBufferWrapper)
                    .append(", expected=").append(moldSession.getSessionName())
                    .commit();
            return;
        }

        // parse header
        var firstMsgSeqNum = packetBuffer.getLong(MoldConstants.SEQ_NUM_OFFSET);
        var numMessages = packetBuffer.getShort(MoldConstants.NUM_MESSAGES_OFFSET);
        var maxMsgSeqNum = firstMsgSeqNum + numMessages;
        moldSession.setNextSequenceNumber(Math.max(moldSession.getNextSequenceNumber(), maxMsgSeqNum));

        // process packet
        var offset = MoldConstants.HEADER_SIZE;
        var msgSeqNum = firstMsgSeqNum;
        while (msgSeqNum < maxMsgSeqNum) {
            // process event header
            if (offset + Short.BYTES > bytesRead) {
                log.warn().append("dropping remainder of MoldUDP64 packet with not enough bytes for message: seqNum=").append(msgSeqNum).commit();
                return;
            }
            var eventLength = packetBuffer.getShort(offset);
            offset += Short.BYTES;

            if (offset + eventLength > bytesRead) {
                log.warn().append("dropping remainder of MoldUDP64 packet with not enough bytes for message: seqNum=").append(msgSeqNum).commit();
                return;
            }

            // process event
            if (msgSeqNum == nextSeqNum) {
                // next event to process
                nextSeqNum++;
                packetBufferWrapper.wrap(packetBuffer, offset, eventLength);
                for (var eventListener : eventListeners) {
                    eventListener.accept(packetBufferWrapper);
                }
            } else if (msgSeqNum < nextSeqNum) {
                // the session is ahead and has already processed this event
                log.warn().append("dropping already processed event: seqNum=").append(msgSeqNum).commit();
            }
            offset += eventLength;
            msgSeqNum++;
        }
    }

    private void processBehind() throws IOException {
        if (rewindTaskId != 0 || discoveryTaskId != 0) {
            // waiting on a rewind or discovery request to complete or timeout
            return;
        }

        if (firstRewind) {
            // falling behind once caught up does not cause you to go inactive
            firstRewind = false;
            activator.notReady("rewinding");
        }

        if (rewindAddresses.isEmpty()) {
            sendDiscoveryRequest();
        } else {
            sendRewindRequest();
        }
    }

    private void sendDiscoveryRequest() throws IOException {
        if (discoveryChannelSocket == null) {
            return;
        }

        discoveryTaskId = scheduler.scheduleIn(discoveryTaskId, DISCOVERY_TIMEOUT, onDiscoveryTimeout,
                "MoldEventReceiver:sendDiscoveryRequest", 0);

        log.info().append("starting discovery request: ").append(discoveryChannelAddress).commit();

        packetBuffer.putByte(0, (byte) 'D');
        discoveryChannelSocket.send(packetBuffer, 0, 1, discoveryChannelAddress);
    }

    private void sendRewindRequest() throws IOException {
        if (rewindSocket == null) {
            return;
        }

        rewindTaskId = scheduler.scheduleIn(rewindTaskId, REWIND_TIMEOUT, onRewindTimeout,
                "MoldEventReceiver:sendRewindRequest", 0);

        var eventsBehind = getEventsBehind();
        var numMessages = (short) Math.min(eventsBehind, Short.MAX_VALUE);
        packetBuffer.putBytes(0, moldSession.getSessionName(), 0, moldSession.getSessionName().capacity());
        packetBuffer.putLong(MoldConstants.SEQ_NUM_OFFSET, nextSeqNum);
        packetBuffer.putShort(MoldConstants.NUM_MESSAGES_OFFSET, numMessages);

        if (!rewindSocket.isConnected()) {
            var rewindAddress = rewindAddresses.remove(rewindAddresses.size() - 1);
            log.info().append("starting rewind request: seqNum=").append(nextSeqNum)
                    .append(", numMessages=").append(eventsBehind)
                    .append(", rewinder=").append(rewindAddress)
                    .commit();
            rewindSocket.connect(rewindAddress);
        }

        rewindSocket.write(packetBuffer, 0, MoldConstants.HEADER_SIZE);
    }

    private void onDiscoveryTimeout() {
        try {
            log.info().append("completed discovery request: address=").append(discoveryChannelAddress)
                    .append(", numRewinders=").append(rewindAddresses.size())
                    .commit();
            discoveryTaskId = 0;
            if (isBehind()) {
                processBehind();
            }
        } catch (IOException e) {
            log.warn().append("error sending discovery request, closing sockets: ").append(e).commit();
            activator.stop();
        }
    }

    private void onRewindTimeout() {
        try {
            log.info().append("rewind request timeout: ").append(rewindSocket.getRemoteAddress()).commit();
            rewindTaskId = 0;
            rewindSocket.disconnect();
            if (isBehind()) {
                processBehind();
            }
        } catch (IOException e) {
            log.warn().append("error sending rewind request, closing sockets: ").append(e).commit();
            activator.stop();
        }
    }

    private boolean isBehind() {
        return nextSeqNum < moldSession.getNextSequenceNumber();
    }

    private long getEventsBehind() {
        return moldSession.getNextSequenceNumber() - nextSeqNum;
    }

    @Override
    public String toString() {
        return toEncodedString();
    }

    @Command(path = "status", readOnly = true)
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("activator").object(activator)
                .string("session").object(moldSession)
                .string("eventChannelSocket").object(eventChannelSocket)
                .string("discoveryChannelSocket").object(discoveryChannelSocket)
                .string("rewindSocket").object(rewindSocket)
                .string("nextSeqNum").number(nextSeqNum)
                .string("eventsBehind").number(getEventsBehind())
                .string("rewindTaskId").number(rewindTaskId)
                .string("discoveryTaskId").number(discoveryTaskId)
                .closeMap();
    }
}
