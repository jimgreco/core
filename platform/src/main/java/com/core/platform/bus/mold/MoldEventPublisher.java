package com.core.platform.bus.mold;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.DatagramChannel;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.platform.activation.Activatable;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.util.Objects;

class MoldEventPublisher implements Activatable, Encodable {

    private final Selector selector;
    @Property
    private final String eventChannelAddress;
    private final MoldSession moldSession;
    private final Log log;
    private final Activator activator;
    private final MutableDirectBuffer packetHeader;
    private final MutableDirectBuffer eventWrapper;
    private final MessageStore messageStore;
    private final int[] eventLengths;
    private final ByteBuffer[] packetHeaderAndEvents;
    private DatagramChannel eventChannel;
    private int position;
    private short eventsInPacket;
    private MutableDirectBuffer eventBuffer;

    MoldEventPublisher(
            Selector selector,
            LogFactory logFactory,
            ActivatorFactory activatorFactory,
            MoldSession moldSession,
            MessageStore messageStore,
            String eventChannelAddress) {
        this.selector = Objects.requireNonNull(selector, "selectService is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(activatorFactory, "activationManager is null");
        this.moldSession = Objects.requireNonNull(moldSession, "moldSession is null");
        this.messageStore = Objects.requireNonNull(messageStore, "eventStore is null");
        this.eventChannelAddress = Objects.requireNonNull(eventChannelAddress, "eventChannelAddress is null");

        packetHeader = BufferUtils.allocateDirect(MoldConstants.HEADER_SIZE);
        eventWrapper = BufferUtils.mutableEmptyBuffer();
        packetHeaderAndEvents = new ByteBuffer[2];
        eventLengths = new int[100];
        log = logFactory.create(getClass());

        activator = activatorFactory.createActivator(
                "MoldEventPublisher:" + eventChannelAddress, this, moldSession);
    }

    @Override
    public void activate() {
        try {
            log.info().append("opening event connector: session=").append(moldSession.getSessionName())
                    .append(", eventChannel=").append(eventChannelAddress)
                    .commit();

            eventsInPacket = 0;

            var sessionName = moldSession.getSessionName();
            packetHeader.putBytes(MoldConstants.SESSION_OFFSET, sessionName, 0, sessionName.capacity());
            messageStore.open(sessionName);

            eventChannel = selector.createDatagramChannel();
            eventChannel.configureBlocking(false);
            eventChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            eventChannel.setOption(StandardSocketOptions.SO_REUSEPORT, true);
            eventChannel.join(eventChannelAddress);
            eventChannel.connect(eventChannelAddress);

            activator.ready();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void deactivate() {
        try {
            activator.notReady("closed");

            log.info().append("closing event connector: session=").append(moldSession.getSessionName())
                    .append(", eventChannel=").append(eventChannelAddress)
                    .commit();

            eventChannel.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Returns a buffer for writing events to the MoldUDP64 packet.
     *
     * @return a buffer for writing events to the MoldUdp64 packet
     */
    public MutableDirectBuffer acquire() {
        if (eventsInPacket == 0) {
            eventBuffer = messageStore.acquire();
            position = 0;
        }
        var offset = position + Short.BYTES;
        eventWrapper.wrap(eventBuffer, offset, MoldConstants.MTU_SIZE - MoldConstants.HEADER_SIZE - offset);
        return eventWrapper;
    }

    /**
     * Commits the event to the MoldUDP64 packet.
     *
     * @param eventLength the number of bytes in the event
     */
    public void commit(int eventLength) {
        eventBuffer.putShort(position, (short) eventLength);

        var len = Short.BYTES + eventLength;
        position += len;
        eventLengths[eventsInPacket] = len;
        eventsInPacket++;
    }

    /**
     * Sends the MoldUdp64 packet to event channel.
     */
    public void send() {
        try {
            if (eventsInPacket == 0) {
                return;
            }

            // update the packet header
            packetHeader.putLong(MoldConstants.SEQ_NUM_OFFSET, moldSession.getNextSequenceNumber());
            packetHeader.putShort(MoldConstants.NUM_MESSAGES_OFFSET, eventsInPacket);

            // update store
            messageStore.commit(eventLengths, 0, eventsInPacket);

            // update session
            moldSession.setNextSequenceNumber(moldSession.getNextSequenceNumber() + eventsInPacket);
            eventsInPacket = 0;

            // send to the event channel
            if (eventChannel.isOpen()) {
                packetHeaderAndEvents[0] = BufferUtils.byteBuffer(packetHeader, 0, packetHeader.capacity());
                packetHeaderAndEvents[1] = BufferUtils.byteBuffer(eventBuffer, 0, position);
                var bytesWritten = eventChannel.write(packetHeaderAndEvents, 0, 2);
                if (bytesWritten != position + MoldConstants.HEADER_SIZE) {
                    log.warn().append("write buffer full: session=").append(moldSession.getSessionName())
                            .append(", nextSeqNum=").append(moldSession.getNextSequenceNumber())
                            .commit();
                }
            }
        } catch (IOException e) {
            log.warn().append("error sending event, closing event channel socket: session=").append(moldSession.getSessionName())
                    .append(", nextSeqNum=").append(moldSession.getNextSequenceNumber())
                    .commit();
            activator.stop();
        }
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
                .string("eventChannelAddress").string(eventChannelAddress)
                .string("messageStore").object(messageStore)
                .closeMap();
    }
}
