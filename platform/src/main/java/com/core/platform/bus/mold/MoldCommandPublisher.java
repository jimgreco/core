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
import com.core.infrastructure.messages.Decoder;
import com.core.infrastructure.messages.Encoder;
import com.core.infrastructure.messages.Schema;
import com.core.infrastructure.time.Scheduler;
import com.core.platform.activation.Activatable;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.activation.Activator;
import com.core.platform.bus.BusClient;
import com.core.infrastructure.messages.MessagePublisher;
import com.core.platform.shell.Shell;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Sends MoldUDP64 packets across a multicast bus.
 *
 * <p>MoldUDP64 packet have the following format.
 * <table>
 *     <tr>
 *         <th>Byte</th>
 *         <th>Length</th>
 *         <th>Description</th>
 *     </tr>
 *     <tr>
 *         <td>0</td>
 *         <td>10</td>
 *         <td>Session</td>
 *     </tr>
 *     <tr>
 *         <td>10</td>
 *         <td>8</td>
 *         <td>First message sequence number</td>
 *     </tr>
 *     <tr>
 *         <td>18</td>
 *         <td>2</td>
 *         <td>Number of messages in the packet</td>
 *     </tr>
 *     <tr>
 *         <td>20</td>
 *         <td>2</td>
 *         <td>The length of the message in the packet</td>
 *     </tr>
 *     <tr>
 *         <td>22</td>
 *         <td>N</td>
 *         <td>The message contents</td>
 *     </tr>
 * </table>
 *
 * <p>This command publisher only supports a single message in the packet.
 * The second, third, ..., Nth message in the packet would be stored sequentially after the first message with a 2-byte
 * length and the message contents for each message.
 */
class MoldCommandPublisher implements Encodable, MessagePublisher, Activatable {

    private static final int INITIAL_MAX_MESSAGES = 1000;
    private static final long SEND_TIMEOUT = TimeUnit.MILLISECONDS.toNanos(100);
    private static final DirectBuffer VM_PROPERTY = BufferUtils.fromAsciiString("vm_name");

    private final Shell shell;
    private final Log log;
    private final Selector selector;
    private final MoldSession moldSession;
    private final Scheduler scheduler;
    @Property
    private final String applicationName;
    private final String scheduledTaskName;
    private final DirectBuffer applicationNameBuf;
    private final MutableDirectBuffer messageWrapper;
    private final Activator activator;
    private final Runnable onSendTimeout;
    private final Schema<?, ?> schema;
    private final Encoder appDefinitionEncoder;
    private final String applicationDefinitionNameField;
    private final Encoder appDiscoveryEncoder;
    private final boolean multicast;
    private final Object associatedObject;

    @Property
    private String commandChannelAddress;
    private Packet[] packets;
    private Packet currentPacket;
    @Property
    private int nextConfirmedPacketIndex;
    @Property
    private int nextPacketIndex;
    @Property
    private int nextConfirmedAppSeqNum;
    @Property
    private int nextAppSeqNum;

    private DatagramChannel commandChannel;
    @Property
    private short appId;
    private long sendTimeoutTaskId;
    private Runnable allCommandsClearedListener;

    MoldCommandPublisher(
            Shell shell,
            Selector selector,
            Scheduler scheduler,
            LogFactory logFactory,
            ActivatorFactory activatorFactory,
            BusClient<?, ?> busClient,
            MoldSession moldSession,
            Object eventReceiver,
            String applicationName,
            String commandChannelAddress,
            boolean multicast,
            Object associatedObject) {
        this.shell = Objects.requireNonNull(shell, "shell is null");
        this.selector = Objects.requireNonNull(selector, "selectService is null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(busClient, "busClient is null");
        Objects.requireNonNull(activatorFactory, "activationManager is null");
        this.moldSession = Objects.requireNonNull(moldSession, "moldSession is null");
        Objects.requireNonNull(eventReceiver, "eventReceiver is null");
        this.applicationName = Objects.requireNonNull(applicationName, "applicationName is null");
        this.commandChannelAddress = Objects.requireNonNull(
                commandChannelAddress, "commandChannelAddress is null");
        this.multicast = multicast;
        this.associatedObject = Objects.requireNonNull(associatedObject, "associatedObject is null");

        schema = busClient.getSchema();
        nextAppSeqNum = 1;
        nextConfirmedAppSeqNum = 1;

        packets = new Packet[INITIAL_MAX_MESSAGES];
        for (var i = 0; i < packets.length; i++) {
            packets[i] = new Packet();
        }
        currentPacket = packets[0];
        currentPacket.init(nextAppSeqNum);

        messageWrapper = BufferUtils.mutableEmptyBuffer();
        log = logFactory.create(getClass());
        onSendTimeout = this::onSendTimeout;
        applicationNameBuf = BufferUtils.fromAsciiString(applicationName);
        scheduledTaskName = "MoldCommandPublisher:" + applicationName;

        var dispatcher = busClient.getDispatcher();
        dispatcher.addListenerBeforeDispatch(this::onBeforeMessage);
        dispatcher.addListenerAfterDispatch(this::onAfterMessage);

        // write out the application definition for this publisher
        appDefinitionEncoder = schema.createEncoder(schema.getProperty("applicationDefinitionMessageName"));
        applicationDefinitionNameField = schema.getProperty("applicationDefinitionNameField");
        commit(appDefinitionEncoder.wrap(acquire())
                .set(applicationDefinitionNameField, applicationNameBuf)
                .length());
        send();

        appDiscoveryEncoder = schema.createEncoder("applicationDiscovery");

        activator = activatorFactory.createActivator("MoldCommandPublisher:" + applicationName, this, eventReceiver);
    }

    @Override
    public void activate() {
        try {
            commandChannel = selector.createDatagramChannel();
            commandChannel.configureBlocking(false);
            commandChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            commandChannel.setOption(StandardSocketOptions.SO_REUSEPORT, true);

            if (multicast) {
                commandChannel.join(commandChannelAddress);
            }

            commandChannel.connect(commandChannelAddress);

            initSession();

            if (appId != 0) {
                activator.ready();
            }

            commit(appDiscoveryEncoder.wrap(acquire())
                    .set("vmName", shell.getPropertyValue(VM_PROPERTY))
                    .set("commandPath", BufferUtils.temp(shell.getPath(associatedObject)))
                    .set("activationStatus", (byte) 1)
                    .length());
            send();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void initSession() {
        var session = moldSession.getSessionName();
        for (var packet : packets) {
            packet.buffer.putBytes(0, session, 0, session.capacity());
            packet.buffer.putLong(MoldConstants.SEQ_NUM_OFFSET, 0);
        }
    }

    @Override
    public void deactivate() {
        try {
            commit(appDiscoveryEncoder.wrap(acquire())
                    .set("vmName", shell.getPropertyValue(VM_PROPERTY))
                    .set("commandPath", BufferUtils.temp(shell.getPath(associatedObject)))
                    .set("activationStatus", (byte) 2)
                    .length());
            send();

            activator.notReady("closed");
            commandChannel.close();
            sendTimeoutTaskId = scheduler.cancel(sendTimeoutTaskId);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public MutableDirectBuffer acquire() {
        messageWrapper.wrap(currentPacket.buffer, currentPacket.length + Short.BYTES, MoldConstants.MAX_MESSAGE_SIZE);
        return messageWrapper;
    }

    @Override
    public void commit(int msgLength) {
        if (msgLength > MoldConstants.MAX_MESSAGE_SIZE) {
            throw new IllegalArgumentException("message length larger than 1450 bytes: " + msgLength);
        }

        var fullMsgLength = Short.BYTES + msgLength;
        if (currentPacket.length + fullMsgLength <= MoldConstants.MTU_SIZE) {
            // sets the applicationId and applicationSequenceNumber in the message header
            messageWrapper.putShort(schema.getApplicationIdOffset(), appId);
            messageWrapper.putInt(schema.getApplicationSequenceNumberOffset(), nextAppSeqNum);
            nextAppSeqNum++;
            // clear so the buffer can't be written to again
            messageWrapper.wrap(0, 0);

            // new message fits in the existing packet
            currentPacket.buffer.putShort(currentPacket.length, (short) msgLength);
            currentPacket.length += fullMsgLength;
            currentPacket.numMessages++;
        } else {
            // does not fit in the existing packet, send out, then acquire and copy the bytes
            var oldPacket = currentPacket;
            var oldPacketMessageOffset = currentPacket.length + Short.BYTES;
            send();
            var newMessageBuffer = acquire();
            newMessageBuffer.putBytes(0, oldPacket.buffer, oldPacketMessageOffset, msgLength);
            commit(msgLength);
        }
    }

    @Override
    public void send() {
        if (currentPacket.numMessages != 0) {
            // set the number of messages in the packet header
            currentPacket.buffer.putShort(MoldConstants.NUM_MESSAGES_OFFSET, currentPacket.numMessages);

            nextPacketIndex++;
            checkResize();
            currentPacket = packets[nextPacketIndex % packets.length];
            currentPacket.init(nextAppSeqNum);
        }

        try {
            if (sendTimeoutTaskId == 0 && nextConfirmedPacketIndex < nextPacketIndex
                    && commandChannel != null && commandChannel.isOpen()) {
                var packetInFlight = packets[nextConfirmedPacketIndex % packets.length];
                sendTimeoutTaskId = scheduler.scheduleIn(
                        sendTimeoutTaskId, SEND_TIMEOUT, onSendTimeout, scheduledTaskName,
                        packetInFlight.firstAppSeqNum);
                commandChannel.write(packetInFlight.buffer, 0, packetInFlight.length);
            }
        } catch (IOException e) {
            log.warn().append("error sending command, closing command channel socket: application=")
                    .append(applicationName)
                    .append(", appSeqNum=").append(nextConfirmedAppSeqNum)
                    .append(", exception=").append(e)
                    .commit();
            activator.stop();
        }
    }

    @Override
    public String getApplicationName() {
        return applicationName;
    }

    private void checkResize() {
        if (nextConfirmedPacketIndex + packets.length == nextPacketIndex) {
            // resize
            var newPackets = new Packet[2 * packets.length];
            log.warn().append("resizing packet buffer: oldMaxPackets=").append(packets.length)
                    .append(", newMaxPackets=").append(newPackets.length)
                    .commit();

            // copy over old packets
            for (var i = nextConfirmedPacketIndex; i < nextPacketIndex; i++) {
                newPackets[i % newPackets.length] = packets[i % packets.length];
            }
            // fill in empty new packets
            for (var i = 0; i < newPackets.length; i++) {
                if (newPackets[i] == null) {
                    newPackets[i] = new Packet();
                }
            }

            packets = newPackets;
            initSession();
        }
    }

    @Override
    public short getApplicationId() {
        return appId;
    }

    @Override
    public boolean isCurrent() {
        return nextConfirmedAppSeqNum == nextAppSeqNum;
    }

    private void onSendTimeout() {
        log.warn().append("command sending timeout: session=").append(moldSession.getSessionName())
                .append(", application=").append(applicationName)
                .append(", appSeqNum=").append(nextConfirmedAppSeqNum)
                .append(", nextAppSeqNum=").append(nextAppSeqNum)
                .commit();
        sendTimeoutTaskId = 0;
        send();
    }

    private void onBeforeMessage(Decoder decoder) {
        if (decoder.getApplicationId() == appId) {
            checkPacket(decoder);

            send();
        } else if (appId == 0
                && decoder.messageName().equals(appDefinitionEncoder.messageName())
                && applicationNameBuf.equals(decoder.get(applicationDefinitionNameField))) {
            appId = decoder.getApplicationId();
            checkPacket(decoder);

            // write the applicationId for all the queued messages
            for (var i = nextConfirmedPacketIndex; i < nextPacketIndex; i++) {
                var packet = packets[i % packets.length];
                var packetOffset = MoldConstants.HEADER_SIZE;
                while (packetOffset < packet.length) {
                    var msgLength = packet.buffer.getShort(packetOffset);
                    packetOffset += Short.BYTES;
                    packet.buffer.putShort(packetOffset + schema.getApplicationIdOffset(), appId);
                    packetOffset += msgLength;
                }
            }

            activator.ready();
            send();
        }
    }

    private void checkPacket(Decoder decoder) {
        nextConfirmedAppSeqNum = decoder.getApplicationSequenceNumber() + 1;
        var finishedPacket = false;

        var packet = packets[nextConfirmedPacketIndex % packets.length];
        while (nextConfirmedPacketIndex < nextPacketIndex
                && packet.firstAppSeqNum + packet.numMessages <= nextConfirmedAppSeqNum) {
            finishedPacket = true;
            packet.reset();
            nextConfirmedPacketIndex++;
            packet = packets[nextConfirmedPacketIndex % packets.length];
        }

        if (finishedPacket) {
            sendTimeoutTaskId = scheduler.cancel(sendTimeoutTaskId);
        }
    }

    @SuppressWarnings("PMD.UnusedFormalParameter")
    private void onAfterMessage(Decoder decoder) {
        if (isCurrent() && allCommandsClearedListener != null) {
            allCommandsClearedListener.run();
            allCommandsClearedListener = null;
        }
    }

    @Override
    public String toString() {
        return toEncodedString();
    }

    @Command(path = "status")
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("session").object(moldSession)
                .string("application").string(applicationName)
                .string("commandChannel").string(commandChannelAddress)
                .string("commandChannelOpen").bool(commandChannel != null && commandChannel.isOpen())
                .string("activator").object(activator)
                .string("allocatedPackets").number(packets.length)
                .string("nextConfirmedPacket").number(nextConfirmedPacketIndex)
                .string("nextPacket").number(nextPacketIndex)
                .string("nextConfirmedAppSeqNum").number(nextConfirmedAppSeqNum)
                .string("nextAppSeqNum").number(nextAppSeqNum)
                .string("current").bool(isCurrent())
                .closeMap();
    }

    void setCommandChannelAddress(String commandChannelAddress) {
        this.commandChannelAddress = commandChannelAddress;
    }

    private static class Packet {

        MutableDirectBuffer buffer;
        short length;
        int firstAppSeqNum;
        short numMessages;

        Packet() {
            buffer = BufferUtils.allocate(2 * MoldConstants.MTU_SIZE);
            reset();
        }

        void init(int firstAppSeqNum) {
            length = MoldConstants.HEADER_SIZE;
            this.firstAppSeqNum = firstAppSeqNum;
            numMessages = 0;
        }

        void reset() {
            length = -1;
            firstAppSeqNum = -1;
            numMessages = -1;
        }
    }
}
