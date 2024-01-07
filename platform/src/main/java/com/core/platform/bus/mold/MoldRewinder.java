package com.core.platform.bus.mold;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.io.DatagramChannel;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.platform.activation.Activatable;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.util.Objects;

class MoldRewinder implements Activatable {

    private final Selector selector;
    private final String discoveryChannelAddress;
    private final Log log;
    private final Activator activator;
    private final MutableDirectBuffer readBuffer;
    private final DirectBuffer readBufferWrapper;
    private final MoldSession moldSession;
    private final MutableDirectBuffer writeBuffer;
    private final MessageStore messageStore;
    private DatagramChannel discoveryChannelSocket;
    private DatagramChannel rewindSocket;
    private DirectBuffer discoveryReply;

    MoldRewinder(
            String name, Selector selector, LogFactory logFactory, ActivatorFactory activatorFactory,
            MoldSession moldSession, MessageStore messageStore, String discoveryChannelAddress) {
        Objects.requireNonNull(name, "name is null");
        this.selector = Objects.requireNonNull(selector, "selectService is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(activatorFactory, "activationManager is null");
        this.moldSession = Objects.requireNonNull(moldSession, "moldSession is null");
        this.messageStore = Objects.requireNonNull(messageStore, "eventStore is null");
        this.discoveryChannelAddress = Objects.requireNonNull(
                discoveryChannelAddress, "discoveryChannelAddress is null");

        readBuffer = BufferUtils.allocate(MoldConstants.MTU_SIZE);
        readBufferWrapper = BufferUtils.emptyBuffer();
        writeBuffer = BufferUtils.allocate(2 * MoldConstants.MTU_SIZE);
        log = logFactory.create(getClass());

        activator = activatorFactory.createActivator(name, this, moldSession);
    }

    /**
     * Opens the event connector.
     * Events will be processed in order from the event channel.
     *
     * @throws IllegalStateException if an I/O error occurs
     */
    @Override
    public void activate() {
        try {
            log.info().append("joining and binding the discovery channel socket: ").append(discoveryChannelAddress).commit();
            discoveryChannelSocket = selector.createDatagramChannel();
            discoveryChannelSocket.configureBlocking(false);
            discoveryChannelSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            discoveryChannelSocket.setOption(StandardSocketOptions.SO_REUSEPORT, true);
            discoveryChannelSocket.join(discoveryChannelAddress);
            discoveryChannelSocket.bind(discoveryChannelAddress);
            discoveryChannelSocket.setReadListener(this::onMulticastRead);

            rewindSocket = selector.createDatagramChannel();
            rewindSocket.configureBlocking(false);
            rewindSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            rewindSocket.setOption(StandardSocketOptions.SO_REUSEPORT, true);
            rewindSocket.bind(null);
            rewindSocket.setReadListener(this::onUnicastRead);
            discoveryReply = BufferUtils.fromAsciiString(rewindSocket.getLocalAddress());
            log.info().append("binding the rewind socket: ").append(discoveryReply).commit();

            activator.ready();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Closes the event connector.
     * Events will no longer be processed from the event channel.
     */
    @Override
    public void deactivate() {
        try {
            activator.notReady("closed");

            if (discoveryChannelSocket != null) {
                log.info().append("closing the discovery channel socket: ").append(discoveryChannelAddress).commit();
                discoveryChannelSocket.close();
                discoveryChannelSocket = null;
            }
            if (rewindSocket != null) {
                log.info().append("closing the rewind socket: ").append(rewindSocket.getLocalAddress()).commit();
                rewindSocket.close();
                rewindSocket = null;
                discoveryReply = null;
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void onMulticastRead() {
        try {
            var bytesRead = discoveryChannelSocket.receive(readBuffer);
            if (bytesRead == 1) {
                discoveryChannelSocket.send(discoveryReply, 0, discoveryReply.capacity(), discoveryChannelAddress);
            }
        } catch (IOException e) {
            log.warn().append("error read/write from discovery channel socket, closing: channel=").append(discoveryChannelAddress)
                    .append(", exception=").append(e)
                    .commit();
            activator.stop();
        }
    }

    private void onUnicastRead() {
        try {
            var bytesRead = rewindSocket.receive(readBuffer, 0, readBuffer.capacity());
            if (bytesRead < MoldConstants.HEADER_SIZE) {
                log.warn().append("dropping rewind request with < 20 bytes").commit();
                return;
            }

            // verify session
            readBufferWrapper.wrap(readBuffer, 0, MoldConstants.SESSION_LENGTH);
            if (!readBufferWrapper.equals(moldSession.getSessionName())) {
                log.warn().append("dropping rewind request with different session: received=").append(readBufferWrapper)
                        .append(", expected=").append(moldSession.getSessionName())
                        .commit();
            }

            var msgSeqNum = readBuffer.getLong(MoldConstants.SEQ_NUM_OFFSET);
            var numMessages = readBuffer.getShort(MoldConstants.NUM_MESSAGES_OFFSET);
            if (msgSeqNum <= 0 || numMessages <= 0 || msgSeqNum + numMessages > moldSession.getNextSequenceNumber()) {
                log.warn().append("dropping rewind request with invalid sequence number or num messages: msgSeqNum=").append(msgSeqNum)
                        .append(", numMessages=").append(numMessages)
                        .append(", sessionMsgSeqNum=").append(moldSession.getNextSequenceNumber())
                        .commit();
                return;
            }

            // write events, one at a time
            var currentSeqNum = msgSeqNum;
            var offset = MoldConstants.HEADER_SIZE;
            while (currentSeqNum < msgSeqNum + numMessages) {
                var length = messageStore.read(writeBuffer, offset, currentSeqNum);
                if (offset + length < MoldConstants.MTU_SIZE) {
                    currentSeqNum++;
                    offset += length;
                } else {
                    // the events are too big for the packet
                    break;
                }
            }

            // write header
            var packetMessages = (short) (currentSeqNum - msgSeqNum);
            writeBuffer.putBytes(0, moldSession.getSessionName(), 0, moldSession.getSessionName().capacity());
            writeBuffer.putLong(MoldConstants.SEQ_NUM_OFFSET, msgSeqNum);
            writeBuffer.putShort(MoldConstants.NUM_MESSAGES_OFFSET, packetMessages);
            rewindSocket.reply(writeBuffer, 0, offset);
        } catch (IOException e) {
            log.warn().append("error reading rewind socket, closing: ").append(e).commit();
            activator.stop();
        }
    }
}
