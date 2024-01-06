package com.core.platform.bus.mold;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Directory;
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
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.util.Objects;
import java.util.function.Consumer;

class MoldCommandReceiver implements Activatable, Encodable {

    private final Selector selector;
    @Property
    private final String commandChannelAddress;
    private final MoldSession moldSession;
    @Directory(path = ".")
    private final Activator activator;
    private final MutableDirectBuffer packetBuffer;
    private final DirectBuffer packetBufferWrapper;
    private final Log log;
    private final boolean multicast;
    private DatagramChannel commandChannel;
    private Consumer<DirectBuffer> commandListener;

    MoldCommandReceiver(
            Selector selector,
            LogFactory logFactory,
            ActivatorFactory activatorFactory,
            MoldSession moldSession,
            String commandChannelAddress,
            boolean multicast) {
        this.selector = Objects.requireNonNull(selector, "selectService is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(activatorFactory, "activationManager is null");
        this.moldSession = Objects.requireNonNull(moldSession, "moldSession is null");
        this.commandChannelAddress = Objects.requireNonNull(commandChannelAddress, "commandChannelAddress is null");
        this.multicast = multicast;

        packetBuffer = BufferUtils.allocateDirect(MoldConstants.MTU_SIZE);
        packetBufferWrapper = BufferUtils.emptyBuffer();

        log = logFactory.create(getClass());
        activator = activatorFactory.createActivator(
                "MoldCommandReceiver:" + commandChannelAddress, this, moldSession);
    }

    @Override
    public void activate() {
        try {
            log.info().append("joining and binding the command channel socket: ").append(commandChannelAddress).commit();
            commandChannel = selector.createDatagramChannel();
            commandChannel.configureBlocking(false);
            commandChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            commandChannel.setOption(StandardSocketOptions.SO_REUSEPORT, true);

            if (multicast) {
                commandChannel.join(commandChannelAddress);
            }

            commandChannel.bind(commandChannelAddress);
            commandChannel.setReadListener(this::onCommand);

            activator.ready();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void deactivate() {
        try {
            activator.notReady("closed");

            log.info().append("closed the command channel socket: ").append(commandChannelAddress).commit();
            commandChannel.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Sets command listener.
     *
     * @param commandListener the command listener
     */
    public void setCommandListener(Consumer<DirectBuffer> commandListener) {
        this.commandListener = Objects.requireNonNull(commandListener, "commandListener is null");
    }

    private void onCommand() {
        try {
            // read packet
            var bytesRead = commandChannel.receive(packetBuffer);
            if (bytesRead < MoldConstants.HEADER_SIZE) {
                log.warn().append("dropping MoldUdp64 packet with less than 20 bytes: received=").append(bytesRead).commit();
                return;
            }

            // parse session
            packetBufferWrapper.wrap(packetBuffer, 0, MoldConstants.SESSION_LENGTH);
            if (!moldSession.getSessionName().equals(packetBufferWrapper)) {
                log.warn().append("dropping MoldUdp64 packet with different session: received=").append(packetBufferWrapper)
                        .append(", expected=").append(moldSession.getSessionName())
                        .commit();
                return;
            }

            // parse header
            var numCommands = packetBuffer.getShort(MoldConstants.NUM_MESSAGES_OFFSET);

            // process packet
            var offset = MoldConstants.HEADER_SIZE;
            var command = 0;
            while (command < numCommands) {
                // process command header
                if (offset + Short.BYTES > bytesRead) {
                    log.warn().append("dropping remainder of MoldUdp64 packet with not enough bytes for command: command=").append(command + 1).commit();
                    return;
                }
                var commandLength = packetBuffer.getShort(offset);
                offset += Short.BYTES;

                if (offset + commandLength > bytesRead) {
                    log.warn().append("dropping remainder of MoldUdp64 packet with not enough bytes for command: command=").append(command + 1).commit();
                    return;
                }

                packetBufferWrapper.wrap(packetBuffer, offset, commandLength);
                commandListener.accept(packetBufferWrapper);
                command++;
                offset += commandLength;
            }
        } catch (IOException e) {
            log.warn().append("error processing command channel packet, closing sockets: ").append(e).commit();
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
                .string("commandChannelAddress").string(commandChannelAddress)
                .closeMap();
    }
}
