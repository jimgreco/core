package com.core.platform.bus.mold;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Directory;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.messages.Dispatcher;
import com.core.infrastructure.messages.Provider;
import com.core.infrastructure.messages.Schema;
import com.core.infrastructure.metrics.MetricFactory;
import com.core.infrastructure.time.Scheduler;
import com.core.infrastructure.time.Time;
import com.core.platform.activation.Activatable;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.bus.AbstractBusServer;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.Objects;
import java.util.Random;
import java.util.function.Consumer;

/**
 * The {@code TcpBusServer} is an implementation of {@code BusServer} for buses that receives messages over UDP and
 * sends messages to clients connected over TCP.
 *
 * @param <DispatcherT> the dispatcher type
 * @param <ProviderT> the provider type
 */
public class TcpBusServer<DispatcherT extends Dispatcher, ProviderT extends Provider>
        extends AbstractBusServer<DispatcherT, ProviderT>
        implements Activatable, Encodable {

    @Directory
    private final MoldSession session;
    @Directory
    private final MoldCommandReceiver commandReceiver;
    @Directory
    private final TcpMessageReceiver messageReceiver;
    @Directory
    private final TcpMessagePublisher messagePublisher;

    @Directory(path = ".")
    private final Activator activator;

    private final Time time;
    private final int timestampOffset;

    private MutableDirectBuffer messageBuffer;

    /**
     * Creates a {@code TcpBusServer} and the components required to operate a MoldUDP64 bus from the specified
     * parameters.
     *
     * @param selector a selector for asynchronous I/O
     * @param time a real-time source of timestamp
     * @param scheduler a real-time scheduler of tasks
     * @param logFactory a factory to create logs
     * @param metricFactory a factory to create metrics
     * @param activatorFactory a factory to crate activators
     * @param schema the message schema the sequencer uses
     * @param messageStore a store of messages
     * @param messageSendAddress the address to receive messages from
     * @param messageReceiveAddress the address to send messages to
     */
    public TcpBusServer(
            Selector selector,
            Time time,
            Scheduler scheduler,
            LogFactory logFactory,
            MetricFactory metricFactory,
            ActivatorFactory activatorFactory,
            Schema<DispatcherT, ProviderT> schema,
            MessageStore messageStore,
            String messageSendAddress,
            String messageReceiveAddress) {
        super(schema);
        Objects.requireNonNull(selector, "selectService is null");
        this.time = Objects.requireNonNull(time, "time is null");
        Objects.requireNonNull(scheduler, "scheduler is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(metricFactory, "metricFactory is null");
        Objects.requireNonNull(activatorFactory, "activationManager is null");
        Objects.requireNonNull(messageStore, "messageStore is null");
        Objects.requireNonNull(messageSendAddress, "messageSendAddress is null");
        Objects.requireNonNull(messageReceiveAddress, "messageReceiveAddress is null");

        timestampOffset = getSchema().getTimestampOffset();

        session = new MoldSession(
                "MoldServerSession:" + messageSendAddress, time, activatorFactory);

        messagePublisher = new TcpMessagePublisher(
                selector,
                time,
                scheduler,
                logFactory,
                activatorFactory,
                session,
                messageStore,
                messageSendAddress);

        messageReceiver = new TcpBusServerMessageReceiver(
                selector,
                time,
                scheduler,
                logFactory,
                metricFactory,
                activatorFactory,
                session,
                "TcpServerMessageReceiver:" + messageSendAddress,
                messageSendAddress);

        commandReceiver = new MoldCommandReceiver(
                selector,
                logFactory,
                activatorFactory,
                session,
                messageReceiveAddress,
                false);

        activator = activatorFactory.createActivator(
                "TcpBusServer:" + messageSendAddress,
                this,
                session, commandReceiver, messagePublisher);
    }

    @Override
    public MutableDirectBuffer acquire() {
        messageBuffer = messagePublisher.acquire();
        return messageBuffer;
    }

    @Override
    public void commit(int msgLength) {
        if (activator.isActive()) {
            messageBuffer.putLong(timestampOffset, time.nanos());
        }
        messagePublisher.commit(msgLength);
    }

    @Override
    public void commit(int msgLength, long timestamp) {
        if (activator.isActive()) {
            messageBuffer.putLong(timestampOffset, timestamp);
        }
        messagePublisher.commit(msgLength);
    }

    @Override
    public boolean isActive() {
        return activator.isActive();
    }

    @Override
    public void send() {
        // do nothing, sent over TCP so send is not required to form a packet
    }

    /**
     * Creates the session with the specified session suffix.
     * The session is 10-characters with the following format: {@code yyyyMMddXX}.
     * Where {@code yyyyMMdd} is the date the session was created (from the UTC timezone) and
     * {@code XX} is the {@code sessionSuffix} parameter.
     *
     * @param sessionSuffix the suffix of the session
     * @throws IllegalStateException if the session has already been created or set
     * @throws IllegalArgumentException if {@code sessionSuffix} is not 2 bytes
     */
    @Command
    public void createSession(DirectBuffer sessionSuffix) {
        session.create(sessionSuffix);
    }

    /**
     * Creates a session with a random suffix.
     *
     * @apiNote this is useful for testing where the session needs to be unique
     */
    @Command
    public void randomSession() {
        var random = new Random();
        char letter1 = (char) ('A' + random.nextInt(26));
        char letter2 = (char) ('A' + random.nextInt(26));
        session.create(BufferUtils.fromAsciiString("" + letter1 + letter2));
    }

    @Override
    public void activate() {
        messageReceiver.deactivate();
        activator.ready();
    }

    @Override
    public void deactivate() {
        messageReceiver.activate();
        activator.notReady("backup mode");
    }

    @Override
    public void setEventListener(Consumer<DirectBuffer> messageListener) {
        messageReceiver.setMessageListener(messageListener);
    }

    @Override
    public void setCommandListener(Consumer<DirectBuffer> messageListener) {
        commandReceiver.setCommandListener(messageListener);
    }

    @Command(path = "status", readOnly = true)
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("session").object(session)
                .string("activator").object(activator)
                .string("messageReceiver").object(messageReceiver)
                .string("messagePublisher").object(messagePublisher)
                .string("commandReceiver").object(commandReceiver)
                .closeMap();
    }

    @Override
    public String toString() {
        return toEncodedString();
    }
}
