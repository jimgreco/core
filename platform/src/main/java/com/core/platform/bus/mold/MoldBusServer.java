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
import java.util.function.Consumer;

/**
 * The {@code MoldBusServer} is an implementation of {@code BusServer} for buses that communicate with the MoldUdp64
 * protocol.
 *
 * <p>The MoldUDP64 protocol is located on the
 * <a href="https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/moldudp64.pdf">Nasdaq
 * website</a>
 *
 * @param <DispatcherT> the dispatcher type
 * @param <ProviderT> the provider type
 */
public class MoldBusServer<DispatcherT extends Dispatcher, ProviderT extends Provider>
        extends AbstractBusServer<DispatcherT, ProviderT>
        implements Activatable, Encodable {

    @Directory
    private final MoldSession session;
    @Directory(failIfExists = false)
    private final MoldEventReceiver eventReceiver;
    @Directory
    private final MoldCommandReceiver commandReceiver;
    @Directory
    private final MoldEventPublisher eventPublisher;
    @Directory(path = ".")
    private final Activator activator;
    private final Time time;
    private final int timestampOffset;

    private MutableDirectBuffer messageBuffer;

    /**
     * Creates a {@code MoldBusServer} and the components required to operate a MoldUDP64 bus from the specified
     * parameters.
     *
     * @param selector a selector for asynchronous I/O
     * @param time a real-time source of timestamp
     * @param scheduler a real-time scheduler of tasks
     * @param logFactory a factory to create logs
     * @param metricFactory a factory to create metrics
     * @param activatorFactory a factory to crate activators
     * @param busName the unique name for this bus in this VM
     * @param schema the message schema the sequencer uses
     * @param messageStore a store of messages
     * @param eventChannelAddress the address of the multicast event channel
     * @param commandChannelAddress the address of the multicast command channel
     * @param discoveryChannelAddress the address of the multicast discovery channel
     */
    public MoldBusServer(
            Selector selector,
            Time time,
            Scheduler scheduler,
            LogFactory logFactory,
            MetricFactory metricFactory,
            ActivatorFactory activatorFactory,
            String busName,
            Schema<DispatcherT, ProviderT> schema,
            MessageStore messageStore,
            String eventChannelAddress,
            String commandChannelAddress,
            String discoveryChannelAddress) {
        super(schema);
        Objects.requireNonNull(selector, "selectService is null");
        this.time = Objects.requireNonNull(time, "time is null");
        Objects.requireNonNull(scheduler, "scheduler is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(metricFactory, "metricFactory is null");
        Objects.requireNonNull(activatorFactory, "activationManager is null");
        Objects.requireNonNull(busName, "busName is null");
        Objects.requireNonNull(messageStore, "eventStore is null");
        Objects.requireNonNull(eventChannelAddress, "eventChannelAddress is null");
        Objects.requireNonNull(commandChannelAddress, "commandChannelAddress is null");
        Objects.requireNonNull(discoveryChannelAddress, "discoveryChannelAddress is null");

        timestampOffset = getSchema().getTimestampOffset();

        session = new MoldSession(
                busName + ":MoldServerSession:" + eventChannelAddress, time, activatorFactory);

        eventPublisher = new MoldEventPublisher(
                busName + ":MoldEventPublisher:" + eventChannelAddress,
                selector,
                logFactory,
                activatorFactory,
                session,
                messageStore,
                eventChannelAddress);

        eventReceiver = new MoldBusServerEventReceiver(
                busName,
                busName + ":MoldServerEventReceiver:" + eventChannelAddress,
                selector,
                scheduler,
                logFactory,
                metricFactory,
                activatorFactory,
                session,
                eventChannelAddress,
                discoveryChannelAddress);

        commandReceiver = new MoldCommandReceiver(
                busName + ":MoldCommandReceiver:" + commandChannelAddress,
                selector,
                logFactory,
                activatorFactory,
                session,
                commandChannelAddress,
                true);

        var rewinder = new MoldRewinder(
                busName + ":MoldRewinder:" + discoveryChannelAddress,
                selector,
                logFactory,
                activatorFactory,
                session,
                messageStore,
                discoveryChannelAddress);

        activator = activatorFactory.createActivator(
                busName + ":MoldBusServer:" + eventChannelAddress,
                this,
                session, commandReceiver, rewinder, eventPublisher);
        activator.ready();
    }

    /**
     * Creates a {@code MoldBusServer} and the components required to operate a MoldUDP64 bus from the specified
     * parameters, including a from a {@code MoldBusClient} which shares the event stream.
     *
     * @param selector a selector for asynchronous I/O
     * @param time a real-time source of timestamp
     * @param scheduler a real-time scheduler of tasks
     * @param logFactory a factory to create logs
     * @param metricFactory a factory to create metrics
     * @param activatorFactory a factory to crate activators
     * @param busName the unique name for this bus in this VM
     * @param busClient the MOLD bus client
     * @param schema the message schema the sequencer uses
     * @param messageStore a store of messages
     * @param eventChannelAddress the address of the multicast event channel
     * @param commandChannelAddress the address of the multicast command channel
     * @param discoveryChannelAddress the address of the multicast discovery channel
     */
    public MoldBusServer(
            Selector selector,
            Time time,
            Scheduler scheduler,
            LogFactory logFactory,
            MetricFactory metricFactory,
            ActivatorFactory activatorFactory,
            String busName,
            MoldBusClient<DispatcherT, ProviderT> busClient,
            Schema<DispatcherT, ProviderT> schema,
            MessageStore messageStore,
            String eventChannelAddress,
            String commandChannelAddress,
            String discoveryChannelAddress) {
        super(schema);
        Objects.requireNonNull(selector, "selectService is null");
        this.time = Objects.requireNonNull(time, "time is null");
        Objects.requireNonNull(scheduler, "scheduler is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(metricFactory, "metricFactory is null");
        Objects.requireNonNull(activatorFactory, "activationManager is null");
        Objects.requireNonNull(busName, "busName is null");
        Objects.requireNonNull(busClient, "busClient is null");
        Objects.requireNonNull(messageStore, "eventStore is null");
        Objects.requireNonNull(eventChannelAddress, "eventChannelAddress is null");
        Objects.requireNonNull(commandChannelAddress, "commandChannelAddress is null");
        Objects.requireNonNull(discoveryChannelAddress, "discoveryChannelAddress is null");

        timestampOffset = getSchema().getTimestampOffset();

        session = new MoldSession(
                busName + ":MoldServerSession:" + eventChannelAddress, time, activatorFactory);
        busClient.getMoldSession().addOpenSessionListener(() -> session.setSessionSuffix(BufferUtils.fromAsciiString(
                busClient.getMoldSession().getSessionNameAsString().substring(MoldConstants.SESSION_LENGTH - 2))));

        eventPublisher = new MoldEventPublisher(
                busName + ":MoldEventPublisher:" + eventChannelAddress,
                selector,
                logFactory,
                activatorFactory,
                session,
                messageStore,
                eventChannelAddress);

        eventReceiver = busClient.getEventReceiver();

        commandReceiver = new MoldCommandReceiver(
                busName + ":MoldCommandReceiver:" + commandChannelAddress,
                selector,
                logFactory,
                activatorFactory,
                session,
                commandChannelAddress,
                true);

        var rewinder = new MoldRewinder(
                busName + ":MoldRewinder:" + discoveryChannelAddress,
                selector,
                logFactory,
                activatorFactory,
                session,
                messageStore,
                discoveryChannelAddress);

        activator = activatorFactory.createActivator(
                busName + ":MoldBusServer:" + eventChannelAddress,
                this,
                session, commandReceiver, rewinder, eventPublisher);
        activator.ready();
    }

    @Override
    public MutableDirectBuffer acquire() {
        messageBuffer = eventPublisher.acquire();
        return messageBuffer;
    }

    @Override
    public void commit(int msgLength) {
        if (activator.isActive()) {
            messageBuffer.putLong(timestampOffset, time.nanos());
        }
        eventPublisher.commit(msgLength);
    }

    @Override
    public void commit(int msgLength, long timestamp) {
        if (activator.isActive()) {
            messageBuffer.putLong(timestampOffset, timestamp);
        }
        eventPublisher.commit(msgLength);
    }

    @Override
    public boolean isActive() {
        return activator.isActive();
    }

    @Override
    public void send() {
        eventPublisher.send();
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
    public void setSessionSuffix(DirectBuffer sessionSuffix) {
        session.setSessionSuffix(sessionSuffix);
    }

    @Override
    public void activate() {
        activator.ready();
    }

    @Override
    public void deactivate() {
        activator.notReady("backup mode");
    }

    @Override
    public void addEventListener(Consumer<DirectBuffer> eventListener) {
        eventReceiver.addEventListener(eventListener);
    }

    @Override
    public void setCommandListener(Consumer<DirectBuffer> commandListener) {
        commandReceiver.setCommandListener(commandListener);
    }

    @Override
    public String toString() {
        return toEncodedString();
    }

    @Command(path = "status", readOnly = true)
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("session").object(session)
                .string("activator").object(activator)
                .string("eventReceiver").object(eventReceiver)
                .string("commandReceiver").object(commandReceiver)
                .string("eventPublisher").object(eventPublisher)
                .closeMap();
    }
}
