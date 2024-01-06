package com.core.platform.bus.inproc;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.collections.LinkedList;
import com.core.infrastructure.collections.ObjectPool;
import com.core.infrastructure.collections.Resettable;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Directory;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.messages.Dispatcher;
import com.core.infrastructure.messages.Encoder;
import com.core.infrastructure.messages.MessagePublisher;
import com.core.infrastructure.messages.Provider;
import com.core.infrastructure.messages.Schema;
import com.core.infrastructure.metrics.MetricFactory;
import com.core.infrastructure.time.Scheduler;
import com.core.infrastructure.time.Time;
import com.core.platform.activation.Activatable;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.bus.AbstractBusServer;
import com.core.platform.bus.BusClient;
import com.core.platform.bus.mold.MoldConstants;
import com.core.platform.shell.Shell;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Provides an in-process bus which dispatches messages encoded into pooled buffers.
 *
 * @param <DispatcherT> the dispatcher type
 * @param <ProviderT> the provider type
 */
public class InProcessBus<DispatcherT extends Dispatcher, ProviderT extends Provider> implements Runnable {

    private static final DirectBuffer VM_NAME = BufferUtils.fromAsciiString("vm_name");

    private final Selector selector;
    private final Scheduler scheduler;
    private final Time time;
    private final Shell shell;
    private final ActivatorFactory activatorFactory;
    private final Schema<DispatcherT, ProviderT> schema;
    private final DispatcherT dispatcher;
    private final ObjectPool<Message> messagePool;
    private final Log log;

    @Property(write = true)
    private boolean done;
    @Directory(path = "/bus")
    private final InProcessBusClient client;
    @Directory(path = "/busServer")
    private final InProcessBusServer server;

    private final Encoder appDefinitionEncoder;
    private final Encoder appDiscoveryEncoder;

    private String sessionString;

    /**
     * Construct a new in-process bus.
     *
     * @param shell the command shell
     * @param selector the selector
     * @param time the time
     * @param scheduler the scheduler
     * @param logFactory the log factory
     * @param metricFactory the metric factory
     * @param activatorFactory the activator factory
     * @param schema the schema
     */
    public InProcessBus(
            Shell shell,
            Selector selector,
            Time time,
            Scheduler scheduler,
            LogFactory logFactory,
            MetricFactory metricFactory,
            ActivatorFactory activatorFactory,
            Schema<DispatcherT, ProviderT> schema) {
        this.shell = Objects.requireNonNull(shell, "shell is null");
        this.selector = Objects.requireNonNull(selector, "selectService is null");
        this.time = Objects.requireNonNull(time, "time is null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(metricFactory, "metricFactory is null");
        this.activatorFactory = Objects.requireNonNull(activatorFactory, "activationManager is null");
        this.schema = Objects.requireNonNull(schema, "schema is null");

        log = logFactory.create(InProcessBus.class);
        dispatcher = schema.createDispatcher();
        appDefinitionEncoder = schema.createEncoder("applicationDefinition");
        appDiscoveryEncoder = schema.createEncoder("applicationDiscovery");

        messagePool = new ObjectPool<>(Message::new);
        client = new InProcessBusClient();
        server = new InProcessBusServer();
    }

    @Override
    public void run() {
        try {
            while (!done) {
                time.updateTime();
                server.dispatch();
                scheduler.fire();
                selector.selectNow();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private class InProcessMessagePublisher implements MessagePublisher, Activatable, Encodable {

        private final String applicationName;
        private final Object associatedObject;
        private final Activator activator;
        private final LinkedList<Message> queuedMessages;
        private Message currentMessage;
        private short applicationId;
        private int applicationSeqNum;
        private int confirmedApplicationSeqNum;
        private boolean open;
        private boolean sentFirstMessage;

        public InProcessMessagePublisher(String applicationName, Object associatedObject) {
            this.applicationName = applicationName;
            this.associatedObject = associatedObject;

            log.info().append("creating new message publisher: ").append(applicationName)
                    .append(", object=").append(associatedObject.getClass().getName())
                    .commit();

            queuedMessages = new LinkedList<>();
            commit(appDefinitionEncoder.wrap(acquire())
                    .set("name", BufferUtils.fromAsciiString(applicationName))
                    .length());

            activator = activatorFactory.createActivator(
                    "InProcessMessagePublisher:" + applicationName,
                    this, client);

            dispatcher.addListener("applicationDefinition", decoder -> {
                var appName = (DirectBuffer) decoder.get("name");
                if (applicationId == 0 && appName.equals(applicationName)) {
                    applicationId = decoder.getApplicationId();

                    log.info().append("application defined: name=").append(applicationName)
                            .append(", id=").append(applicationId)
                            .commit();

                    for (var message : queuedMessages) {
                        message.buffer.putShort(schema.getApplicationIdOffset(), applicationId);
                    }

                    activator.ready();
                    send();
                }
            });
        }

        @Override
        public MutableDirectBuffer acquire() {
            if (currentMessage == null) {
                currentMessage = messagePool.borrowObject();
                return currentMessage.buffer;
            } else {
                return currentMessage.buffer;
            }
        }

        @Override
        public void commit(int commandLength) {
            currentMessage.buffer.putShort(schema.getApplicationIdOffset(), applicationId);
            currentMessage.buffer.putInt(schema.getApplicationSequenceNumberOffset(), ++applicationSeqNum);
            currentMessage.wrapper.wrap(currentMessage.buffer, 0, commandLength);
            queuedMessages.addLast(currentMessage);
            currentMessage = null;
        }

        @Override
        public void send() {
            if (applicationId == 0) {
                if (open && !sentFirstMessage) {
                    log.info().append("sending app definition: ").append(applicationName).commit();

                    var message = queuedMessages.removeFirst();
                    server.inboundMessageQueue.addLast(message);
                    sentFirstMessage = true;
                }
            } else {
                while (open && !queuedMessages.isEmpty()) {
                    var message = queuedMessages.removeFirst();
                    server.inboundMessageQueue.addLast(message);
                }
            }
        }

        @Override
        public String getApplicationName() {
            return applicationName;
        }

        @Override
        public short getApplicationId() {
            return applicationId;
        }

        @Override
        public boolean isCurrent() {
            return confirmedApplicationSeqNum == applicationSeqNum;
        }

        @Override
        public void activate() {
            open = true;
            if (applicationId != 0) {
                activator.ready();
            }

            commit(appDiscoveryEncoder.wrap(acquire())
                    .set("vmName", shell.getPropertyValue(VM_NAME))
                    .set("commandPath", BufferUtils.temp(shell.getPath(associatedObject)))
                    .set("activationStatus", (byte) 1)
                    .length());
            send();
        }

        @Override
        public void deactivate() {
            commit(appDiscoveryEncoder.wrap(acquire())
                    .set("vmName", shell.getPropertyValue(VM_NAME))
                    .set("commandPath", BufferUtils.temp(shell.getPath(associatedObject)))
                    .set("activationStatus", (byte) 2)
                    .length());
            send();

            activator.notReady();
            open = false;
        }

        @Command(path = "status", readOnly = true)
        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.openMap()
                    .string("session").object(sessionString)
                    .string("application").string(applicationName)
                    .string("applicationId").number(applicationId)
                    .string("associatedObject").object(associatedObject.getClass().getName())
                    .string("open").bool(open)
                    .string("sentFirstMessage").bool(sentFirstMessage)
                    .string("activator").object(activator)
                    .string("queuedMessages").number(queuedMessages.size())
                    .closeMap();
        }
    }

    private class InProcessBusClient implements BusClient<DispatcherT, ProviderT>, Encodable {

        private final Map<DirectBuffer, ProviderT> appNameToProvider;
        @Directory(path = ".")
        private final Activator activator;
        private InProcessMessagePublisher[] publishers;
        private final List<Runnable> openListeners;

        @SuppressWarnings("unchecked")
        InProcessBusClient() {
            appNameToProvider = new UnifiedMap<>();
            publishers = (InProcessMessagePublisher[]) Array.newInstance(InProcessMessagePublisher.class, 0);
            openListeners = new CoreList<>();

            dispatcher.addListener("applicationDefinition", decoder -> {
                var appId = decoder.getApplicationId();
                publishers = Arrays.copyOf(publishers, Math.max(appId + 2, publishers.length));

                var provider = appNameToProvider.get((DirectBuffer) decoder.get("name"));
                if (provider != null) {
                    var publisher = (InProcessMessagePublisher) provider.getMessagePublisher();
                    publisher.confirmedApplicationSeqNum = 1;
                    publishers[appId] = publisher;
                }
            });
            dispatcher.addListenerBeforeDispatch(decoder -> {
                var appId = decoder.getApplicationId();
                if (appId < publishers.length) {
                    var publisher = publishers[appId];
                    if (publisher != null) {
                        publisher.confirmedApplicationSeqNum = decoder.getApplicationSequenceNumber();
                    }
                }
            });

            activator = activatorFactory.createActivator("InProcessBusClient", this);
            activator.ready();
        }

        @Override
        public Schema<DispatcherT, ProviderT> getSchema() {
            return schema;
        }

        @Override
        public DispatcherT getDispatcher() {
            return dispatcher;
        }

        @Override
        public ProviderT getProvider(String applicationName, Object associatedObject) {
            var provider = appNameToProvider.get(BufferUtils.temp(applicationName));
            if (provider == null) {
                var messagePublisher = new InProcessMessagePublisher(applicationName, associatedObject);
                provider = schema.createProvider(messagePublisher);
                appNameToProvider.put(BufferUtils.fromAsciiString(applicationName), provider);

                var activator = activatorFactory.createActivator(
                        "InProcessMessageProvider:" + applicationName, provider, messagePublisher);
                activator.ready();
            }
            return provider;
        }

        @Override
        public String getSession() {
            return sessionString;
        }

        @Override
        public void addOpenSessionListener(Runnable listener) {
            openListeners.add(listener);
        }

        @Override
        public void addCloseSessionListener(Runnable listener) {
            // we don't close!
        }

        @Command(path = "status", readOnly = true)
        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.openMap()
                    .string("session").object(sessionString)
                    .string("activator").object(activator)
                    .string("providers").object(appNameToProvider.keySet())
                    .string("openListeners").number(openListeners.size())
                    .closeMap();
        }
    }

    private class InProcessBusServer extends AbstractBusServer<DispatcherT, ProviderT> implements Encodable {

        private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

        @Directory(path = ".")
        private final Activator activator;
        private final LinkedList<Message> outboundMessageQueue;
        private final LinkedList<Message> inboundMessageQueue;

        private final int timestampOffset;
        private Message currentMessage;
        private Consumer<DirectBuffer> commandListener;

        InProcessBusServer() {
            super(schema);

            outboundMessageQueue = new LinkedList<>();
            inboundMessageQueue = new LinkedList<>();

            timestampOffset = getSchema().getTimestampOffset();

            activator = activatorFactory.createActivator("InProcessBusServer", this);
            activator.ready();
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
            Objects.requireNonNull(sessionSuffix);
            if (sessionSuffix.capacity() != 2) {
                throw new IllegalArgumentException("sessionSuffix must be length of 2");
            }
            var instant = Instant.ofEpochSecond(time.nanos() / TimeUnit.SECONDS.toNanos(1));
            var localDate = LocalDate.ofInstant(instant, ZoneOffset.UTC);
            var localTime = LocalTime.ofInstant(instant, ZoneOffset.UTC);
            if (localTime.getHour() >= 22) {
                localDate = localDate.plusDays(1);
            }
            sessionString = DATE_FORMATTER.format(localDate) + BufferUtils.toAsciiString(sessionSuffix);

            shell.setPropertyValue("runner", shell.getPath(InProcessBus.this));

            for (var openListener : client.openListeners) {
                openListener.run();
            }
        }

        @Override
        public MutableDirectBuffer acquire() {
            if (currentMessage == null) {
                currentMessage = messagePool.borrowObject();
                return currentMessage.buffer;
            } else {
                return currentMessage.buffer;
            }
        }

        @Override
        public void commit(int msgLength) {
            commit(msgLength, time.nanos());
        }

        @Override
        public void commit(int msgLength, long timestamp) {
            currentMessage.buffer.putLong(timestampOffset, timestamp);
            currentMessage.wrapper.wrap(currentMessage.buffer, 0, msgLength);
            outboundMessageQueue.addLast(currentMessage);
            currentMessage = null;
        }

        @Override
        public boolean isActive() {
            return activator.isActive();
        }

        @Override
        public void send() {
            // not used as outbound packets are added on commit
        }

        @Override
        public void setEventListener(Consumer<DirectBuffer> eventListener) {
            // no backup sequencer
        }

        @Override
        public void setCommandListener(Consumer<DirectBuffer> commandListener) {
            this.commandListener = commandListener;
        }

        void dispatch() {
            while (!inboundMessageQueue.isEmpty()) {
                var packet = inboundMessageQueue.removeFirst();
                commandListener.accept(packet.wrapper);
                messagePool.returnObject(packet);
            }

            while (!outboundMessageQueue.isEmpty()) {
                var packet = outboundMessageQueue.removeFirst();
                dispatcher.dispatch(packet.buffer, 0, packet.wrapper.capacity());
                messagePool.returnObject(packet);
            }
        }

        @Command(path = "status", readOnly = true)
        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.openMap()
                    .string("session").object(sessionString)
                    .string("activator").object(activator)
                    .string("inboundQueuedMessages").number(inboundMessageQueue.size())
                    .string("outboundQueuedMessages").number(outboundMessageQueue.size())
                    .closeMap();
        }
    }

    private static class Message implements Resettable {

        MutableDirectBuffer buffer;
        DirectBuffer wrapper;

        Message() {
            buffer = BufferUtils.allocate(MoldConstants.MTU_SIZE);
            wrapper = BufferUtils.emptyBuffer();
        }

        @Override
        public void reset() {
            wrapper.wrap(0, 0);
        }
    }
}
