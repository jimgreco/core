package com.core.platform.bus.mold;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreMap;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Directory;
import com.core.infrastructure.command.Property;
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
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.bus.BusClient;
import com.core.platform.shell.CommandException;
import com.core.platform.shell.Shell;

import java.util.Map;
import java.util.Objects;

/**
 * The {@code TcpBusClient} is an implementation of {@code BusClient} that sends messages over UDP and receives messages
 * over TCP.
 *
 * @param <DispatcherT> the dispatcher type
 * @param <ProviderT> the provider type
 */
public class TcpBusClient<DispatcherT extends Dispatcher, ProviderT extends Provider>
        implements BusClient<DispatcherT, ProviderT>, Encodable {

    private final Shell shell;
    private final Selector selector;
    private final Scheduler scheduler;
    private final LogFactory logFactory;
    private final MoldSession moldSession;
    @Directory
    private final TcpMessageReceiver messageReceiver;
    private final ActivatorFactory activatorFactory;
    private final Map<String, ProviderT> nameToMessageProviders;
    @Directory(path = ".")
    private final Activator activator;
    private final Schema<DispatcherT, ProviderT> schema;
    private final DispatcherT dispatcher;

    @Property
    private String messageSendAddress;

    /**
     * Constructs a {@code TcpBusClient} with the specified parameters.
     * The schema will be used to create a message dispatcher.
     * All incoming messages will then be dispatched through this dispatcher.
     *
     * @param shell the command shell
     * @param selector a selector to create UDP sockets for communicating over MOLD
     * @param time the system time source
     * @param scheduler the system time scheduler
     * @param logFactory a factory to create logs
     * @param metricFactory a factory to create metrics
     * @param activatorFactory a factory to create activators
     * @param schema the message schema
     * @param messageReceiveAddress the address to receive messages
     * @param messageSendAddress the address channel address
     */
    public TcpBusClient(
            Shell shell,
            Selector selector,
            Time time,
            Scheduler scheduler,
            LogFactory logFactory,
            MetricFactory metricFactory,
            ActivatorFactory activatorFactory,
            Schema<DispatcherT, ProviderT> schema,
            String messageReceiveAddress,
            String messageSendAddress) {
        this.shell = Objects.requireNonNull(shell, "shell is null");
        this.selector = Objects.requireNonNull(selector, "selectService is null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler is null");
        this.logFactory = Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(metricFactory, "metricFactory is null");
        this.activatorFactory = Objects.requireNonNull(activatorFactory, "activationManager is null");
        this.schema = Objects.requireNonNull(schema, "schema is null");
        Objects.requireNonNull(messageReceiveAddress, "messageReceiveAddress is null");
        this.messageSendAddress = Objects.requireNonNull(messageSendAddress, "messageSendAddress is null");

        nameToMessageProviders = new CoreMap<>();

        moldSession = new MoldSession("MoldSession:" + messageReceiveAddress, time, activatorFactory);
        dispatcher = schema.createDispatcher();

        messageReceiver = new TcpMessageReceiver(
                selector,
                time,
                scheduler,
                logFactory,
                metricFactory,
                activatorFactory,
                moldSession,
                "TcpMessageReceiver:" + messageReceiveAddress,
                messageReceiveAddress);
        messageReceiver.setMessageListener(buffer -> dispatcher.dispatch(buffer, 0, buffer.capacity()));

        activator = activatorFactory.createActivator(
                "TcpBusClient:" + messageReceiveAddress, this, messageReceiver);
        activator.ready();
    }

    /**
     * Sets the address to send messages to.
     *
     * @param messageSendAddress the address
     */
    @Command(path = "setMessageSendAddress")
    public void setMessageSendAddress(String messageSendAddress) {
        this.messageSendAddress = Objects.requireNonNull(messageSendAddress, "messageSendAddress is null");
        for (var value : nameToMessageProviders.values()) {
            var messagePublisher = (MoldCommandPublisher) value.getMessagePublisher();
            messagePublisher.setCommandChannelAddress(messageSendAddress);
        }
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
        try {
            var messageProvider = nameToMessageProviders.get(applicationName);

            if (messageProvider == null) {
                var messagePublisher = new MoldCommandPublisher(
                        shell,
                        selector,
                        scheduler,
                        logFactory,
                        activatorFactory,
                        this,
                        moldSession,
                        messageReceiver,
                        applicationName,
                        messageSendAddress,
                        false,
                        associatedObject);
                shell.addObject(this, BufferUtils.fromAsciiString("publishers/" + applicationName), messagePublisher);
                messageProvider = schema.createProvider(messagePublisher);
                var providerActivator = activatorFactory.createActivator(
                        "Provider:" + applicationName, messageProvider, messagePublisher);
                providerActivator.ready();
                nameToMessageProviders.put(applicationName, messageProvider);
            }

            return messageProvider;
        } catch (CommandException e) {
            throw new IllegalArgumentException("application is already registered: " + applicationName);
        }
    }

    @Override
    public String getSession() {
        return moldSession.getSessionNameAsString();
    }

    @Override
    public void addOpenSessionListener(Runnable listener) {
        moldSession.addOpenSessionListener(listener);
    }

    @Override
    public void addCloseSessionListener(Runnable listener) {
        // do nothing yet...
    }

    @Command(path = "status")
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("messageReceiver").object(messageReceiver)
                .string("messageProviders").number(nameToMessageProviders.size())
                .string("activator").object(activator)
                .closeMap();
    }

    @Override
    public String toString() {
        return toEncodedString();
    }
}
