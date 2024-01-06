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
 * The {@code MoldBusClient} is an implementation of {@code BusClient} that sends and receives messages through the
 * MoldUDP64 protocol.
 * 
 * <p>The MoldUDP64 protocol is located on the
 * <a href="https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/moldudp64.pdf">Nasdaq
 * website</a>
 *
 * @param <DispatcherT> the dispatcher type
 * @param <ProviderT> the provider type
 */
public class MoldBusClient<DispatcherT extends Dispatcher, ProviderT extends Provider>
        implements BusClient<DispatcherT, ProviderT>, Encodable {

    private final Shell shell;
    private final Selector selector;
    private final Scheduler scheduler;
    private final LogFactory logFactory;
    private final MoldSession moldSession;
    @Directory
    private final MoldEventReceiver eventReceiver;
    @Property
    private final String commandChannelAddress;
    private final ActivatorFactory activatorFactory;
    private final Map<String, ProviderT> nameToCommandProviders;
    @Directory(path = ".")
    private final Activator activator;
    private final Schema<DispatcherT, ProviderT> schema;
    private final DispatcherT dispatcher;

    /**
     * Constructs a {@code MoldBusClient} with the specified parameters.
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
     * @param busName the unique name for this bus in this VM
     * @param schema the message schema
     * @param eventChannelAddress the event channel address
     * @param commandChannelAddress the command channel address
     * @param discoveryChannelAddress the discovery channel address
     */
    public MoldBusClient(
            Shell shell,
            Selector selector,
            Time time,
            Scheduler scheduler,
            LogFactory logFactory,
            MetricFactory metricFactory,
            ActivatorFactory activatorFactory,
            String busName,
            Schema<DispatcherT, ProviderT> schema,
            String eventChannelAddress,
            String commandChannelAddress,
            String discoveryChannelAddress) {
        this.shell = Objects.requireNonNull(shell, "shell is null");
        this.selector = Objects.requireNonNull(selector, "selectService is null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler is null");
        this.logFactory = Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(metricFactory, "metricFactory is null");
        this.activatorFactory = Objects.requireNonNull(activatorFactory, "activationManager is null");
        Objects.requireNonNull(busName, "busName is null");
        this.schema = Objects.requireNonNull(schema, "schema is null");
        this.commandChannelAddress = Objects.requireNonNull(commandChannelAddress, "commandChannelAddress is null");
        Objects.requireNonNull(discoveryChannelAddress, "discoveryChannelAddress is null");

        nameToCommandProviders = new CoreMap<>();

        moldSession = new MoldSession("MoldSession:" + eventChannelAddress, time, activatorFactory);
        dispatcher = schema.createDispatcher();

        eventReceiver = new MoldEventReceiver(
                busName,
                "MoldEventReceiver:" + eventChannelAddress,
                selector,
                scheduler,
                logFactory,
                metricFactory,
                activatorFactory,
                moldSession,
                eventChannelAddress,
                discoveryChannelAddress);
        eventReceiver.setEventListener(buffer -> dispatcher.dispatch(buffer, 0, buffer.capacity()));

        activator = activatorFactory.createActivator(
                "MoldBusClient:" + eventChannelAddress, this, eventReceiver);
        activator.ready();
    }

    MoldEventReceiver getEventReceiver() {
        return eventReceiver;
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
            var commandProvider = nameToCommandProviders.get(applicationName);

            if (commandProvider == null) {
                var messagePublisher = new MoldCommandPublisher(
                        shell,
                        selector,
                        scheduler,
                        logFactory,
                        activatorFactory,
                        this,
                        moldSession,
                        eventReceiver,
                        applicationName,
                        commandChannelAddress,
                        true,
                        associatedObject);
                shell.addObject(this, BufferUtils.fromAsciiString("publishers/" + applicationName), messagePublisher);
                commandProvider = schema.createProvider(messagePublisher);
                var providerActivator = activatorFactory.createActivator(
                        "Provider:" + applicationName, commandProvider, messagePublisher);
                providerActivator.ready();
                nameToCommandProviders.put(applicationName, commandProvider);
            }

            return commandProvider;
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
                .string("eventReceiver").object(eventReceiver)
                .string("commandProviders").number(nameToCommandProviders.size())
                .string("activator").object(activator)
                .closeMap();
    }

    @Override
    public String toString() {
        return toEncodedString();
    }
}
