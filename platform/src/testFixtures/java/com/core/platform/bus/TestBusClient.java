package com.core.platform.bus;

import com.core.infrastructure.collections.CoreMap;
import com.core.infrastructure.messages.Dispatcher;
import com.core.infrastructure.messages.Encoder;
import com.core.infrastructure.messages.Provider;
import com.core.infrastructure.messages.Schema;
import com.core.platform.activation.ActivatorFactory;

import java.util.Map;

public class TestBusClient<DispatcherT extends Dispatcher, ProviderT extends Provider>
        implements BusClient<DispatcherT, ProviderT> {

    private final Map<String, TestMessagePublisher> applicationToMessagePublisher;
    private final Map<String, ProviderT> nameToCommandProviders;
    private final Schema<DispatcherT, ProviderT> schema;
    private final DispatcherT dispatcher;
    private final ActivatorFactory activatorFactory;

    public TestBusClient(Schema<DispatcherT, ProviderT> schema, ActivatorFactory activatorFactory) {
        this.schema = schema;
        this.activatorFactory = activatorFactory;
        dispatcher = schema.createDispatcher();
        applicationToMessagePublisher = new CoreMap<>();
        nameToCommandProviders = new CoreMap<>();

        var activator = activatorFactory.createActivator("TestBusClient", this);
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

    public TestMessagePublisher getMessagePublisher(String applicationName) {
        var busPublisher = applicationToMessagePublisher.get(applicationName);
        if (busPublisher == null) {
            // let the sequencer start at 1 for these test client/server
            busPublisher = new TestMessagePublisher(
                    activatorFactory, schema, applicationToMessagePublisher.size() + 2, true);
            applicationToMessagePublisher.put(applicationName, busPublisher);
        }
        return busPublisher;
    }

    @Override
    public ProviderT getProvider(String applicationName, Object associatedObject) {
        var commandProvider = nameToCommandProviders.get(applicationName);

        if (commandProvider == null) {
            var messagePublisher = getMessagePublisher(applicationName);
            commandProvider = schema.createProvider(messagePublisher);
            var providerActivator = activatorFactory.createActivator(
                    "Provider:" + applicationName, commandProvider, messagePublisher);
            providerActivator.ready();
            nameToCommandProviders.put(applicationName, commandProvider);
        }

        return commandProvider;
    }

    @Override
    public String getSession() {
        return "TEST";
    }

    @Override
    public void addOpenSessionListener(Runnable listener) {
        listener.run();
    }

    @Override
    public void addCloseSessionListener(Runnable listener) {
        // do nothing
    }

    public void dispatch(Encoder encoder) {
        dispatcher.dispatch(encoder.buffer(), encoder.offset(), encoder.length());
    }
}
