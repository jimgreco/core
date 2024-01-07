package com.core.platform.bus;

import com.core.infrastructure.messages.Dispatcher;
import com.core.infrastructure.messages.Encoder;
import com.core.infrastructure.messages.Provider;
import com.core.infrastructure.messages.Schema;
import com.core.infrastructure.time.Time;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.function.Consumer;

public class TestBusServer<DispatcherT extends Dispatcher, ProviderT extends Provider>
        extends AbstractBusServer<DispatcherT, ProviderT>
        implements BusServer<DispatcherT, ProviderT> {

    private final TestMessagePublisher eventPublisher;
    private final Activator activator;
    private final Time time;
    private Consumer<DirectBuffer> eventListener;
    private Consumer<DirectBuffer> commandListener;
    private MutableDirectBuffer messageBuffer;

    public TestBusServer(
            Time time, Schema<DispatcherT, ProviderT> schema, ActivatorFactory activatorFactory) {
        super(schema);
        this.time = time;

        eventPublisher = new TestMessagePublisher(activatorFactory, schema, 1, false);

        activator = activatorFactory.createActivator("TestBusServer", this, eventPublisher);
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
            messageBuffer.putLong(getSchema().getTimestampOffset(), time.nanos());
        }
        eventPublisher.commit(msgLength);
    }

    @Override
    public void commit(int msgLength, long timestamp) {
        if (activator.isActive()) {
            messageBuffer.putLong(getSchema().getTimestampOffset(), timestamp);
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

    @Override
    public void addEventListener(Consumer<DirectBuffer> eventListener) {
        this.eventListener = eventListener;
    }

    @Override
    public void setCommandListener(Consumer<DirectBuffer> commandListener) {
        this.commandListener = commandListener;
    }

    public TestMessagePublisher getEventPublisher() {
        return eventPublisher;
    }

    public void publishEvent(DirectBuffer event) {
        if (!activator.isActive()) {
            eventListener.accept(event);
        }
    }

    public void publishEvent(Encoder decoder) {
        publishEvent(decoder.toDecoder().buffer());
    }

    public void publishCommand(Encoder encoder) {
        publishCommand(encoder.toDecoder().buffer());
    }

    public void publishCommand(DirectBuffer command) {
        if (activator.isActive()) {
            commandListener.accept(command);
        }
    }
}
