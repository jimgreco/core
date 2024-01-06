package com.core.platform.bus;

import com.core.infrastructure.messages.Dispatcher;
import com.core.infrastructure.messages.MessagePublisher;
import com.core.infrastructure.messages.Provider;
import com.core.infrastructure.messages.Schema;

/**
 * The bus client is used by applications to send and receive messages from the core.
 * Messages can be dispatched with message-specific decoders through the {@link #getDispatcher()}.
 * Messages are sent through {@link MessagePublisher}s, each of which represents a unique contributor to the core.
 *
 * @param <DispatcherT> the message dispatcher type
 */
public interface BusClient<DispatcherT extends Dispatcher, ProviderT extends Provider> {

    /**
     * Returns the message schema used by this bus.
     *
     * @return the message schema used by this bus
     */
    Schema<DispatcherT, ProviderT> getSchema();

    /**
     * Returns a dispatcher of messages received in sequence by the bus.
     *
     * @return a dispatcher of messages received in sequence by the bus
     */
    DispatcherT getDispatcher();

    /**
     * Returns the message provider associated with the specified application name.
     * If a message provider is not currently associated with the application name, then a new one will be created.
     *
     * @param applicationName the application name
     * @param associatedObject the object associated with this provider
     * @return the message publisher
     */
    ProviderT getProvider(String applicationName, Object associatedObject);

    /**
     * Returns the name of the session the bus client is connected to.
     *
     * @return the name of the session the bus client is connected to
     */
    String getSession();

    /**
     * Adds a listener to be invoked when the session is opened and messages will be dispatched.
     *
     * @param listener a listener to be invoked when the session is opened and messages will be dispatched
     */
    void addOpenSessionListener(Runnable listener);

    /**
     * Adds a listener to be invoked when the session is closed and no more messages will be dispatched.
     *
     * @param listener a listener to be invoked when the session is closed and no more messages will be dispatched
     */
    void addCloseSessionListener(Runnable listener);
}
