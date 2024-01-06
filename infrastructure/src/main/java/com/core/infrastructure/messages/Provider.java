package com.core.infrastructure.messages;

/**
 * A provider provides access to message encoders.
 */
public interface Provider {

    /**
     * Sends queued messages.
     */
    void send();

    /**
     * Returns the encoder corresponding to the specified message name.
     *
     * @param messageName the name of the message
     * @return the encoder
     */
    Encoder getEncoder(String messageName);

    /**
     * Returns the message publisher wrapped by the provider.
     *
     * @return the message publisher wrapped by the provider
     */
    MessagePublisher getMessagePublisher();
}
