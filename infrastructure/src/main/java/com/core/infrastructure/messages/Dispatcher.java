package com.core.infrastructure.messages;

import org.agrona.DirectBuffer;

import java.util.function.Consumer;

/**
 * A dispatcher dispatches encoded messages to listeners who subscribe by message type.
 */
public interface Dispatcher {

    /**
     * Returns the timestamp, in nanoseconds since Unix epoch, of the last message dispatched.
     *
     * @return the timestamp, in nanoseconds since Unix epoch, of the last message dispatched
     */
    long getTimestamp();

    /**
     * Decodes the message type from {@code buffer}, wraps with the decoder associated with the message type, and
     * dispatches the decoder to any listeners associated with the message type.
     *
     * @param buffer the buffer to decode the message from
     * @param offset the first byte of the buffer to decode the message from
     * @param length the number of bytes in the message
     */
    void dispatch(DirectBuffer buffer, int offset, int length);

    /**
     * Adds a listener to all messages after the message is dispatched to message-specific listeners.
     *
     * @param listener the listener
     * @return this
     */
    Dispatcher addListenerAfterDispatch(Consumer<Decoder> listener);

    /**
     * Adds a listener to all messages before the message is dispatched to message-specific listeners.
     *
     * @param listener the listener
     * @return this
     */
    Dispatcher addListenerBeforeDispatch(Consumer<Decoder> listener);

    /**
     * Adds a {@code listener} with the specified message name to the dispatcher.
     *
     * @param messageName the name of the message
     * @param listener the listener
     * @param <T> the type of message to add
     * @return this
     */
    <T extends Decoder> Dispatcher addListener(String messageName, Consumer<T> listener);

    /**
     * Returns a decoder for the encoded message, or null if the buffer does not contain a message.
     *
     * @param buffer the message buffer
     * @param offset the first byte of the message
     * @param length the length of the message
     * @return a decoder for the encoded message
     */
    Decoder getDecoder(DirectBuffer buffer, int offset, int length);
}
