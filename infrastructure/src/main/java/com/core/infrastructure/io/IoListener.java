package com.core.infrastructure.io;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * A listener for input and output events from a socket or higher-level protocol.
 */
public interface IoListener {

    boolean isDebug();

    /**
     * Borrows a temporary buffer to use for writing a message.
     *
     * @return a temporary buffer to use for writing a message
     */
    MutableDirectBuffer borrowBuffer();

    /**
     * Invoked a write event.
     *
     * @param text the text
     */
    void onWriteEvent(String text);

    default void onWriteEvent(DirectBuffer buffer) {
        onWriteEvent(buffer, 0, buffer.capacity());
    }

    /**
     * Invoked on a write event.
     *
     * @param buffer the buffer
     * @param offset the first byte of the buffer to write
     * @param length the length of the buffer to write
     */
    void onWriteEvent(DirectBuffer buffer, int offset, int length);

    /**
     * Invoked on a read event.
     *
     * @param text the text
     */
    void onReadEvent(String text);

    default void onReadEvent(DirectBuffer buffer) {
        onReadEvent(buffer, 0, buffer.capacity());
    }

    /**
     * Invoked on a read event.
     *
     * @param buffer the buffer
     * @param offset the first byte of the buffer to write
     * @param length the length of the buffer to write
     */
    void onReadEvent(DirectBuffer buffer, int offset, int length);

    /**
     * Invoked on an event.
     *
     * @param event the event
     */
    void onConnectionEvent(String event);

    default void onConnectionEvent(DirectBuffer buffer) {
        onConnectionEvent(buffer, 0, buffer.capacity());
    }

    /**
     * Invoked on an event.
     *
     * @param buffer the buffer
     * @param offset the first byte of the buffer to write
     * @param length the length of the buffer to write
     */
    void onConnectionEvent(DirectBuffer buffer, int offset, int length);

    /**
     * Invoked when the connection fails.
     *
     * @param reason the reason of the failure
     * @param exception an exception thrown
     */
    void onConnectionFailed(String reason, Exception exception);
}
