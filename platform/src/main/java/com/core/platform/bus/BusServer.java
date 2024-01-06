package com.core.platform.bus;

import com.core.infrastructure.messages.Decoder;
import com.core.infrastructure.messages.Dispatcher;
import com.core.infrastructure.messages.Encoder;
import com.core.infrastructure.messages.Provider;
import com.core.infrastructure.messages.Schema;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.function.Consumer;

/**
 * The bus server is used by components of the Sequencer application to receive commands and events
 * and publish events.
 *
 * @param <DispatcherT> the message dispatcher type
 */
public interface BusServer<DispatcherT extends Dispatcher, ProviderT extends Provider> {

    /**
     * Acquires a buffer to encode a message.
     * The message cannot be sent until {@code commit()} is invoked.
     *
     * @return a buffer
     */
    MutableDirectBuffer acquire();

    /**
     * Finalizes the message, encoded in the acquired buffer.
     * This method will set the contributor identifier and contributor sequence number in the message.
     * The message is not immediately sent out until {@code send()} is invoked.
     *
     * <p>If active, the current timestamp will be encoded into the message at the offset specified in the message
     * schema.
     *
     * @param msgLength the length of the message
     */
    void commit(int msgLength);

    /**
     * Finalizes the message, encoded in the acquired buffer.
     * This method will set the contributor identifier and contributor sequence number in the message.
     * The message is not immediately sent out until {@code send()} is invoked.
     *
     * <p>If active, the specified {@code timestamp} will be encoded into the message at the offset specified in the
     * message schema.
     *
     * @param msgLength the length of the message
     * @param timestamp the timestamp
     */
    void commit(int msgLength, long timestamp);

    /**
     * Finalizes the message, encoded with the specified {@code encoder} that wrapped a buffer acquired through the
     * {@code acquire()} method.
     *
     * @param busServer the bus server
     * @param encoder the encoder
     * @implSpec equivalent to invoking {@code busServer.commit(encoder.length())}
     */
    static void commit(BusServer<?, ?> busServer, Encoder encoder) {
        busServer.commit(encoder.length());
    }

    /**
     * Finalizes the message, encoded with the specified {@code encoder} that wrapped a buffer acquired through the
     * {@code acquire()} method.
     *
     * @param busServer the bus server
     * @param encoder the encoder
     * @param timestamp the timestamp
     * @implSpec equivalent to invoking {@code busServer.commit(encoder.length(), timestamp)}
     */
    static void commit(BusServer<?, ?> busServer, Encoder encoder, long timestamp) {
        busServer.commit(encoder.length(), timestamp);
    }

    /**
     * Returns whether the bus server is active.
     *
     * @return whether the bus server is active
     */
    boolean isActive();

    /**
     * Sends all committed messages since the previous invocation of this method.
     */
    void send();

    /**
     * Returns the application identifier associated with the sequencer.
     *
     * @return the application identifier associated with the sequencer
     */
    short getApplicationId();

    /**
     * Sets the specified application sequence number for the specified application identifier.
     *
     * @param applicationId the application identifier
     * @param applicationSequenceNumber the application sequence number
     */
    void setApplicationSequenceNumber(int applicationId, int applicationSequenceNumber);

    /**
     * Increments and returns the application sequence number for the specified application identifier.
     * Returns 0 for an unknown application identifier.
     *
     * @param applicationId the application identifier
     * @return the application sequence number
     */
    int incrementAndGetApplicationSequenceNumber(int applicationId);

    /**
     * Returns the application sequence number for the specified application identifier.
     * Returns 0 for an unknown application identifier.
     *
     * @param applicationId the application identifier
     * @return the application sequence number
     */
    int getApplicationSequenceNumber(int applicationId);

    /**
     * Copies the message wrapped by the decoder to the output.
     *
     * @param busServer the bus server
     * @param decoder the decoder
     * @implSpec equivalent to acquiring a buffer from this object, copying the buffer from the decoder, and committing
     *     the number of bytes copied.
     */
    static void copy(BusServer<?, ?> busServer, Decoder decoder) {
        var buffer = busServer.acquire();
        buffer.putBytes(0, decoder.buffer(), decoder.offset(), decoder.length());
        busServer.commit(decoder.length());
    }

    /**
     * Copies the message wrapped by the decoder to the output with the specified {@code timestamp}.
     *
     * @param busServer the bus server
     * @param decoder the decoder
     * @param timestamp the timestamp
     * @implSpec equivalent to acquiring a buffer from this object, copying the buffer from the decoder, and committing
     *     the number of bytes copied.
     */
    static void copy(BusServer<?, ?> busServer, Decoder decoder, long timestamp) {
        var buffer = busServer.acquire();
        buffer.putBytes(0, decoder.buffer(), decoder.offset(), decoder.length());
        busServer.commit(decoder.length(), timestamp);
    }

    /**
     * Sets the listener for messages received in sequence on the event channel.
     *
     * @param eventListener the listener for events
     */
    void setEventListener(Consumer<DirectBuffer> eventListener);

    /**
     * Sets the listener for messages received on the command channel.
     *
     * @param commandListener the listener for commands
     */
    void setCommandListener(Consumer<DirectBuffer> commandListener);

    /**
     * Returns the message schema used by this bus.
     *
     * @return the message schema used by this bus
     */
    Schema<DispatcherT, ProviderT> getSchema();

    /**
     * Returns a dispatcher of all messages received by the bus server.
     *
     * @return a dispatcher of all messages received by the bus server
     */
    DispatcherT getDispatcher();
}
