package com.core.infrastructure.messages;

import org.agrona.MutableDirectBuffer;

/**
 * A publisher of messages to the core.
 *
 * <p>Messages are guaranteed to be sent sequentially from the publisher.
 * That is, the first message committed is sent before the second message committed which is sent before the third
 * message committed, etc.
 *
 * <p>The Sequencer will acknowledge all messages sent by the publisher.
 * An accepted message is repeated by the Sequencer as an event.
 * A rejected message is sent by the Sequencer as a sequencer reject event.
 * The message publisher will resend messages that have not acknowledged.
 *
 * <p>A batch mode is supported by some implementations.
 * For example, a MOLD implementation allows as many messages that fit in a multicast packet to be batched together.
 * <pre>
 * var message = messagePublisher.acquire();
 * // write message 1
 * messagePublisher.commit(message1Length);
 * message = messagePublisher.acquire();
 * // write message 2
 * messagePublisher.commit(message2Length);
 * // send message 1 and 2
 * messagePublisher.send();
 * </pre>
 */
public interface MessagePublisher {

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
     * @param commandLength the length of the message
     */
    void commit(int commandLength);

    /**
     * Sends all committed messages since the previous invocation of this method.
     */
    void send();

    /**
     * Returns the name of the application associated with this message publisher.
     *
     * @return the name of the application associated with this message publisher
     */
    String getApplicationName();

    /**
     * Returns the application identifier associated with this message publisher.
     *
     * @return the application identifier associated with this message publisher
     */
    short getApplicationId();

    /**
     * Returns true if the message publisher has received confirmations for all sent messages.
     *
     * @return true if the message publisher has received confirmations for all sent messages
     */
    boolean isCurrent();
}
