package com.core.infrastructure.messages;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * An encoder of core messages.
 */
public interface Encoder {

    /**
     * Wraps the buffer in which the message is to be encoded.
     *
     * @param buffer the buffer to encode the message to
     * @return this
     */
    Encoder wrap(MutableDirectBuffer buffer);

    /**
     * Wraps the buffer in which the message is to be encoded.
     *
     * @param buffer the buffer to encode the message to
     * @param offset the first byte of the buffer to encode the message to
     * @param length the number of bytes in the message
     * @return this
     */
    Encoder wrap(MutableDirectBuffer buffer, int offset, int length);

    /**
     * Copies the bytes from the {@code src} buffer into the {@code dst} buffer and wraps the {@code dst} buffer for
     * additional encoding of the message.
     *
     * @param decoder the decoder to copy from
     * @param dst the destination buffer
     * @return this
     */
    Encoder copy(Decoder decoder, MutableDirectBuffer dst);

    /**
     * Copies the bytes from the {@code src} buffer into the {@code dst} buffer and wraps the {@code dst} buffer for
     * additional encoding of the message.
     *
     * @param decoder the decoder to copy from
     * @param dst the destination buffer
     * @param offset the first byte of the buffer to encode the message to
     * @param length the number of bytes in the message
     * @return this
     */
    Encoder copy(Decoder decoder, MutableDirectBuffer dst, int offset, int length);

    /**
     * Returns the version of the schema used to generate this encoder.
     *
     * @return the version of the schema used to generate this encoder
     */
    int version();

    /**
     * Returns the fields in this message.
     * The implementation may cache the returned array.
     *
     * @return the fields in this message
     */
    Field[] fields();

    /**
     * Returns the field corresponding to the specified field name.
     *
     * @param name the field name
     * @return the field
     */
    Field field(String name);

    /**
     * Returns the name of the message.
     *
     * @return the name of the message
     */
    String messageName();

    /**
     * Returns the message type value.
     *
     * @return the message type value
     */
    byte messageType();

    /**
     * Returns the name of the entity associated with the message.
     *
     * @return the name of the entity associated with the message
     */
    String entityName();

    /**
     * Returns the name of the base entity associated with the message, or the {@code entityName} value if the encoder
     * does not have a base entity.
     *
     * @return the name of the base entity associated with the message
     */
    String baseEntityName();

    /**
     * Returns the underlying buffer.
     *
     * @return the underlying buffer
     */
    DirectBuffer buffer();

    /**
     * Returns the first byte of the underlying buffer that contains the message.
     *
     * @return the first byte of the underlying buffer that contains the message
     */
    int offset();

    /**
     * Returns the length of the message.
     *
     * @return the length of the message
     */
    int length();

    /**
     * Finalizes the message and returns the message publisher used to encode the message.
     *
     * @return the message publisher used to encode the message
     */
    MessagePublisher commit();

    /**
     * Sets the identifier of the topic of the message.
     *
     * @param value the identifier of the topic of the message
     * @return this
     */
    Encoder setApplicationId(short value);

    /**
     * Sets the sequence number of the topic.
     *
     * @param value the sequence number of the topic.
     * @return this
     */
    Encoder setApplicationSequenceNumber(int value);

    /**
     * Sets the time the sequencer published this message.
     *
     * @param value the time the sequencer published this message
     * @return this
     */
    Encoder setTimestamp(long value);

    /**
     * Sets the byte offset from the start of the message to the start of the optional fields in the message.
     *
     * @param value the byte offset from the start of the message to the start of the optional fields in the message
     * @return this
     */
    Encoder setOptionalFieldsIndex(short value);

    /**
     * Sets the schema version used to publish this message.
     *
     * @param value the schema version used to publish this message
     * @return this
     */
    Encoder setSchemaVersion(byte value);

    /**
     * Sets the type of this message.
     *
     * @param value the type of this message
     * @return this
     */
    Encoder setMessageType(byte value);

    /**
     * Returns a decoder for the currently encoded message.
     *
     * @return the decoder
     */
    Decoder toDecoder();

    /**
     * Sets the field associated with the specified {@code name} to the specified {@code value}.
     *
     * @param name the name of the field
     * @param value the value of the field
     * @return this
     * @throws IllegalArgumentException if a field with the specified {@code name} is not associated with this message
     */
    Encoder set(String name, Object value);

    /**
     * Sets the byte field associated with the specified {@code name} to the specified {@code value}.
     *
     * @param name the name of the field
     * @param value the value of the field
     * @return this
     * @throws IllegalArgumentException if a byte field with the specified {@code name} is not associated with this
     *     message
     */
    Encoder set(String name, byte value);

    /**
     * Sets the short field associated with the specified {@code name} to the specified {@code value}.
     *
     * @param name the name of the field
     * @param value the value of the field
     * @return this
     * @throws IllegalArgumentException if a short field with the specified {@code name} is not associated with this
     *     message
     */
    Encoder set(String name, short value);

    /**
     * Sets the int field associated with the specified {@code name} to the specified {@code value}.
     *
     * @param name the name of the field
     * @param value the value of the field
     * @return this
     * @throws IllegalArgumentException if an int field with the specified {@code name} is not associated with this
     *     message
     */
    Encoder set(String name, int value);

    /**
     * Sets the long field associated with the specified {@code name} to the specified {@code value}.
     *
     * @param name the name of the field
     * @param value the value of the field
     * @return this
     * @throws IllegalArgumentException if a long field with the specified {@code name} is not associated with this
     *     message
     */
    Encoder set(String name, long value);
}
