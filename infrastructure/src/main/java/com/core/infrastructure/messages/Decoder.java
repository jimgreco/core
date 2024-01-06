package com.core.infrastructure.messages;

import org.agrona.DirectBuffer;

/**
 * A decoder of core messages.
 */
public interface Decoder {

    /**
     * Wraps the buffer from which the message is to be decoded.
     *
     * @param buffer the buffer to decode the message from
     * @return this
     */
    Decoder wrap(DirectBuffer buffer);

    /**
     * Wraps the buffer from which the message is to be decoded.
     *
     * @param buffer the buffer to decode the message from
     * @param offset the first byte of the buffer to decode the message from
     * @param length the number of bytes in the message
     * @return this
     */
    Decoder wrap(DirectBuffer buffer, int offset, int length);

    /**
     * Returns the version of the schema used to generate this decoder.
     *
     * @return the version of the schema used to generate this decoder
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
     * Returns the name of the base entity associated with the message, or the {@code entityName} value if the decoder
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
     * Returns the identifier of the topic of the message.
     *
     * @return the identifier of the topic of the message
     */
    short getApplicationId();

    /**
     * Returns the sequence number of the topic.
     *
     * @return the sequence number of the topic
     */
    int getApplicationSequenceNumber();

    /**
     * Returns the identifier of the contributor that sent the message.
     *
     * @return the identifier of the contributor that sent the message
     */
    short getSenderId();

    /**
     * Returns the time the sequencer published this message.
     *
     * @return the time the sequencer published this message
     */
    long getTimestamp();

    /**
     * Returns the byte offset from the start of the message to the start of the optional fields in the message.
     *
     * @return the byte offset from the start of the message to the start of the optional fields in the message
     */
    short getOptionalFieldsIndex();

    /**
     * Returns the schema version used to publish this message.
     *
     * @return the schema version used to publish this message
     */
    byte getSchemaVersion();

    /**
     * Returns the type of this message.
     *
     * @return the type of this message
     */
    byte getMessageType();

    /**
     * Return the value of the field associated with the specified {@code name}.
     *
     * @param name the name of the field
     * @return the value of the field
     * @throws IllegalArgumentException if a field with the specified {@code name} is not associated with this message
     */
    Object get(String name);

    /**
     * Return the value of the field associated with the specified {@code name} that is a byte, short, int, or long.
     *
     * @param name the name of the field
     * @return the value of the field
     * @throws IllegalArgumentException if a byte, short, int, or long field with the specified {@code name} is not
     *     associated with this message
     */
    long integerValue(String name);

    /**
     * Return the value of the field associated with the specified {@code name} that is a float or double.
     *
     * @param name the name of the field
     * @return the value of the field
     * @throws IllegalArgumentException if a float or double field with the specified {@code name} is not associated
     *     with this message
     */
    double realValue(String name);

    /**
     * Return whether if the field is present in the message.
     *
     * @param name the name of the field
     * @return true if the field is present in the message
     * @throws IllegalArgumentException if a field with the specified {@code name} is not associated with this message
     */
    boolean isPresent(String name);
}
