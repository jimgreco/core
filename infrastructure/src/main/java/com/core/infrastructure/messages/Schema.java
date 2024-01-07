package com.core.infrastructure.messages;

/**
 * A message schema.
 *
 * @param <DispatcherT> the type of the dispatcher
 */
public interface Schema<DispatcherT extends Dispatcher, ProviderT extends Provider> {

    /**
     * Returns the schema version.
     *
     * @return the schema version
     */
    int getVersion();

    /**
     * Returns the names of the messages in the schema.
     * The implementation may cache the returned array.
     *
     * @return the names of the messages in the schema
     */
    String[] getMessageNames();

    /**
     * Returns the byte offset of the name of the contributor to the bus.
     *
     * @return the byte offset of the name of the contributor to the bus
     */
    int getApplicationIdOffset();

    /**
     * Returns the byte offset of the sequence number of the contributor to the bus.
     *
     * @return the byte offset of the sequence number of the contributor to the bus
     */
    int getApplicationSequenceNumberOffset();

    /**
     * Returns the byte offset of the timestamp to the event was sent by the Sequencer.
     *
     * @return the byte offset of the timestamp to the event was sent by the Sequencer
     */
    int getTimestampOffset();

    /**
     * Returns the byte offset of the optional fields.
     *
     * @return the byte offset of the optional fields
     */
    int getOptionalFieldsOffset();

    /**
     * Returns the byte offset of the message version.
     *
     * @return the byte offset of the message version
     */
    int getSchemaVersionOffset();

    /**
     * Returns the byte offset of the message type.
     *
     * @return the byte offset of the message type
     */
    int getMessageTypeOffset();

    /**
     * Returns the length of the message header.
     *
     * @return the length of the message header
     */
    int getMessageHeaderLength();

    /**
     * Returns the value of the "heartbeatMessageName" property.
     *
     * @return the value of the "heartbeatMessageName" property
     * @implSpec the default implementation returns the value of {@code getProperty("heartbeatMessageName")}
     */
    default String getHeartbeatMessageName() {
        return getProperty("heartbeatMessageName");
    }

    /**
     * Returns the value of the "applicationDefinitionMessageName" property.
     *
     * @return the value of the "applicationDefinitionMessageName" property
     * @implSpec the default implementation returns the value of {@code getProperty("applicationDefinitionMessageName")}
     */
    default String getApplicationDefinitionMessageName() {
        return getProperty("applicationDefinitionMessageName");
    }

    /**
     * Returns the value of the "applicationDefinitionNameField" property.
     *
     * @return the value of the "applicationDefinitionNameField" property
     * @implSpec the default implementation returns the value of {@code getProperty("applicationDefinitionNameField")}
     */
    default String getApplicationDefinitionNameField() {
        return getProperty("applicationDefinitionNameField");
    }

    /**
     * Returns the value of the "applicationIdField" property.
     *
     * @return the value of the "applicationIdField" property
     * @implSpec the default implementation returns the value of {@code getProperty("applicationIdField")}
     */
    default String getApplicationIdField() {
        return getProperty("applicationIdField");
    }

    /**
     * Returns the value of the property with the specified key or null if the property is not found.
     *
     * @param key the property key
     * @return the value of the property with the specified key.
     * @throws IllegalArgumentException if a property with the specified {@code name} is not associated with this schema
     */
    String getProperty(String key);

    /**
     * Returns the names of the properties in the schema.
     *
     * @return the names of the properties in the schema
     */
    String[] getProperties();

    /**
     * Returns a new encoder associated with the specified message name.
     *
     * @param messageName the name of the message
     * @return a new encoder associated with the specified message name
     * @throws IllegalArgumentException if a message with the specified {@code name} is not associated with this schema
     * @param <EncoderT> the type of the encoder
     */
    <EncoderT extends Encoder> EncoderT createEncoder(String messageName);

    /**
     * Returns a new decoder associated with the specified message name.
     *
     * @param messageName the name of the message
     * @return a new decoder associated with the specified message name
     * @throws IllegalArgumentException if a message with the specified {@code name} is not associated with this schema
     * @param <DecoderT> the type of the decoder
     */
    <DecoderT extends Decoder> DecoderT createDecoder(String messageName);

    /**
     * Returns the message type associated with the specified message name.
     *
     * @param messageName the name of the message
     * @return the message type
     */
    int getMessageType(String messageName);

    /**
     * Returns the name of the messagefor the specified message type.
     *
     * @param messageType the message type
     * @return the name of the message
     */
    String getMessageName(byte messageType);

    /**
     * Returns a new message dispatcher for this schema.
     *
     * @return a new message dispatcher for this schema
     */
    DispatcherT createDispatcher();

    /**
     * Wraps the specified message publisher and returns a message provider for this schema.
     *
     * @param messagePublisher the message publisher
     * @return a new message publisher for this schema
     */
    ProviderT createProvider(MessagePublisher messagePublisher);
}
