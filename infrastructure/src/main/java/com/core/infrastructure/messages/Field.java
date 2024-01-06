package com.core.infrastructure.messages;

import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;

import java.util.Objects;

/**
 * A core message field.
 */
public class Field implements Encodable {

    private final String name;
    private final Class<?> type;
    private final boolean required;
    private final boolean header;
    private final String metadata;
    private final int impliedDecimals;
    private final boolean primaryKey;
    private final boolean key;
    private final String foreignKey;

    /**
     * Creates a {@code Field} from the specified parameters.
     *
     * @param name the name of the field
     * @param type the type of the field
     * @param required true if the field is required to be present on the message
     * @param header true if the field is part of the message header
     * @param metadata metadata about the field
     * @param impliedDecimals the number of implied decimals in the fixed-point decimal value
     * @param primaryKey true if the field is the identity field for a message
     * @param key true if the field is part of a group of fields that defines the message's uniqueness
     * @param foreignKey the name of the message that the field references
     */
    public Field(
            String name, Class<?> type, boolean required, boolean header,
            String metadata, int impliedDecimals, boolean primaryKey, boolean key, String foreignKey) {
        this.name = Objects.requireNonNull(name, "name is null");
        this.type = Objects.requireNonNull(type, "type is null");
        this.required = required;
        this.header = header;
        this.metadata = metadata;
        this.impliedDecimals = impliedDecimals;
        this.primaryKey = primaryKey;
        this.key = key;
        this.foreignKey = foreignKey;
    }

    /**
     * Returns the name of the field.
     *
     * @return the name of the field
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the type of the field.
     *
     * @return the type of the field
     */
    public Class<?> getType() {
        return type;
    }

    /**
     * Returns true if the field is required to be present in the message.
     *
     * @return if the field is required to be present in the message
     */
    public boolean isRequired() {
        return required;
    }

    /**
     * Returns true if the field is in the header of the message.
     *
     * @return if the field is in the header of the message
     */
    public boolean isHeader() {
        return header;
    }

    /**
     * Returns true if the field is the primary key for an reference data message.
     *
     * @return true if the field is the primary key for an reference data message
     */
    public boolean isPrimaryKey() {
        return primaryKey;
    }

    /**
     * Returns the name of the message the field refers to.
     *
     * @return the name of the message the field refers to
     */
    public String getForeignKey() {
        return foreignKey;
    }

    /**
     * Returns true if the field is a reference to another message.
     *
     * @return if the field is a reference to another message
     */
    public boolean isForeignKey() {
        return foreignKey != null;
    }

    /**
     * Returns true if the field is part of the key that defines the uniqueness for the message.
     *
     * @return true if the field is part of the key that defines the uniqueness for the message
     */
    public boolean isKey() {
        return key;
    }

    /**
     * Returns metadata about the field.
     *
     * @return metadata about the field
     */
    public String getMetadata() {
        return metadata;
    }

    /**
     * Returns the number of implied decimals in the fixed-point decimal value.
     *
     * @return the number of implied decimals in the fixed-point decimal value
     */
    public int getImpliedDecimals() {
        return impliedDecimals;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var field = (Field) o;
        return name.equals(field.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("name").string(name)
                .string("required").bool(required)
                .string("header").bool(required)
                .string("metadata").string(metadata)
                .string("impliedDecimals").number(impliedDecimals)
                .string("primaryKey").bool(primaryKey)
                .string("key").bool(key)
                .string("foreignkey").string(foreignKey)
                .closeMap();
    }

    @Override
    public String toString() {
        return toEncodedString();
    }
}
