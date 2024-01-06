package com.core.platform.fix;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.buffer.BufferNumberUtils;
import org.agrona.DirectBuffer;

import java.util.List;
import java.util.function.Function;

/**
 * A FIX message is an ordered collection of FIX fields.
 * FIX fields are a key/value pair with a integer tag and a value that can be interpreted as a string, integer,
 * fixed-point, or enumeration set value.
 * No attempt is made to account for repeating group structures.
 *
 * <p>The entire message buffer can be retrieved with {@link #getBuffer()}.
 * Individual fields can be retrieved by tag (e.g., {@link #getValue(int) getValue},
 * {@link #getValueAsInteger(int) getValueAsInteger}, {@link #getValueAsDouble(int) getValueAsDouble},
 * {@link #getValueAsEnum(int, Function) getValueAsEnum}) or position in the message
 * (e.g., {@link #getValueAt(int) getValueAt}, and other getValueAs*At methods).
 *
 * <p>The following will iterate through all tag/values in order.<pre>
 *
 *     for (var i = 0; i &lt; fixMsg.getSize(); i++) {
 *         var tag = fixMsg.getTagAt(i);
 *         var value = fixMsg.getValueAt(i);
 *     }
 * </pre>
 */
public interface FixMsg {

    /**
     * Returns the underlying buffer of the FIX message.
     *
     * @return the underlying buffer of the FIX message
     */
    DirectBuffer getBuffer();

    /**
     * Returns the number of fields in the FIX message.
     *
     * @return the number of fields in the FIX message
     */
    int getSize();

    /**
     * Returns the tag of the field at the specified {@code index}.
     *
     * @param index the field index
     * @return the tag of the field
     * @throws IndexOutOfBoundsException if {@code index < 0 || &gt;= getSize()}
     */
    int getTagAt(int index);

    /**
     * Returns the value of the field at the specified {@code index}.
     *
     * @param index the field index
     * @return the value of the field
     * @throws IndexOutOfBoundsException if {@code index < 0 || &gt;= getSize()}
     */
    DirectBuffer getValueAt(int index);

    /**
     * Returns the value of the field for the specified FIX {@code tag}.
     *
     * @param tag the FIX tag
     * @return the value of the field
     * @implSpec the default implementation iterates through the FIX message's tags and returns the value of the first
     *     field with the specified {@code tag}
     */
    default DirectBuffer getValue(int tag) {
        for (var i = 0; i < getSize(); i++) {
            if (getTagAt(i) == tag) {
                return getValueAt(i);
            }
        }
        return null;
    }

    /**
     * Returns true if the FIX message contains the specified FIX {@code tag}.
     *
     * @param tag the FIX tag
     * @return true if the FIX message contains the specified FIX {@code tag}
     * @implSpec the default implementation returns true if {@code getValue} does not return null
     */
    default boolean containsTag(int tag) {
        return getValue(tag) != null;
    }

    /**
     * Parses and returns the value of the field for the specified FIX {@code tag} as an integer.
     * -1 is returned if the value cannot be parsed as an integer.
     *
     * @param tag the FIX tag
     * @return the value of the field for the specified FIX {@code tag} as an integer
     * @implSpec the default implementation returns the result of {@link BufferNumberUtils#parseAsLong(
     *     DirectBuffer, int, int, long) BufferNumberUtils#parseAsLong(value, 0, value.capacity(), -1)} with the value
     *     for the specified {@code tag}, or {@code -1} if there is no value for the specified tag
     */
    default long getValueAsInteger(int tag) {
        return toLong(getValue(tag), -1);
    }

    /**
     * Parses and returns the value of the field for the specified FIX {@code tag} as an integer.
     * The specified {@code defaultValue} is returned if the value cannot be parsed as an integer.
     *
     * @param tag the FIX tag
     * @param defaultValue the value to return if the value cannot be parsed as an integer
     * @return the value of the field for the specified FIX {@code tag} as an integer
     * @implSpec the default implementation returns the result of {@link BufferNumberUtils#parseAsLong(
     *     DirectBuffer, int, int, long) BufferNumberUtils.parseAsLong(value, 0, value.capacity(), defaultValue)} with
     *     the value for the specified {@code tag}, or {@code defaultValue} if there is no value for the specified tag
     */
    default long getValueAsInteger(int tag, long defaultValue) {
        return toLong(getValue(tag), defaultValue);
    }

    /**
     * Parses and returns the value of the field at the specified {@code index} as an integer.
     * -1 is returned if the value cannot be parsed as an integer.
     *
     * @param index the index of the tag
     * @return the value of the field for the specified {@code index} as an integer
     * @implSpec the default implementation returns the result of {@link BufferNumberUtils#parseAsLong(
     *     DirectBuffer, int, int, long) BufferNumberUtils.parseAsLong(value, 0, value.capacity(), -1)} with the value
     *     for the specified {@code index}, or {@code -1} if there is no value for the specified index
     */
    default long getValueAsIntegerAt(int index) {
        return toLong(getValueAt(index), -1);
    }

    /**
     * Parses and returns the value of the field at the specified {@code index} as an integer.
     * The specified {@code defaultValue} is returned if the value cannot be parsed as an integer.
     *
     * @param index the index of the tag
     * @param defaultValue the value to return if the value cannot be parsed as an integer
     * @return the value of the field for the specified FIX {@code index} as an integer
     * @implSpec the default implementation returns the result of {@link BufferNumberUtils#parseAsLong(
     *     DirectBuffer, int, int, long) BufferNumberUtils.parseAsLong(value, 0, value.capacity(), defaultValue)} with
     *     the value for the specified {@code index}, or {@code defaultValue} if there is no value for the specified
     *     index
     */
    default long getValueAsIntegerAt(int index, long defaultValue) {
        return toLong(getValueAt(index), defaultValue);
    }

    private static long toLong(DirectBuffer value, long defaultValue) {
        return value == null ? defaultValue : BufferNumberUtils.parseAsLong(value, 0, value.capacity(), defaultValue);
    }

    /**
     * Parses and returns the value of the field for the specified FIX {@code tag} as a double.
     * The default value {@code -1} is returned if the value cannot be parsed as a double.
     *
     * @param tag the FIX tag
     * @return the value of the field for the specified FIX {@code tag} as a double
     * @implSpec the default implementation returns the result of {@link BufferNumberUtils#fastParseAsDouble(
     *     DirectBuffer, int, int, double) BufferNumberUtils.fastParseAsDouble(value, 0, value.capacity(), -1)}
     *     with the value for the specified {@code tag}, or {@code -1} if there is no value for the specified tag
     */
    default double getValueAsDouble(int tag) {
        return toDouble(getValue(tag), -1);
    }

    /**
     * Parses and returns the value of the field for the specified FIX {@code tag} as a double
     * The specified default value is returned if the value cannot be parsed as a double.
     *
     * @param tag the FIX tag
     * @param defaultValue the value to return if the field cannot be parsed as a double
     * @return the value of the field for the specified FIX {@code tag} as a double
     * @implSpec the default implementation returns the result of
     *     {@link BufferNumberUtils#fastParseAsDouble(DirectBuffer, int, int, double)
     *     BufferNumberUtils.fastParseAsDouble(value, 0, value.capacity(), defaultValue)} with
     *     the value for the specified {@code tag}, or {@code defaultValue} if there is no value for the specified tag
     */
    default double getValueAsDouble(int tag, double defaultValue) {
        return toDouble(getValue(tag), defaultValue);
    }

    /**
     * Parses and returns the value of the field at the specified {@code index} as a double.
     * The default value {@code -1} is returned if the value cannot be parsed as a double.
     *
     * @param index the index of the tag
     * @return the value of the field for the specified FIX {@code tag} as a double
     * @implSpec the default implementation returns the result of
     *     {@link BufferNumberUtils#fastParseAsDouble(DirectBuffer, int, int, double)
     *     BufferNumberUtils.fastParseAsDouble(value, 0, value.capacity(), -1)} with the value for the specified
     *     {@code tag}, or {@code -1} if there is no value for the specified tag
     */
    default double getValueAsDoubleAt(int index) {
        return toDouble(getValueAt(index), -1);
    }

    /**
     * Parses and returns the value of the field at the specified {@code index} as a double.
     * The specified default value is returned if the value cannot be parsed as a double.
     *
     * @param index the index of the tag
     * @param defaultValue the value to return if the field cannot be parsed
     * @return the value of the field for the specified FIX {@code tag} as a double
     * @implSpec the default implementation returns the result of
     *     {@link BufferNumberUtils#fastParseAsDouble(DirectBuffer, int, int, double)
     *     BufferNumberUtils.fastParseAsDouble(value, 0, value.capacity(), defaultValue)} with the
     *     value for the specified {@code tag}, or {@code defaultValue} if there is no value for the specified tag
     */
    default double getValueAsDoubleAt(int index, double defaultValue) {
        return toDouble(getValueAt(index), defaultValue);
    }

    private static double toDouble(DirectBuffer value, double defaultValue) {
        return value == null
                ? defaultValue
                : BufferNumberUtils.fastParseAsDouble(value, 0, value.capacity(), defaultValue);
    }

    /**
     * Parses and returns the value of the field for the specified FIX {@code tag} as the specified FIX enumerator
     * conversion function.
     *
     * @param tag the FIX tag
     * @param fixEnumConversionFn the FIX enumerator conversion function
     * @param <T> the FIX enumerator type
     * @return the FIX enumeration set value
     * @implSpec the default implementation invokes {@code FixEnum.apply(value)} with the value for the specified
     *     {@code tag}, or null if there is no value for the specified tag.
     */
    default <T extends Enum<T>> T getValueAsEnum(int tag, Function<DirectBuffer, T> fixEnumConversionFn) {
        return toEnum(getValue(tag), fixEnumConversionFn);
    }

    /**
     * Parses and returns the value of the field at the specified {@code index} as the specified FIX enumerator
     * conversion function.
     *
     * @param index the index of the tag
     * @param fixEnumConversionFn the FIX enumerator conversion function
     * @param <T> the FIX enumerator type
     * @return the FIX enumeration set value
     * @implSpec the default implementation invokes {@code FixEnum.apply(value)} with the value at the specified
     *     {@code index}, or null if there is no value at the specified index.
     */
    default <T extends Enum<T>> T getValueAsEnumAt(int index, Function<DirectBuffer, T> fixEnumConversionFn) {
        return toEnum(getValueAt(index), fixEnumConversionFn);
    }

    private static <T extends Enum<T>> T toEnum(DirectBuffer value, Function<DirectBuffer, T> fixEnumConversionFn) {
        return value == null ? null : fixEnumConversionFn.apply(value);
    }

    /**
     * Parses and returns the value of the field for the specified FIX {@code tag} as a timestamp representing the
     * number of nanoseconds since Unix epoch, otherwise returns -1 if the field does not exist or the timestamp is
     * unparseable.
     *
     * @param tag the FIX tag
     * @return the timestamp
     */
    long getValueAsTimestamp(int tag);

    /**
     * Parses and returns the value of the field at the specified {@code index} as a timestamp representing the
     * number of nanoseconds since Unix epoch, otherwise returns -1 if the field does not exist or the timestamp is
     * unparseable.
     *
     * @param index the index of the tag
     * @return the timestamp
     */
    long getValueAsTimestampAt(int index);

    /**
     * Parses and returns the value of the field for the specified FIX {@code tag} as a datestamp representing the
     * number of days since Unix epoch, otherwise returns -1 if the field does not exist or the datestamp is
     * unparseable.
     *
     * @param tag the FIX tag
     * @return the timestamp
     */
    default long getValueAsDatestamp(int tag) {
        return toDatestamp(getValue(tag));
    }

    /**
     * Parses and returns the value of the field at the specified {@code index} as a datestamp representing the
     * number of days since Unix epoch, otherwise returns -1 if the field does not exist or the timestamp is
     * unparseable.
     *
     * @param index the index of the tag
     * @return the timestamp
     */
    default long getValueAsDatestampAt(int index) {
        return toDatestamp(getValueAt(index));
    }

    /**
     * Parses and returns the value of the field for the specified FIX {@code tag} as a datestamp representing the
     * number of days since Unix epoch, otherwise returns -1 if the field does not exist or the datestamp is
     * unparseable.
     *
     * @param tag the FIX tag
     * @return the timestamp
     */
    default long getValueAsTime(int tag) {
        return toTime(getValue(tag));
    }

    /**
     * Parses and returns the value of the field at the specified {@code index} as a datestamp representing the
     * number of days since Unix epoch, otherwise returns -1 if the field does not exist or the timestamp is
     * unparseable.
     *
     * @param index the index of the tag
     * @return the timestamp
     */
    default long getValueAsTimeAt(int index) {
        return toTime(getValueAt(index));
    }

    /**
     * Returns a list of repeating groups specified by a tag that specifies the number of groups and the first tag in
     * each repeating group.
     *
     * @param numGroupsTag the tag that specifies the number of repeating groups
     * @param firstTagInGroup the first tag in each group
     * @return the repeating groups
     */
    List<FixMsg> getRepeatingGroups(int numGroupsTag, int firstTagInGroup);

    private static long toDatestamp(DirectBuffer value) {
        return value == null ? -1 : FixDateTimeParser.parseDate(value, 0, value.capacity());
    }

    private static long toTime(DirectBuffer value) {
        return value == null ? -1 : FixDateTimeParser.parseTime(value, 0, value.capacity());
    }

    /**
     * Creates a new FIX message from the specified string.
     * The pipe character ('|') is replaced with the SOH character (0x01) before parsing.
     *
     * @param fixMsgStr the FIX message represented as a string
     * @return the FIX message
     */
    static FixMsg parse(String fixMsgStr) {
        var buffer = BufferUtils.fromAsciiString(
                fixMsgStr.replace(FixUtils.SOH_PRINT, (char) FixUtils.SOH));
        var fixMsg = new BufferFixMsg();
        fixMsg.parse(buffer, 0, buffer.capacity());
        return fixMsg;
    }
}
