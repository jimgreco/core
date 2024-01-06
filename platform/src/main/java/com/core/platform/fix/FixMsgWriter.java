package com.core.platform.fix;

import org.agrona.DirectBuffer;

/**
 * A writer of FIX messages.
 *
 * <p>The FIX message writer has type-specific methods to write FIX fields including
 * {@link #putBuffer(int, DirectBuffer) buffers}, {@link #putAsciiString(int, String) strings},
 * {@link #putAsciiCharacter(int, char) characters}, {@link #putEnum(int, FixEnum) enums},
 * {@link #putInteger(int, long) integers}, and {@link #putDecimal(int, double) decimals}.
 *
 * <p>Invoking {@link #send()} will send the FIX message to the counterparty.
 */
public interface FixMsgWriter {

    /**
     * Writes the the FIX field with the specified {@code tag} and buffer {@code value}.
     *
     * @param tag the tag
     * @param value the value
     * @return this
     * @implSpec the default implementation invokes {@code putBuffer(tag, value, 0, value.capacity())}
     */
    default FixMsgWriter putBuffer(int tag, DirectBuffer value) {
        return putBuffer(tag, value, 0, value.capacity());
    }

    /**
     * Writes the the FIX field with the specified {@code tag} and buffer {@code value}.
     *
     * @param tag the tag
     * @param value the value
     * @param offset the first byte of the value to write
     * @param length the number of bytes to write
     * @return this
     */
    FixMsgWriter putBuffer(int tag, DirectBuffer value, int offset, int length);

    /**
     * Writes the the FIX field with the specified {@code tag} and string {@code value}.
     *
     * @param tag the tag
     * @param value the value
     * @return this
     */
    FixMsgWriter putAsciiString(int tag, String value);

    /**
     * Writes the the FIX field with the specified {@code tag} and ASCII character {@code value}.
     *
     * @param tag the tag
     * @param value the value
     * @return this
     */
    FixMsgWriter putAsciiCharacter(int tag, char value);

    /**
     * Writes the the FIX field with the specified {@code tag} and enumeration set {@code value}.
     *
     * @param tag the tag
     * @param value the value
     * @param <T> the enumeration type
     * @return this
     * @implSpec the default implementation invokes {@code putBuffer(tag, value.value(), 0, value.value().capacity()}
     */
    default <T extends Enum<T>> FixMsgWriter putEnum(int tag, FixEnum<T> value) {
        return putBuffer(tag, value.getValue(), 0, value.getValue().capacity());
    }

    /**
     * Writes the the FIX field with the specified {@code tag} and the long {@code value} as an ASCII integer.
     *
     * @param tag the tag
     * @param value the value
     * @return this
     */
    FixMsgWriter putInteger(int tag, long value);

    /**
     * Writes the the FIX field with the specified {@code tag} and the floating-point double {@code value} as an ASCII
     * decimal.
     *
     * @param tag the tag
     * @param value the value
     * @return this
     */
    FixMsgWriter putDecimal(int tag, double value);

    /**
     * Writes the the FIX field with the specified {@code tag} and the long {@code value} in nanoseconds since epoch,
     * January 1, 1970, as FIX timestamp (e.g., {@code yyyyMMdd-HH:mm:ss.SSS}).
     *
     * @param tag the tag
     * @param value the value
     * @return this
     */
    FixMsgWriter putTimestamp(int tag, long value);

    /**
     * Writes the BodyLength[9] and Checksum[10] fields.
     */
    void send();

    /**
     * Returns the underlying buffer of the FIX message that has been written.
     *
     * @return the underlying buffer of the FIX message that has been written
     */
    DirectBuffer getBuffer();

    /**
     * Returns the {@code FixMsg} view of the written FIX message.
     * This is only a view and new fields written will be reflected in the view.
     *
     * @return the {@code FixMsg} view of the written FIX message
     * @apiNote this returned view is not optimized for performance
     */
    FixMsg getFixMsg();
}
