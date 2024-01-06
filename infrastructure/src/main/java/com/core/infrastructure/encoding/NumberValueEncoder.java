package com.core.infrastructure.encoding;

import org.agrona.MutableDirectBuffer;

/**
 * A value encoder for long number values.
 */
public interface NumberValueEncoder {

    /**
     * Encodes the specified long {@code number} into the specified {@code buffer}.
     *
     * @param encoder the value encoder
     * @param buffer the buffer to encode
     * @param index the first byte of the buffer to encode
     * @param number the value to encode
     * @return the number of bytes encoded
     */
    int encode(ValueEncoder encoder, MutableDirectBuffer buffer, int index, long number);
}
