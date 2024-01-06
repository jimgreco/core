package com.core.infrastructure.encoding;

import com.core.infrastructure.buffer.BufferNumberUtils;
import org.agrona.MutableDirectBuffer;

/**
 * The {@code FixedPointValueEncoder} writes an ASCII encoded decimal representation of a fixed-point value into the
 * buffer.
 */
public class FixedPointValueEncoder implements NumberValueEncoder {

    private int impliedDecimals;
    private int minDecimals;
    private long multiplier;

    /**
     * Constructs a {@code FixedPointValueEncoder} with zero implied and minimum decimals.
     */
    public FixedPointValueEncoder() {
    }

    /**
     * Returns the number of implied decimals to encode.
     *
     * @return the number of implied decimals to encode
     */
    public int getImpliedDecimals() {
        return impliedDecimals;
    }

    /**
     * Returns the number of implied decimals to encode.
     *
     * @return the number of implied decimals to encode
     */
    public int getMinDecimals() {
        return minDecimals;
    }

    /**
     * Returns the fixed-point value as a double.
     *
     * @param value the fixed-point value
     * @return the double value
     */
    public double toDouble(long value) {
        return (double) value / multiplier;
    }

    /**
     * Returns the fixed-point value as a double.
     *
     * @param value the fixed-point value
     * @return the double value
     */
    public long toLong(double value) {
        return Math.round(value * multiplier);
    }

    /**
     * Sets the number of implied decimals to encode.
     *
     * @param impliedDecimals the number of implied decimals to encode
     * @return this
     */
    public FixedPointValueEncoder setImpliedDecimals(int impliedDecimals) {
        this.impliedDecimals = impliedDecimals;
        multiplier = BufferNumberUtils.pow10(impliedDecimals);
        return this;
    }

    /**
     * Sets the number of minimum decimals to encode.
     *
     * @param minDecimals the number of implied decimals to encode
     * @return this
     */
    public FixedPointValueEncoder setMinDecimals(int minDecimals) {
        this.minDecimals = minDecimals;
        return this;
    }

    @Override
    public int encode(ValueEncoder encoder, MutableDirectBuffer buffer, int index, long number) {
        return BufferNumberUtils.putAsAsciiDecimal(buffer, index, toDouble(number), minDecimals, impliedDecimals);
    }
}
