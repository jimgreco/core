package com.core.infrastructure.buffer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Utilities for working with floating-point numbers.
 */
public class BufferNumberUtils {

    private static final int DEFAULT_MAX_DECIMALS = 8;

    /**
     * Powers of 10.
     * The value of each array element is equal to 10^index.
     */
    private static final long[] POW10 = new long[] {
            1L,
            10L,
            100L,
            1000L,
            10000L,
            100000L,
            1000000L,
            10000000L,
            100000000L,
            1000000000L,
            10000000000L,
            100000000000L,
            1000000000000L,
            10000000000000L,
            100000000000000L,
            1000000000000000L,
            10000000000000000L,
            100000000000000000L,
            1000000000000000000L
    };

    /**
     * All the positive powers of 10 that can be
     * represented exactly in double/float.
     */
    private static final double[] SMALL_10_POW = {
            1.0e0,
            1.0e1, 1.0e2, 1.0e3, 1.0e4, 1.0e5,
            1.0e6, 1.0e7, 1.0e8, 1.0e9, 1.0e10,
            1.0e11, 1.0e12, 1.0e13, 1.0e14, 1.0e15,
            1.0e16, 1.0e17, 1.0e18, 1.0e19, 1.0e20,
            1.0e21, 1.0e22
    };

    private static final int MAX_SMALL_TEN = SMALL_10_POW.length - 1;
    private static final int MAX_DECIMAL_DIGITS = 18;

    private static final int MIN_LONG_VALUE_SIZE = String.valueOf(Long.MIN_VALUE).length();
    // these values are the most significant 15 fraction decimals for representing a long
    // -9223372036854775808L;
    private static final double MIN_DECIMAL = -9.223372036854775E18;
    // 9223372036854775807L;
    private static final double MAX_DECIMAL = 9.223372036854775E18;

    /**
     * Returns the power of 10 for the specified {@code exponent}.
     *
     * @param exponent the exponent
     * @return the power of 10 for the specified {@code exponent}
     */
    public static long pow10(int exponent) {
        return POW10[exponent];
    }

    /**
     * Returns the size of a long number when written as an ASCII string.
     *
     * @param number the number
     * @return the size of the ASCII string
     */
    public static int asciiSize(long number) {
        var num = number;
        var length = 0;

        if (num == 0) {
            return 1;
        } else if (num == Long.MIN_VALUE) {
            return MIN_LONG_VALUE_SIZE;
        } else if (num < 0) {
            length++;
            num = -num;
        }

        while (num > 0) {
            num /= 10;
            length++;
        }
        return length;
    }

    /**
     * Parses the specified {@code buffer} with an ASCII number into a long, returning {@code -1} if the buffer cannot
     * be parsed.
     * This is equivalent to invoking
     * {@link #parseAsLong(DirectBuffer, int, int, long) parseAsLong(buffer, 0,buffer.capacity(), -1)}.
     *
     * @param buffer the buffer to parse
     * @return the fixed-point representation of the ASCII decimal number.
     */
    public static long parseAsLong(DirectBuffer buffer) {
        return parseAsLong(buffer, 0, buffer.capacity(), -1);
    }

    /**
     * Parses the specified {@code buffer} with a ASCII number into a long, returning the specified default value if the
     * buffer cannot be parsed.
     * This is equivalent to invoking
     * {@link #parseAsLong(DirectBuffer, int, int, long) parseAsLong(buffer, 0, buffer.capacity(), defaultValue)}.
     *
     * @param buffer the buffer to parse
     * @param defaultValue the value to return if the buffer could not be parsed
     * @return the fixed-point representation of the ASCII decimal number.
     */
    public static long parseAsLong(DirectBuffer buffer, long defaultValue) {
        return parseAsLong(buffer, 0, buffer.capacity(), defaultValue);
    }

    /**
     * Parses the specified {@code buffer} with a ASCII number into a long, returning {@code -1} if the buffer cannot be
     * parsed.
     * This is equivalent to invoking
     * {@link #parseAsLong(DirectBuffer, int, int, long) parseAsLong(buffer, index, length, -1)}.
     *
     * @param buffer the buffer to parse
     * @param index the first byte of the buffer to parse
     * @param length the number of bytes in the buffer to parse
     * @return the fixed-point representation of the ASCII decimal number.
     */
    public static long parseAsLong(DirectBuffer buffer, int index, int length) {
        return parseAsLong(buffer, index, length, -1);
    }

    /**
     * Parses the specified {@code buffer} with a ASCII number into a long, returning the specified default value if the
     * buffer cannot be parsed.
     *
     * @param buffer the buffer to parse
     * @param index the first byte of the buffer to parse
     * @param length the number of bytes in the buffer to parse
     * @param defaultValue the value to return if the buffer could not be parsed
     * @return the fixed-point representation of the ASCII decimal number.
     */
    public static long parseAsLong(DirectBuffer buffer, int index, int length, long defaultValue) {
        buffer.boundsCheck(index, length);

        if (length == 0) {
            return defaultValue;
        } else if (length == 1) {
            var digit = buffer.getByte(index);
            var value = digit - '0';
            if (digit >= '0' && digit <= '9') {
                return value;
            } else {
                return defaultValue;
            }
        }

        final var end = index + length;
        final var first = buffer.getByte(index);
        var i = index;
        if (first == '-' || first == '+') {
            i++;
        }

        var value = 0L;

        for (; i < end; i++) {
            var digit = buffer.getByte(i);
            if (digit >= '0' && digit <= '9') {
                value *= 10;
                // add negative so we don't overflow
                value -= digit - '0';
            } else {
                // unknown character
                return defaultValue;
            }
        }

        return first == '-' ? value : -value;
    }

    /**
     * Parses the specified {@code buffer} with an ASCII decimal into a double, returning the {@code -1} if the buffer
     * cannot be parsed.
     * This is equivalent to invoking
     * {@link #fastParseAsDouble(DirectBuffer, int, int, double) fastParseAsDouble(buffer, index, length, -1)}.
     *
     * @param buffer the buffer to parse
     * @return the fixed-point representation of the ASCII decimal number.
     */
    public static double fastParseAsDouble(DirectBuffer buffer) {
        return fastParseAsDouble(buffer, 0, buffer.capacity(), -1);
    }

    /**
     * Parses the specified {@code buffer} with an ASCII decimal into a double, returning the specified default value if
     * the buffer cannot be parsed.
     * This is equivalent to invoking
     * {@link #fastParseAsDouble(DirectBuffer, int, int, double)
     * fastParseAsDouble(buffer, index, length, defaultValue)}.
     *
     * @param buffer the buffer to parse
     * @param defaultValue the value to return if the buffer could not be parsed
     * @return the fixed-point representation of the ASCII decimal number.
     */
    public static double fastParseAsDouble(DirectBuffer buffer, double defaultValue) {
        return fastParseAsDouble(buffer, 0, buffer.capacity(), defaultValue);
    }

    /**
     * Parses the specified {@code buffer} with an ASCII decimal into a double, returning {@code -1} if the buffer
     * cannot be parsed.
     * This is equivalent to invoking
     * {@link #fastParseAsDouble(DirectBuffer, int, int, double) fastParseAsDouble(buffer, index, length, -1)}.
     *
     * @param buffer the buffer to parse
     * @param index the first byte of the buffer to parse
     * @param length the number of bytes in the buffer to parse
     * @return the fixed-point representation of the ASCII decimal number.
     */
    public static double fastParseAsDouble(DirectBuffer buffer, int index, int length) {
        return fastParseAsDouble(buffer, index, length, -1);
    }

    /**
     * Parses the specified {@code buffer} with an ASCII decimal into a double, returning the specified default value if
     * the buffer cannot be parsed.
     *
     * @param buffer the buffer to parse
     * @param index the first byte of the buffer to parse
     * @param length the number of bytes in the buffer to parse
     * @param defaultValue the value to return if the buffer could not be parsed
     * @return the fixed-point representation of the ASCII decimal number.
     */
    public static double fastParseAsDouble(DirectBuffer buffer, int index, int length, double defaultValue) {
        buffer.boundsCheck(index, length);
        if (length == 0) {
            return defaultValue;
        } else if (length == 1) {
            var digit = buffer.getByte(index);
            return digit >= '0' && digit <= '9' ? digit - '0' : defaultValue;
        }

        var first = buffer.getByte(index);
        var isNegative = first == '-';
        var i = index;
        if (isNegative || first == '+') {
            i++;
        }

        var decimalPoint = -1;
        var longValue = 0L;
        var numDigits = 0;

        var end = index + length;
        for (; i < end; i++) {
            var digit = buffer.getByte(i);
            if (digit >= '0' && digit <= '9') {
                longValue *= 10;
                longValue += digit - '0';
                numDigits++;
            } else if (digit == '.') {
                if (decimalPoint != -1) {
                    // seen decimals twice or already in exponent where this is not allowed
                    return defaultValue;
                }
                decimalPoint = numDigits;
            } else if (digit == 'e' || digit == 'E') {
                i++;
                break;
            } else {
                // unknown character
                return defaultValue;
            }
        }

        var negativeExponent = false;
        if (i < end && buffer.getByte(i) == '-') {
            negativeExponent = true;
            i++;
        }

        var exponentValue = 0;
        for (; i < end; i++) {
            var digit = buffer.getByte(i);
            if (digit >= '0' && digit <= '9') {
                exponentValue *= 10;
                exponentValue += digit - '0';
            } else {
                // unknown character
                return defaultValue;
            }
        }

        var value = floatingPointComponentsToDouble(
                isNegative, longValue, decimalPoint, numDigits, negativeExponent, exponentValue, defaultValue);
        if (Double.compare(value, defaultValue) == 0) {
            try {
                return Double.parseDouble(BufferUtils.toAsciiString(buffer, index, length));
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        } else {
            return value;
        }
    }

    /**
     * Converts the specified components of a floating-point number into a double.
     * Computes the following formula with greater precision than a naive implementation:<pre>
     *
     *     (negativeFraction ? -1 : 1) * fixedToFloatingPoint(fraction, decimalPointIndex, fractionDigits)
     *     * 10 ^ ((negativeExponent ? -1 : 1) * exponent)
     * </pre>
     *
     * <p>Where the {@code fixedToFloatingPoint} function converts the long fixed-point value with an implied decimal
     * point at the specified {@code decimalPointIndex} value.
     * For example, {@code floatingPointComponentsToDouble(true, 1234, 1, 4, true, 3, -1) = -1.234e-3 = -0.001234}.
     *
     * <p>The {@code defaultValue} value will be returned if {@code fractionDigits} is greater than {@code 18}.
     *
     * @param negativeFraction true if the fraction component is negative
     * @param fraction the fraction component as a long fixed-point value
     * @param decimalPointIndex the index of the decimal point in the {@code fraction} component, or {@code -1} if the
     *     {@code fraction} component does not contain a decimal point
     * @param fractionDigits the total number of digits in the fraction, excluding the negative sign and decimal point
     * @param negativeExponent true if the exponent component is negative
     * @param exponent the exponent value
     * @param defaultValue the default value to return if the components cannot be converted into a double floating
     *     point number
     * @return the double value
     */
    public static double floatingPointComponentsToDouble(
            boolean negativeFraction, long fraction, int decimalPointIndex, int fractionDigits,
            boolean negativeExponent, int exponent,
            double defaultValue) {
        int decExponent;
        if (decimalPointIndex == -1) {
            // no decimal point so the exponent is the entire number
            decExponent = fractionDigits;
        } else {
            // exponent is the decimal point location
            decExponent = decimalPointIndex;
        }
        decExponent += (negativeExponent ? -1 : 1) * exponent;

        double doubleValue = fraction;
        int exp = decExponent - fractionDigits;

        //
        // longValue now contains a long integer with the value of
        // the first kDigits digits of the number.
        // dValue contains the (double) of the same.
        //

        if (fractionDigits <= MAX_DECIMAL_DIGITS) {
            if (exp == 0 || doubleValue == 0.0) {
                return negativeFraction ? -doubleValue : doubleValue; // small floating integer
            } else if (exp >= 0) {
                if (exp <= MAX_SMALL_TEN) {
                    //
                    // Can get the answer with one operation,
                    // thus one roundoff.
                    //
                    var roundValue = doubleValue * SMALL_10_POW[exp];
                    return negativeFraction ? -roundValue : roundValue;
                }
                var slop = MAX_DECIMAL_DIGITS - fractionDigits;
                if (exp <= MAX_SMALL_TEN + slop) {
                    //
                    // We can multiply dValue by 10^(slop)
                    // and it is still "small" and exact.
                    // Then we can multiply by 10^(exp-slop)
                    // with one rounding.
                    //
                    doubleValue *= SMALL_10_POW[slop];
                    var roundValue = doubleValue * SMALL_10_POW[exp - slop];
                    return negativeFraction ? -roundValue : roundValue;
                }
                //
                // Else we have a hard case with a positive exp.
                //
            } else {
                if (exp >= -MAX_SMALL_TEN) {
                    //
                    // Can get the answer in one division.
                    //
                    var roundValue = doubleValue / SMALL_10_POW[-exp];
                    return negativeFraction ? -roundValue : roundValue;
                }
                //
                // Else we have a hard case with a negative exp.
                //
            }
        }

        return defaultValue;
    }

    /**
     * Puts an ASCII encoded decimal representation of the double {@code value} into the {@code buffer} with minimum of
     * {@code 0} and a maximum of {@code 8} decimals printed.
     *
     * @param buffer the buffer
     * @param index the first byte of the buffer to write
     * @param value the double value
     * @return the number of bytes written
     * @throws IllegalArgumentException if {@code minDecimals} is negative
     */
    public static int putAsAsciiDecimal(
            MutableDirectBuffer buffer, int index, double value) {
        return putAsAsciiDecimal(buffer, index, value, 0, DEFAULT_MAX_DECIMALS);
    }

    /**
     * Puts an ASCII encoded decimal representation of the double {@code value} into the {@code buffer} with the
     * specified number of minimum decimals and a maximum of {@code 8} decimals printed.
     *
     * @param buffer the buffer
     * @param index the first byte of the buffer to write
     * @param value the double value
     * @param minDecimals the minimum number of decimals to write
     * @return the number of bytes written
     * @throws IllegalArgumentException if {@code minDecimals} is negative or greater than {@code 8}
     */
    public static int putAsAsciiDecimal(
            MutableDirectBuffer buffer, int index, double value, int minDecimals) {
        return putAsAsciiDecimal(buffer, index, value, minDecimals, DEFAULT_MAX_DECIMALS);
    }

    /**
     * Puts an ASCII encoded decimal representation of the double {@code value} into the {@code buffer} with the
     * specified number of minimum and maximum decimal places printed.
     *
     * @param buffer the buffer
     * @param index the first byte of the buffer to write
     * @param value the double value
     * @param minDecimals the minimum number of decimals to write
     * @param maxDecimals the maximum number of decimals to write
     * @return the number of bytes written
     * @throws IllegalArgumentException if {@code minDecimals} or {@code maxDecimals} is negative or {@code minDecimals}
     *     is greater than {@code maxDecimals}
     */
    public static int putAsAsciiDecimal(
            MutableDirectBuffer buffer, int index, double value, int minDecimals, int maxDecimals) {
        if (minDecimals < 0 || maxDecimals < 0 || minDecimals > maxDecimals) {
            throw new IllegalArgumentException("min and max decimals must be positive and non-negative:"
                    + " minDecimals=" + minDecimals + ", maxDecimals=" + maxDecimals);
        }

        // special cases
        var decimal = value;
        if (decimal == Double.POSITIVE_INFINITY) {
            return buffer.putStringWithoutLengthAscii(index, "Infinity");
        } else if (decimal == Double.NEGATIVE_INFINITY) {
            return buffer.putStringWithoutLengthAscii(index, "-Infinity");
        } else if (Double.isNaN(decimal)) {
            return buffer.putStringWithoutLengthAscii(index, "NaN");
        }

        var negative = decimal < 0;

        if (!negative && decimal >= MAX_DECIMAL || negative && decimal <= MIN_DECIMAL) {
            return buffer.putStringWithoutLengthAscii(index, Double.toString(decimal));
        }

        var position = index;

        if (negative) {
            decimal = -decimal;
            buffer.putByte(position++, (byte) '-');
        }

        var integer = (long) decimal;

        decimal -= integer;
        var multiplier = POW10[maxDecimals];
        var fraction = Math.round(decimal * multiplier);

        // write integer
        position += buffer.putLongAscii(position, integer);

        // return if the fraction can't be represented within the multiplier eg 0.999999999999
        if (fraction == multiplier) {
            return position - index;
        }

        // write decimal point
        if (fraction != 0 || minDecimals > 0) {
            buffer.putByte(position++, (byte) '.');
        }

        // write fraction
        var numFractions = 0;
        while (++numFractions <= minDecimals || fraction != 0) {
            multiplier = POW10[maxDecimals - numFractions];
            var digit = fraction / multiplier;
            buffer.putByte(position++, (byte) (digit + '0'));
            fraction -= digit * multiplier;
        }

        return position - index;
    }
}
