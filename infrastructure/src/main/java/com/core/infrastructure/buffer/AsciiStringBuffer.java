package com.core.infrastructure.buffer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import static org.agrona.BufferUtil.ARRAY_BASE_OFFSET;

/**
 * An implementation of {@code DirectBuffer} that wraps a string.
 */
public class AsciiStringBuffer implements DirectBuffer {

    private String string;
    private byte[] bytes;
    private ByteBuffer buffer;

    /**
     * Creates a new {@code AsciiStringBuffer}.
     */
    public AsciiStringBuffer() {
        this(null);
    }

    /**
     * Creates a new {@code AsciiStringBuffer} with the specified wrapped {@code string}.
     *
     * @param string the string
     */
    public AsciiStringBuffer(String string) {
        wrap(string);
    }

    /**
     * Clears the wrapped string.
     *
     * @return this
     */
    public AsciiStringBuffer clear() {
        string = null;
        bytes = null;
        buffer = null;
        return this;
    }

    /**
     * Wraps the string.
     *
     * @param string the string
     * @return this
     */
    public AsciiStringBuffer wrap(String string) {
        this.string = string;
        bytes = null;
        buffer = null;
        return this;
    }

    @Override
    public void wrap(byte[] buffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void wrap(byte[] buffer, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void wrap(ByteBuffer buffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void wrap(ByteBuffer buffer, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void wrap(DirectBuffer buffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void wrap(DirectBuffer buffer, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void wrap(long address, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long addressOffset() {
        return ARRAY_BASE_OFFSET;
    }

    @Override
    public byte[] byteArray() {
        if (bytes == null) {
            bytes = string.getBytes(StandardCharsets.US_ASCII);
        }
        return bytes;
    }

    @Override
    public ByteBuffer byteBuffer() {
        if (buffer == null) {
            buffer = ByteBuffer.wrap(byteArray());
        }
        return buffer;
    }

    @Override
    public int capacity() {
        return string.length();
    }

    @Override
    public void checkLimit(int limit) {
        if (limit > string.length()) {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public long getLong(int index, ByteOrder byteOrder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int index, ByteOrder byteOrder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int parseNaturalIntAscii(int index, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long parseNaturalLongAscii(int index, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int parseIntAscii(int index, int length) {
        return Integer.parseInt(string, index, length, 10);
    }

    @Override
    public long parseLongAscii(int index, int length) {
        return Long.parseLong(string, index, length, 10);
    }

    @Override
    public double getDouble(int index, ByteOrder byteOrder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int index, ByteOrder byteOrder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(int index, ByteOrder byteOrder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public char getChar(int index, ByteOrder byteOrder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public char getChar(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(int index) {
        return (byte) string.charAt(index);
    }

    @Override
    public void getBytes(int index, byte[] dst) {
        for (var i = 0; i < dst.length; i++) {
            dst[i] = (byte) string.charAt(index + i);
        }
    }

    @Override
    public void getBytes(int index, byte[] dst, int offset, int length) {
        for (var i = 0; i < length; i++) {
            dst[offset + i] = (byte) string.charAt(index + i);
        }
    }

    @Override
    public void getBytes(int index, MutableDirectBuffer dstBuffer, int dstIndex, int length) {
        for (var i = 0; i < length; i++) {
            dstBuffer.putByte(dstIndex + i, (byte) string.charAt(index + i));
        }
    }

    @Override
    public void getBytes(int index, ByteBuffer dstBuffer, int length) {
        for (var i = 0; i < length; i++) {
            dstBuffer.put(i, (byte) string.charAt(index + i));
        }
    }

    @Override
    public void getBytes(int index, ByteBuffer dstBuffer, int dstOffset, int length) {
        for (var i = 0; i < length; i++) {
            dstBuffer.put(dstOffset + i, (byte) string.charAt(index + i));
        }
    }

    @Override
    public String getStringAscii(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStringAscii(int index, Appendable appendable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getStringAscii(int index, ByteOrder byteOrder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStringAscii(int index, Appendable appendable, ByteOrder byteOrder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getStringAscii(int index, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStringAscii(int index, int length, Appendable appendable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getStringWithoutLengthAscii(int index, int length) {
        return string.substring(index, index + length);
    }

    @Override
    public int getStringWithoutLengthAscii(int index, int length, Appendable appendable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getStringUtf8(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getStringUtf8(int index, ByteOrder byteOrder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getStringUtf8(int index, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getStringWithoutLengthUtf8(int index, int length) {
        return string.substring(index, index + length);
    }

    @Override
    public void boundsCheck(int index, int length) {
        if (index < 0 || index + length > string.length()) {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public int wrapAdjustment() {
        return 0;
    }

    @Override
    public int compareTo(DirectBuffer that) {
        final int thisCapacity = capacity();
        final int thatCapacity = that.capacity();

        for (int i = 0, length = Math.min(thisCapacity, thatCapacity); i < length; i++) {
            final int cmp = Byte.compare(getByte(i), that.getByte(i));
            if (0 != cmp) {
                return cmp;
            }
        }

        if (thisCapacity != thatCapacity) {
            return thisCapacity - thatCapacity;
        }

        return 0;
    }

    @Override
    public int hashCode() {
        var hashCode = 1;

        for (int i = 0, length = capacity(); i < length; i++) {
            hashCode = 31 * hashCode + getByte(i);
        }

        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        if (obj instanceof final AsciiStringBuffer that) {
            return string.equals(that.string);
        } else if (obj instanceof final DirectBuffer that) {
            if (capacity() != that.capacity()) {
                return false;
            }

            for (var i = 0; i < capacity(); i++) {
                if (getByte(i) != that.getByte(i)) {
                    return false;
                }
            }

            return true;
        } else if (obj instanceof final String that) {
            return string.equals(that);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return string;
    }
}
