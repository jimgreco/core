package com.core.infrastructure.buffer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Utilities for manipulating {@code DirectBuffer}s.
 */
public class BufferUtils {

    private static final byte[] TRUE = "true".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] FALSE = "false".getBytes(StandardCharsets.US_ASCII);
    private static final AsciiStringBuffer TEMP = new AsciiStringBuffer();

    /**
     * An empty buffer.
     */
    public static final DirectBuffer EMPTY = BufferUtils.emptyBuffer();

    /**
     * Returns a new buffer with zero capacity.
     *
     * @return a new buffer with zero capacity
     */
    public static DirectBuffer emptyBuffer() {
        return new UnsafeBuffer();
    }

    /**
     * Returns a new mutable buffer with zero capacity.
     *
     * @return a new mutable buffer with zero capacity
     */
    public static MutableDirectBuffer mutableEmptyBuffer() {
        return new UnsafeBuffer();
    }

    /**
     * Wraps the string and returns a buffer that can be used temporarily.
     *
     * @param string the string to wrap
     * @return the buffer
     */
    public static DirectBuffer temp(String string) {
        TEMP.wrap(string);
        return TEMP;
    }

    /**
     * Wraps the buffer and returns a buffer that can be used temporarily.
     *
     * @param buffer the buffer
     * @param offset the offset
     * @param length the length
     * @return the buffer
     */
    public static DirectBuffer temp(DirectBuffer buffer, int offset, int length) {
        TEMP.wrap(buffer, offset, length);
        return TEMP;
    }

    /**
     * Returns a new mutable buffer with the specified {@code capacity} backed by a
     * {@code ByteBuffer}.
     *
     * @param capacity the new buffer's capacity, in bytes
     * @return the new buffer
     */
    public static MutableDirectBuffer allocate(int capacity) {
        var allocate = ByteBuffer.allocate(capacity);
        allocate.order(ByteOrder.LITTLE_ENDIAN);
        return new UnsafeBuffer(allocate);
    }

    /**
     * Returns a new mutable expandable buffer with the specified initial {@code capacity}.
     *
     * @param initialCapacity the new buffer's initial capacity, in bytes
     * @return the new buffer
     */
    public static MutableDirectBuffer allocateExpandable(int initialCapacity) {
        return new ExpandableArrayBuffer(initialCapacity);
    }

    /**
     * Returns a new mutable buffer with the specified {@code capacity} backed by a direct {@code ByteBuffer}.
     *
     * @param capacity the new buffer's capacity, in bytes
     * @return the new buffer
     */
    public static MutableDirectBuffer allocateDirect(int capacity) {
        var allocate = ByteBuffer.allocateDirect(capacity);
        allocate.order(ByteOrder.LITTLE_ENDIAN);
        return new UnsafeBuffer(allocate);
    }

    /**
     * Returns a new buffer that wraps the specified ASCII string.
     *
     * @param str the ASCII string to wrap
     * @return the new buffer
     */
    public static DirectBuffer fromAsciiString(String str) {
        return new AsciiStringBuffer(str);
    }

    /**
     * Creates an empty buffer and wraps the specified {@code buffer}.
     *
     * @param buffer the buffer to wrap
     * @param offset the first byte of the buffer to wrap
     * @param length the number of bytes in the buffer to wrap
     * @return the new buffer
     */
    public static DirectBuffer wrap(DirectBuffer buffer, int offset, int length) {
        var emptyBuffer = emptyBuffer();
        emptyBuffer.wrap(buffer, offset, length);
        return emptyBuffer;
    }

    /**
     * Returns a new buffer that is a byte-for-byte copy of the specified {@code buffer}.
     * 
     * @param buffer the buffer to copy
     * @return the new buffer
     */
    public static DirectBuffer copy(DirectBuffer buffer) {
        return copy(buffer, 0, buffer.capacity());
    }

    /**
     * Returns a new buffer that is a byte-for-byte copy of the specified {@code buffer} from the specified
     * {@code index} to {@code index + length}.
     *
     * @param buffer the buffer to copy
     * @param index the first byte of the buffer to copy
     * @param length the number of bytes to copy
     * @return the new buffer
     */
    public static DirectBuffer copy(DirectBuffer buffer, int index, int length) {
        var direct = buffer.byteBuffer() != null && buffer.byteBuffer().isDirect();
        var copy = direct ? allocateDirect(length) : allocate(length);
        copy.putBytes(0, buffer, index, length);
        return copy;
    }

    /**
     * Puts an ASCII encoded boolean {@code value} into the {@code buffer} at the specified {@code index}.
     * 
     * @param buffer the buffer
     * @param index the first byte of the buffer to put the ASCII encoded boolean {@code value}
     * @param value the boolean value to write
     * @return the number of bytes written
     */
    public static int putBoolAscii(MutableDirectBuffer buffer, int index, boolean value) {
        var written = value ? TRUE : FALSE;
        buffer.putBytes(index, written);
        return written.length;
    }

    /**
     * Returns an ASCII encoded string of the {@code buffer}.
     * Equivalent to invoking {@code buffer.getStringWithoutLengthAscii(0, buffer.capacity())}.
     *
     * @param buffer the buffer
     * @return the string
     */
    public static String toAsciiString(DirectBuffer buffer) {
        return toAsciiString(buffer, 0, buffer.capacity());
    }

    /**
     * Returns an ASCII encoded string of the {@code buffer}.
     * Equivalent to invoking {@code buffer.getStringWithoutLengthAscii(index, length)}.
     *
     * @param buffer the buffer
     * @param index the first byte of the buffer to convert to an ASCII encoded string
     * @param length the number of bytes to convert to an ASCII encoded string
     * @return the string
     */
    public static String toAsciiString(DirectBuffer buffer, int index, int length) {
        return buffer.getStringWithoutLengthAscii(index, length);
    }

    /**
     * Returns the underlying {@code ByteBuffer} with the position set to 0 and the limit set to the buffer's capacity.
     *
     * @param buffer the buffer
     * @return the underlying byte buffer
     */
    public static ByteBuffer byteBuffer(DirectBuffer buffer) {
        return byteBuffer(buffer, 0, buffer.capacity());
    }

    /**
     * Returns the underlying {@code ByteBuffer} with the position set to {@code index} and the limit set to
     * {@code index + length}.
     *
     * @param buffer the buffer
     * @param index the first byte of the underlying byte buffer
     * @param length the number after the index for the limit of the underlying byte buffer
     * @return the underlying byte buffer
     */
    public static ByteBuffer byteBuffer(DirectBuffer buffer, int index, int length) {
        //var position = (int) (buffer.addressOffset() - buffer.wrapAdjustment() + index);
        //var limit = position + length;
        if (buffer.byteBuffer() != null) {
            var position = buffer.wrapAdjustment() + index;
            var limit = position + length;
            var srcBuffer = buffer.byteBuffer();
            srcBuffer.limit(limit);
            srcBuffer.position(position);
            return srcBuffer;
        } else {
            var position = buffer.wrapAdjustment() + index;
            return ByteBuffer.wrap(buffer.byteArray(),  position, length);
        }
    }

    /**
     * Copies the bytes in the {@code buffer} from {@code [offset, offset + length)} to
     * {@code [0, length]}.
     *
     * @param buffer the buffer
     * @param offset the first byte of the buffer to copy to the first byte of the buffer
     * @param length the number of bytes to copy
     */
    public static void compact(MutableDirectBuffer buffer, int offset, int length) {
        if (length == 0) {
            return;
        }
        buffer.putBytes(0, buffer, offset, length);
    }

    /**
     * Returns the index in the {@code buffer} of the {@code pattern} subsequence of bytes
     * or -1 if {@code pattern} doesn't occur in bytes {@code [offset, length]}.
     *
     * @param buffer the buffer
     * @param pattern the pattern
     * @param offset the offset to begin searching from
     * @param length the number after the index of the last byte to search up to
     * @param patternLength the number of bytes to use in the pattern buffer
     * @return the first start index of pattern as a subsequence of buffer, or -1 if this does not occur
     */
    public static int indexOf(DirectBuffer buffer, DirectBuffer pattern, int offset, int length, int patternLength) {
        var maxIndex = length - patternLength;
        for (var i = offset; i <= maxIndex; ++i) {
            if (buffer.getByte(i) != pattern.getByte(0)) {
                continue;
            }
            var j = 1;
            while (j < patternLength && buffer.getByte(i + j) == pattern.getByte(j)) {
                ++j;
            }
            if (j == patternLength) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns the index in the {@code buffer} of the {@code character} byte or -1 if {@code character} doesn't occur in
     * bytes {@code [offset, length]}.
     *
     * @param buffer the buffer
     * @param offset the offset to begin searching from
     * @param length the number after the index of the last byte to search up to
     * @param character the character
     * @return the first start index of the character, or -1 if this does not occur
     */
    public static int indexOf(DirectBuffer buffer, int offset, int length, char character) {
        for (var i = offset; i <= offset + length; i++) {
            var bufferByte = buffer.getByte(i);
            if (bufferByte == character) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns true if the specified {@code buffer} and ASCII {@code string} string contain identical bytes.
     * 
     * @param buffer the buffer
     * @param string the string
     * @return true if the specified {@code buffer} and ASCII {@code string} string contain identical bytes 
     */
    public static boolean asciiEquals(DirectBuffer buffer, CharSequence string) {
        if (buffer == null) {
            return string == null;
        }
        if (string == null || buffer.capacity() != string.length()) {
            return false;
        }
        for (var i = 0; i < buffer.capacity(); i++) {
            var character1 = buffer.getByte(i);
            var character2 = (byte) string.charAt(i);
            if (character1 != character2) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if the specified {@code buffer} and ASCII {@code string} string contain identical bytes.
     *
     * @param buffer the buffer
     * @param string the string
     * @return true if the specified {@code buffer} and ASCII {@code string} string contain identical bytes 
     */
    public static boolean asciiEquals(CharSequence string, DirectBuffer buffer) {
        return asciiEquals(buffer, string);
    }

    /**
     * Returns true if the specified {@code buffer} and ASCII {@code string} string do not contain identical bytes.
     *
     * @param buffer the buffer
     * @param string the string
     * @return true if the specified {@code buffer} and ASCII {@code string} string do not contain identical bytes
     */
    public static boolean asciiNotEquals(CharSequence string, DirectBuffer buffer) {
        return !asciiEquals(buffer, string);
    }

    /**
     * Returns true if the specified {@code buffer} and ASCII {@code string} string do not contain identical bytes.
     *
     * @param buffer the buffer
     * @param string the string
     * @return true if the specified {@code buffer} and ASCII {@code string} string do not contain identical bytes
     */
    public static boolean asciiNotEquals(DirectBuffer buffer, CharSequence string) {
        return !asciiEquals(buffer, string);
    }
}