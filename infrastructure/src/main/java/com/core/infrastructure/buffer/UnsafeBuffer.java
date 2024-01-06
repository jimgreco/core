package com.core.infrastructure.buffer;

import org.agrona.DirectBuffer;
import org.agrona.UnsafeAccess;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Supports regular, byte ordered, and atomic (memory ordered) access to an underlying buffer. The buffer can be a
 * byte[], one of the various {@link ByteBuffer} implementations, or an off Java heap memory address.
 *
 * <p>{@link ByteOrder} of a wrapped buffer is not applied to the {@link org.agrona.concurrent.UnsafeBuffer}.
 * {@link org.agrona.concurrent.UnsafeBuffer}s are effectively stateless and can be used concurrently, with the
 * exception of wrapping. To control {@link ByteOrder} use the appropriate method with the {@link ByteOrder} overload.
 *
 * <p><b>Note:</b> This class has a natural ordering that is inconsistent with equals.
 * Types may be different but equal on buffer contents.
 *
 * <p><b>Note:</b> The wrap methods on this class are not thread safe. Concurrent access should only happen after a
 * successful wrap.
 */
@SuppressWarnings("PMD.OverrideBothEqualsAndHashcode")
public class UnsafeBuffer extends org.agrona.concurrent.UnsafeBuffer {

    /**
     * Empty constructor for a reusable wrapper buffer.
     */
    public UnsafeBuffer() {
        super();
    }

    /**
     * Attach a view to a byte[] for providing direct access.
     *
     * @param buffer to which the view is attached.
     */
    public UnsafeBuffer(byte[] buffer) {
        super(buffer);
    }

    /**
     * Attach a view to a byte[] for providing direct access.
     *
     * @param buffer to which the view is attached.
     * @param offset within the buffer to begin.
     * @param length of the buffer to be included.
     */
    public UnsafeBuffer(byte[] buffer, int offset, int length) {
        super(buffer, offset, length);
    }

    /**
     * Attach a view to a {@link ByteBuffer} for providing direct access, the {@link ByteBuffer} can be
     * heap based or direct.
     *
     * @param buffer to which the view is attached.
     */
    public UnsafeBuffer(ByteBuffer buffer) {
        super(buffer);
    }

    /**
     * Attach a view to a {@link ByteBuffer} for providing direct access, the {@link ByteBuffer} can be
     * heap based or direct.
     *
     * @param buffer to which the view is attached.
     * @param offset within the buffer to begin.
     * @param length of the buffer to be included.
     */
    public UnsafeBuffer(ByteBuffer buffer, int offset, int length) {
        super(buffer, offset, length);
    }

    /**
     * Attach a view to an existing {@link DirectBuffer}.
     *
     * @param buffer to which the view is attached.
     */
    public UnsafeBuffer(DirectBuffer buffer) {
        super(buffer);
    }

    /**
     * Attach a view to an existing {@link DirectBuffer}.
     *
     * @param buffer to which the view is attached.
     * @param offset within the buffer to begin.
     * @param length of the buffer to be included.
     */
    public UnsafeBuffer(DirectBuffer buffer, int offset, int length) {
        super(buffer, offset, length);
    }

    /**
     * Attach a view to an off-heap memory region by address. This is useful for interacting with native libraries.
     *
     * @param address where the memory begins off-heap
     * @param length  of the buffer from the given address
     */
    public UnsafeBuffer(long address, int length) {
        super(address, length);
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
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        if (obj instanceof final org.agrona.concurrent.UnsafeBuffer that) {
            if (capacity() != that.capacity()) {
                return false;
            }

            final byte[] thisByteArray = this.byteArray();
            final byte[] thatByteArray = that.byteArray();
            final long thisOffset = this.addressOffset();
            final long thatOffset = that.addressOffset();

            for (int i = 0, length = capacity(); i < length; i++) {
                if (UnsafeAccess.UNSAFE.getByte(thisByteArray, thisOffset + i)
                        != UnsafeAccess.UNSAFE.getByte(thatByteArray, thatOffset + i)) {
                    return false;
                }
            }

            return true;
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
            if (capacity() != that.length()) {
                return false;
            }

            for (var i = 0; i < capacity(); i++) {
                if (getByte(i) != that.charAt(i)) {
                    return false;
                }
            }

            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return new String(byteArray(), wrapAdjustment(), capacity(), StandardCharsets.US_ASCII);
    }
}
