package com.core.infrastructure.collections;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.List;

/**
 * A {@code DirectBufferPool} is an object pool that maintains a pool of {@code MutableDirectBuffer}s.
 * When the user borrows from the pool, they specify the capacity of the buffer to borrow, and a buffer of that exact
 * size is returned.
 */
public class DirectBufferPool implements Encodable {

    private final List<MutableDirectBuffer>[] buffers;
    private final boolean direct;

    private int size;
    private int outstanding;

    /**
     * Constructs a {@code DirectBufferPool} with the specified max buffer capacity that can be borrowed from this
     * pool and whether the buffers are direct or heap byte buffers.
     *
     * @param maxBufferCapacity the maximum capacity of buffers that can be borrowed from this pool
     * @param direct true if the buffers are direct byte buffers, false if the buffers are heap byte buffers
     */
    @SuppressWarnings("unchecked")
    public DirectBufferPool(int maxBufferCapacity, boolean direct) {
        buffers = new List[maxBufferCapacity + 1];
        this.direct = direct;
    }

    /**
     * Borrows a buffer from the pool with a capacity equal to the specified {@code size}.
     *
     * @param size the capacity of the buffer
     * @return the buffer
     * @throws IllegalArgumentException if the pool does not support the specified {@code size}
     */
    public MutableDirectBuffer borrowBuffer(int size) {
        if (size >= buffers.length) {
            throw new IllegalArgumentException("buffer size is too large: " + size);
        }
        var theBuffers = buffers[size];
        if (theBuffers == null) {
            theBuffers = new CoreList<>();
            buffers[size] = theBuffers;
        }

        if (theBuffers.isEmpty()) {
            this.size++;
            theBuffers.add(direct ? BufferUtils.allocateDirect(size) : BufferUtils.allocate(size));
        }

        outstanding++;
        return theBuffers.remove(theBuffers.size() - 1);
    }

    /**
     * Returns the buffer to the pool.
     *
     * @param buffer the buffer
     */
    public void returnBuffer(DirectBuffer buffer) {
        outstanding--;
        buffers[buffer.capacity()].add((MutableDirectBuffer) buffer);
    }

    /**
     * Borrows a buffer from the pool and copies the specified {@code buffer} into it.
     *
     * @param buffer the buffer to copy
     * @return the buffer from the pool
     */
    public MutableDirectBuffer borrowBufferAndCopy(DirectBuffer buffer) {
        var buf = borrowBuffer(buffer.capacity());
        buf.putBytes(0, buffer, 0, buffer.capacity());
        return buf;
    }

    /**
     * Returns the specified {@code buffer} to the object pool and returns a buffer with a capacity equal to the
     * specified {@code size}.
     *
     * @param buffer the buffer to return to the pool
     * @param size the size of the buffer to return
     * @return a buffer with a capacity of the specified {@code size}
     * @implSpec the default implementation returns the specified {@code buffer} if the specified {@code size} is equal
     *     the capacity of the buffer, otherwise, the specified buffer is returned to the pool and a new buffer of the
     *     specified size is returned
     */
    public MutableDirectBuffer returnAndBorrowBuffer(DirectBuffer buffer, int size) {
        if (size == buffer.capacity()) {
            return (MutableDirectBuffer) buffer;
        } else {
            returnBuffer(buffer);
            return borrowBuffer(size);
        }
    }

    /**
     * Returns the maximum size of buffers in the pool.
     *
     * @return the maximum size of buffers in the pool
     */
    @Command(readOnly = true)
    public int getMaxBufferCapacity() {
        return buffers.length - 1;
    }

    /**
     * Returns true if the borrowed buffers are direct byte buffers.
     *
     * @return true if the borrowed buffers are direct byte buffers
     */
    @Command(readOnly = true)
    public boolean isDirect() {
        return direct;
    }

    /**
     * Returns the total number of buffers that have been borrowed from the pool.
     *
     * @return the total number of buffers that have been borrowed from the pool
     */
    @Command(readOnly = true)
    public int getOutstanding() {
        return outstanding;
    }

    /**
     * Returns the total number of buffers managed by the pool.
     *
     * @return the total number of buffers managed by the pool
     */
    @Command(readOnly = true)
    public int getSize() {
        return size;
    }

    /**
     * Returns the number of buffers remaining in the pool.
     *
     * @return the number of buffers remaining in the pool
     */
    @Command(readOnly = true)
    public int getRemaining() {
        return getSize() - getOutstanding();
    }

    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("direct").bool(isDirect())
                .string("maxBufferCapacity").number(getMaxBufferCapacity())
                .string("size").number(getSize())
                .string("outstanding").number(getOutstanding())
                .string("remaining").number(getRemaining())
                .closeMap();
    }

    @Override
    public String toString() {
        return toEncodedString();
    }
}