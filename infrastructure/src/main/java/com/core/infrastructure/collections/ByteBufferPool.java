package com.core.infrastructure.collections;

import com.core.infrastructure.command.Command;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * A {@code ByteBufferPool} is an object pool that maintains a pool of {@code ByteBuffer}s.
 * When the user borrows from the pool, they specify the capacity of the buffer to borrow, and a buffer of that exact
 * size is returned.
 */
public class ByteBufferPool implements Encodable {

    private final MutableIntObjectMap<List<ByteBuffer>> buffers;
    private final boolean direct;

    private int size;
    private int outstanding;

    /**
     * Constructs a {@code ByteBufferPool} with a boolean that specifies whether the buffers are direct or heap byte
     * buffers.
     *
     * @param direct true if the buffers are direct byte buffers, false if the buffers are heap byte buffers
     */
    public ByteBufferPool(boolean direct) {
        this.direct = direct;
        buffers = new IntObjectHashMap<>();
    }

    /**
     * Borrows a buffer from the pool with a capacity equal to the specified {@code size}.
     *
     * @param size the capacity of the buffer
     * @return the buffer
     * @throws IllegalArgumentException if the pool does not support the specified {@code size}
     */
    public ByteBuffer borrowBuffer(int size) {
        var theBuffers = buffers.get(size);
        if (theBuffers == null) {
            theBuffers = new CoreList<>();
            buffers.put(size, theBuffers);
        }

        if (theBuffers.isEmpty()) {
            this.size++;
            theBuffers.add(direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size));
        }

        outstanding++;
        return theBuffers.remove(theBuffers.size() - 1);
    }

    /**
     * Returns the buffer to the pool.
     *
     * @param buffer the buffer
     */
    public void returnBuffer(ByteBuffer buffer) {
        outstanding--;
        buffer.clear();
        buffers.get(buffer.capacity()).add(buffer);
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