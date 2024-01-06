package com.core.infrastructure.io;

import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.DirectBuffer;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

/**
 * A channel that can write buffers.
 **/
public interface WritableBufferChannel extends GatheringByteChannel {

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     *
     * <p>An attempt is made to write up to {@code length} bytes to the channel
     *
     * <p>Suppose that a byte sequence of length <i>n</i> is written, where {@code 0}&nbsp;{@code <=}&nbsp;<i>n</i>
     * &nbsp;{@code <=}&nbsp;<i>{@code length}</i>.
     * This byte sequence will be transferred from the buffer starting at index <i>p</i>, where <i>p</i> is the buffer's
     * position at the moment this method is invoked; the index of the last byte written will be <i>p</i>&nbsp;{@code +}
     * &nbsp;<i>n</i>&nbsp;{@code -}&nbsp;{@code 1}.
     *
     * <p>Unless otherwise specified, a write operation will return only after writing all of the {@code length}
     * requested bytes.
     * Some types of channels, depending upon their state, may write only some of the bytes or possibly none at all.
     * A socket channel in non-blocking mode, for example, cannot write any more bytes than are free in the socket's
     * output buffer.
     *
     * @param buffer the buffer from which bytes are to be retrieved
     * @param index the first byte of the buffer from which bytes are to be retrieved
     * @param length the maximum number of bytes which are to be written
     * @return the number of bytes written, possibly zero
     * @throws IOException if an I/O error occurs
     */
    default int write(DirectBuffer buffer, int index, int length) throws IOException {
        return write(BufferUtils.byteBuffer(buffer, index, length));
    }

    /**
     * Equivalent to {@link #write(DirectBuffer, int, int) write(buffer, 0, buffer.capacity())}.
     *
     * @param buffer the buffer from which bytes are to be retrieved
     * @return the number of bytes written, possibly zero
     * @throws IOException if an I/O error occurs
     */
    default int write(DirectBuffer buffer) throws IOException {
        return write(BufferUtils.byteBuffer(buffer, 0, buffer.capacity()));
    }

    /**
     * Sets a {@code listener} to be invoked when the channel is available to write.
     * The listener will be repeatedly invoked until the channel is no longer available to write.
     * Setting the listener to null will clear the listener.
     *
     * @param listener the listener
     */
    void setWriteListener(Runnable listener);
}
