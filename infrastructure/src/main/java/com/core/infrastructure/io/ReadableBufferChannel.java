package com.core.infrastructure.io;

import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.nio.channels.ScatteringByteChannel;

/**
 * A channel that can read buffers.
 **/
public interface ReadableBufferChannel extends ScatteringByteChannel {

    /**
     * Reads a sequence of bytes from this channel into the given buffer.
     *
     * <p>An attempt is made to read up to <i>r</i> bytes from the channel, where <i>r</i> is the number of bytes
     * remaining in the buffer after the {@code index}, that is {@code buffer.capacity() - index}.
     *
     * <p>Suppose that a byte sequence of length <i>n</i> is read, where {@code 0}&nbsp;{@code <=}&nbsp;<i>n</i>&nbsp;
     * {@code <=}&nbsp;<i>r</i>.
     * This byte sequence will be transferred into the buffer so that the first byte in the sequence is at {@code index}
     * and the last byte is at {@code index}&nbsp;{@code +}&nbsp;<i>n</i>&nbsp;{@code -}&nbsp;{@code 1}.
     *
     * <p>A read operation might not fill the buffer, and in fact it might not read any bytes at all.
     * Whether or not it does so depends upon the nature and state of the channel.
     * A socket channel in non-blocking mode, for example, cannot read any more bytes than are immediately available
     * from the socket's input buffer; similarly, a file channel cannot read any more bytes than remain in the file.
     * It is guaranteed, however, that if a channel is in blocking mode and there is at least one byte remaining in the
     * buffer then this method will block until at least one byte is read.
     *
     * @param buffer the buffer into which bytes are to be transferred
     * @param index the first byte of the buffer into which bytes are to be transferred
     * @param length the maximum number of bytes to be transferred
     * @return the number of bytes read, possibly zero, or {@code -1} if the channel has reached end-of-stream
     * @throws IOException if an I/O error occurs
     */
    default int read(MutableDirectBuffer buffer, int index, int length) throws IOException {
        return read(BufferUtils.byteBuffer(buffer, index, length));
    }

    /**
     * Equivalent to {@link #read(MutableDirectBuffer, int, int) read(buffer, 0, buffer.capacity())}.
     *
     * @param buffer the buffer into which bytes are to be transferred
     * @return the number of bytes read, possibly zero, or {@code -1} if the channel has reached end-of-stream
     * @throws IOException if an I/O error occurs
     */
    default int read(MutableDirectBuffer buffer) throws IOException {
        return read(BufferUtils.byteBuffer(buffer, 0, buffer.capacity()));
    }

    /**
     * Sets a {@code listener} to be invoked when data is available to be read.
     * The listener will be repeatedly invoked until the channel no longer has data available to be read.
     * Setting the listener to null will clear the listener.
     *
     * @param listener the listener
     */
    void setReadListener(Runnable listener);
}
