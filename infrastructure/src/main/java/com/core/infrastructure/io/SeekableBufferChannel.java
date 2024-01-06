package com.core.infrastructure.io;

import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

/**
 * A buffer channel that maintains a current <i>position</i> and allows the position to be changed.
 *
 * <p>A seekable byte channel is connected to an entity, typically a file, that contains a variable-length sequence of
 * bytes that can be read and written.
 * The current position can be {@link #position() <i>queried</i>} and {@link #position(long) <i>modified</i>}.
 * The channel also provides access to the current <i>size</i> of the entity to which the channel is connected.
 * The size increases when bytes are written beyond its current size; the size decreases when it is
 * {@link #truncate <i>truncated</i>}.
 */
public interface SeekableBufferChannel extends SeekableByteChannel, BufferChannel {

    /**
     * Reads a sequence of bytes from this channel into the given buffer.
     *
     * <p>Bytes are read starting at this channel's current position, and then the position is updated with the number
     * of bytes actually read.
     * Otherwise this method behaves exactly as specified in the {@link ReadableBufferChannel} interface.
     */
    @Override
    default int read(MutableDirectBuffer buffer, int index, int length) throws IOException {
        return read(BufferUtils.byteBuffer(buffer, index, length));
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffer, starting at the given file position.
     *
     * <p>This method works in the same manner as the {@link #read(MutableDirectBuffer, int, int) read} method, except
     * that bytes are read starting at the given file position rather than at the channel's current position.
     * This method does not modify this channel's position.
     * If the given position is greater than the file's current size then no bytes are read.
     *
     * @param position the file position at which the transfer is to begin; must be non-negative
     * @param buffer the buffer into which bytes are to be transferred
     * @param index the first byte of the buffer into which bytes are to be transferred
     * @param length the maximum number of bytes to be transferred
     * @return the number of bytes read, possibly zero, or {@code -1} if the given position is greater than or equal to
     *     the file's current size
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if the position is negative
     */
    int read(long position, MutableDirectBuffer buffer, int index, int length) throws IOException;

    /**
     * Equivalent to {@link #read(long, MutableDirectBuffer, int, int) read(position, buffer, 0, buffer.capacity())}.
     *
     * @param position the file position at which the transfer is to begin; must be non-negative
     * @param buffer the buffer into which bytes are to be transferred
     * @return the number of bytes read, possibly zero, or {@code -1} if the channel has reached end-of-stream
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if the position is negative
     */
    default int read(long position, MutableDirectBuffer buffer) throws IOException {
        return read(position, buffer, 0, buffer.capacity());
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     *
     * <p>Bytes are written starting at this channel's current position.
     * The entity to which the channel is connected is grown, if necessary, to accommodate the written bytes, and then
     * the position is updated with the number of bytes actually written.
     * Otherwise this method behaves exactly as specified by the {@link WritableBufferChannel} interface.
     */
    @Override
    int write(DirectBuffer buffer, int index, int length) throws IOException;

    /**
     * Equivalent to {@link #write(long, DirectBuffer, int, int) write(position, buffer, 0, buffer.capacity())}.
     *
     * @param position the file position at which the transfer is to begin; must be non-negative
     * @param buffer the buffer from which bytes are to be retrieved
     * @return the number of bytes written, possibly zero
     * @throws IOException if an I/O error occurs
     */
    default int write(long position, DirectBuffer buffer) throws IOException {
        return write(position, buffer, 0, buffer.capacity());
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer, starting at the given file position.
     *
     * <p>This method works in the same manner as the {@link #write(DirectBuffer, int, int) write} method, except that
     * bytes are written starting at the given file position rather than at the channel's current position.
     * This method does not modify this channel's position.
     * If the given position is greater than the file's current size then the file will be grown to accommodate the new
     * bytes; the values of any bytes between the previous end-of-file and the newly-written bytes are unspecified.
     *
     * @param position the file position at which the transfer is to begin; must be non-negative
     * @param buffer the buffer from which bytes are to be transferred
     * @param index the first byte of the buffer from which bytes are to be retrieved
     * @param length the maximum number of bytes which are to be written
     * @return the number of bytes written, possibly zero
     * @throws IllegalArgumentException if the position is negative
     * @throws IOException if an I/O error occurs
     */
    int write(long position, DirectBuffer buffer, int index, int length) throws IOException;
}
