package com.core.infrastructure.io;

/**
 * A pair of channels that implements a unidirectional pipe.
 *
 * <p>A pipe consists of a pair of channels: A writable {@link WritableBufferChannel sink} channel and a readable
 * {@link ReadableBufferChannel source} channel.
 * Once some bytes are written to the sink channel they can be read from the source channel in exactly the order in
 * which they were written.
 *
 * <p>Whether or not a thread writing bytes to a pipe will block until another thread reads those bytes, or some
 * previously-written bytes, from the pipe is system-dependent and therefore unspecified.
 * Many pipe implementations will buffer up to a certain number of bytes between the sink and source channels,
 * but such buffering should not be assumed.
 */
public interface Pipe {

    /**
     * Returns this pipe's source channel.
     *
     * @return this pipe's source channel
     */
    SourceChannel getSource();

    /**
     * Returns this pipe's sink channel.
     *
     * @return this pipe's sink channel
     */
    SinkChannel getSink();

    /**
     * A pipe channel that is readable.
     */
    interface SourceChannel extends ReadableBufferChannel, AsynchronousChannel {
    }

    /**
     * A pipe channel that is writable.
     */
    interface SinkChannel extends WritableBufferChannel, AsynchronousChannel {
    }
}
