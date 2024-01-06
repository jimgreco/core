package com.core.infrastructure.io;

/**
 * A channel that can read and write buffers.
 * This interface simply unifies {@link ReadableBufferChannel} and {@link WritableBufferChannel};
 * it does not specify any new operations.
 */
public interface BufferChannel extends ReadableBufferChannel, WritableBufferChannel {
}
