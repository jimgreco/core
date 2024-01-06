package com.core.infrastructure.io;

import java.io.IOException;
import java.nio.channels.IllegalBlockingModeException;

/**
 * A channel that can send and receive data asynchronously.
 *
 * <p>A channel can be set to send and receive data synchronously (the default) by setting the
 * {@link #configureBlocking(boolean) configureBlocking} to true.
 * Data can be sent and received asynchronously by setting {@code configureBlocking} to false.
 */
public interface AsynchronousChannel {

    /**
     * Adjusts this channel's blocking mode.
     *
     * <p>If this channel is registered with one or more selectors then an attempt to place it into blocking mode will
     * cause an {@link IllegalBlockingModeException} to be thrown.
     *
     * <p>This method may be invoked at any time.
     * The new blocking mode will only affect I/O operations that are initiated after this method returns.
     * For some implementations this may require blocking until all pending I/O operations are complete.
     *
     * @param block if {@code true} then this channel will be placed in blocking mode; if {@code false} then it will be
     *     placed non-blocking mode
     * @throws IOException if an I/O error occurs
     * @throws UnsupportedOperationException if the channel does not support configuring blocking more
     */
    void configureBlocking(boolean block) throws IOException;

    /**
     * Tells whether or not every I/O operation on this channel will block until it completes.
     * A newly-created channel is always in blocking mode.
     *
     * <p>If this channel is closed then the value returned by this method is not specified.
     *
     * @return {@code true} if, and only if, this channel is in blocking mode
     */
    boolean isBlocking();
}
