package com.core.infrastructure.io;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

/**
 * A multiplexor of selectable objects.
 *
 * <p>A selection operation queries the underlying operating system for an update as to the readiness of each registered
 * channel.
 */
public interface Selector extends Closeable {

    /**
     * Creates an opens a pipe.
     *
     * @return the new pipe
     * @throws IOException if an I/O error occurs
     */
    Pipe createPipe() throws IOException;

    /**
     * Creates and opens a datagram channel.
     *
     * @return a new datagram channel
     * @throws IOException if an I/O error occurs
     */
    DatagramChannel createDatagramChannel() throws IOException;

    /**
     * Creates and opens a socket channel.
     *
     * @return a new socket channel
     * @throws IOException if an I/O error occurs
     */
    SocketChannel createSocketChannel() throws IOException;

    /**
     * Creates and opens an SSL socket channel.
     *
     * @return a new socket channel
     * @throws IOException if an I/O error occurs
     * @throws NoSuchAlgorithmException if the default SSL algorithm does not exist
     * @throws KeyManagementException if there is an error with the SSL key manager
     */
    SslSocketChannel createSslSocketChannel() throws IOException, NoSuchAlgorithmException, KeyManagementException;

    /**
     * Creates and opens an SSL socket channel with the specified SSL context.
     *
     * @param sslContext the SSL context
     * @return a new socket channel
     * @throws IOException if an I/O error occurs
     */
    SslSocketChannel createSslSocketChannel(SSLContext sslContext) throws IOException;

    /**
     * Creates and opens a server-socket channel.
     *
     * <p>The new channel's socket is initially unbound; it must be bound to a specific address via the
     * {@link ServerSocketChannel#bind(String)} bind} method before connections can be accepted.
     *
     * @return a new server socket channel
     * @throws IOException if an I/O error occurs
     */
    ServerSocketChannel createServerSocketChannel() throws IOException;

    /**
     * Selects a set of channels who are ready for I/O operations.
     *
     * <p>This method performs a non-blocking selection operation.
     *
     * @throws IOException if the channel is closed or an I/O error occurs
     */
    void selectNow() throws IOException;

    /**
     * Selects a set of channels that are ready for I/O operations.
     *
     * <p>This method performs a blocking selection operation.
     * It returns only after at least one channel is selected, the current thread is interrupted, or
     * the given timeout period expires, whichever comes first.
     *
     * <p>This method does not offer real-time guarantees: It schedules the timeout as if by invoking the
     * {@link Object#wait(long)} method
     *
     * @param timeout if positive, block for up to {@code timeout} nanoseconds, more or less, while waiting for a
     *     channel to become ready; if zero, block indefinitely; must not be negative
     *
     * @throws IOException if the channel is closed or an I/O error occurs
     * @throws IllegalArgumentException if the value of the timeout argument is negative
     */
    void select(long timeout) throws IOException;

    /**
     * Selects a set of channels that are ready for I/O operations.
     *
     * <p>This method performs a blocking <a href="#selop">selection operation</a>.
     * It returns only after at least one channel is selected or the current thread is interrupted, whichever comes
     * first.
     *
     * @throws IOException if the channel is closed or an I/O error occurs
     */
    void select() throws IOException;

    /**
     * Closes this selector.
     *
     * <p>Any channels created through this selector are deregistered, and any other resources associated with this
     * selector are released.
     *
     * <p>If this selector is already closed then invoking this method has no effect.
     *
     * <p>After a selector is closed, any further attempt to use it, except by invoking this method, will cause a
     * {@link IOException} to be thrown.
     *
     * @throws IOException if an I/O error occurs
     */
    void close() throws IOException;
}
