package com.core.infrastructure.io;

import java.io.IOException;
import java.net.SocketOption;
import java.nio.channels.Channel;
import java.util.Set;

/**
 * A channel to a network socket.
 *
 * <p>A channel that implements this interface is a channel to a network socket.
 * The {@link #bind(String) bind} method is used to bind the socket to a local address and the
 * {@link #getLocalAddress() getLocalAddress} method returns the address that the socket is bound to.
 */
public interface NetworkChannel extends Channel, AsynchronousChannel {

    /**
     * Binds the channel's socket to a local address.
     *
     * <p>This method is used to establish an association between the socket and a local address.
     * Once an association is established then the socket remains bound until the channel is closed.
     * If the {@code address} parameter has the value {@code null} then the socket will be bound to an address that is
     * assigned automatically.
     *
     * @param address the address to bind the socket, or {@code null} to bind the socket to an automatically assigned
     *     socket address
     * @throws IOException if an I/O error occurs
     * @see #getLocalAddress
     */
    void bind(String address) throws IOException;

    /**
     * Returns the socket address that this channel's socket is bound to.
     *
     * @return the socket address that the socket is bound to, or {@code null} if the channel's socket is not bound
     */
    String getLocalAddress();

    /**
     * Sets the value of a socket option.
     *
     * @param <T> the type of the socket option value
     * @param name the socket option
     * @param value the value of the socket option; a value of {@code null} may be a valid value for some socket options
     * @throws UnsupportedOperationException if the socket option is not supported by this channel
     * @throws IllegalArgumentException if the value is not a valid value for this socket option
     * @throws IOException if an I/O error occurs or the channel is closed
     * @see java.net.StandardSocketOptions
     */
    <T> void setOption(SocketOption<T> name, T value) throws IOException;

    /**
     * Returns the value of a socket option.
     *
     * @param <T> the type of the socket option value
     * @param name the socket option
     * @return the value of the socket option; a value of {@code null} may be a valid value for some socket options
     * @throws UnsupportedOperationException if the socket option is not supported by this channel
     * @throws IOException if an I/O error occurs or the channel is closed
     *
     * @see java.net.StandardSocketOptions
     */
    <T> T getOption(SocketOption<T> name) throws IOException;

    /**
     * Returns a set of the socket options supported by this channel.
     *
     * <p>This method will continue to return the set of options even after the channel has been closed.
     *
     * @return a set of the socket options supported by this channel
     */
    Set<SocketOption<?>> supportedOptions();
}
