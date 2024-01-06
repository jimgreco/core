package com.core.infrastructure.io;

import java.io.IOException;
import java.net.SocketOption;

/**
 * A selectable channel for stream-oriented listening sockets.
 *
 * <p>A server-socket channel is created by invoking the {@link Selector#createServerSocketChannel()} method.
 * It is not possible to create a channel for an arbitrary, pre-existing server socket.
 * A newly-created server-socket channel is open but not yet bound.
 * An attempt to invoke the {@link #accept() accept} method of an unbound server-socket channel will cause an
 * {@link IOException} to be thrown.
 * A server-socket channel can be bound by invoking one of the {@link #bind(String) bind} methods
 * defined by this class.
 *
 * <p>Socket options are configured using the {@link #setOption(SocketOption,Object) setOption} method.
 * Server-socket channels support the following options:
 * <blockquote>
 * <table class="striped">
 * <caption style="display:none">Socket options</caption>
 * <thead>
 *   <tr>
 *     <th scope="col">Option Name</th>
 *     <th scope="col">Description</th>
 *   </tr>
 * </thead>
 * <tbody>
 *   <tr>
 *     <th scope="row">{@link java.net.StandardSocketOptions#SO_RCVBUF SO_RCVBUF}</th>
 *     <td>The size of the socket receive buffer</td>
 *   </tr>
 *   <tr>
 *     <th scope="row">{@link java.net.StandardSocketOptions#SO_REUSEADDR SO_REUSEADDR}</th>
 *     <td>Re-use address</td>
 *   </tr>
 * </tbody>
 * </table>
 * </blockquote>
 * Additional (implementation specific) options may also be supported.
 */
public interface ServerSocketChannel extends NetworkChannel {

    /**
     * Accepts a connection made to this channel's socket.
     *
     * <p>If this channel is in non-blocking mode then this method will immediately return {@code null} if there are no
     * pending connections.
     * Otherwise it will block indefinitely until a new connection is available or an I/O error occurs.
     *
     * <p>The socket channel returned by this method, if any, will be in the blocking mode of this channel.
     *
     * @return the socket channel for the new connection, or {@code null} if this channel is in non-blocking mode and no
     *     connection is available to be accepted
     *
     * @throws IOException if an I/O error occurs
     */
    SocketChannel accept() throws IOException;

    /**
     * Sets a {@code listener} to be invoked when a client is available to be accepted.
     * The listener will be repeatedly invoked until all clients have been accepted.
     * Setting the listener to null will clear the listener.
     *
     * @param listener the listener
     * @throws IllegalStateException if the {@code listener} is not null and the channel has been closed
     */
    void setAcceptListener(Runnable listener);
}
