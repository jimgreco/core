package com.core.infrastructure.io;

import java.io.IOException;
import java.net.SocketOption;

/**
 * A selectable channel for stream-oriented connecting sockets.
 *
 * <p>A socket channel is created by invoking the {@link Selector#createSocketChannel()} method.
 * It is not possible to create a channel for an arbitrary, pre-existing socket.
 * A newly-created socket channel is open but not yet connected.
 * An attempt to invoke an I/O operation upon an unconnected channel will cause an {@link IOException} to be thrown.
 * A socket channel can be connected by invoking its {@link #connect connect} method; once connected, a socket channel
 * remains connected until it is closed.
 * Whether or not a socket channel is connected may be determined by invoking its {@link #isConnected isConnected}
 * method.
 *
 * <p>Socket channels support <i>non-blocking connection:</i>&nbsp; A socket channel may be created and the process of
 * establishing the link to the remote socket may be initiated via the {@link #connect connect} method for later
 * completion by the {@link #finishConnect finishConnect} method.
 * Whether or not a connection operation is in progress may be determined by invoking the
 * {@link #isConnectionPending isConnectionPending} method.
 *
 * <p>Socket options are configured using the {@link #setOption(SocketOption, Object) setOption} method.
 * Socket channels support the following options:
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
 *     <th scope="row">{@link java.net.StandardSocketOptions#SO_SNDBUF SO_SNDBUF}</th>
 *     <td>The size of the socket send buffer</td>
 *   </tr>
 *   <tr>
 *     <th scope="row">{@link java.net.StandardSocketOptions#SO_RCVBUF SO_RCVBUF}</th>
 *     <td>The size of the socket receive buffer </td>
 *   </tr>
 *   <tr>
 *     <th scope="row">{@link java.net.StandardSocketOptions#SO_KEEPALIVE SO_KEEPALIVE}</th>
 *     <td>Keep connection alive </td>
 *   </tr>
 *   <tr>
 *     <th scope="row">{@link java.net.StandardSocketOptions#SO_REUSEADDR SO_REUSEADDR}</th>
 *     <td>Re-use address</td>
 *   </tr>
 *   <tr>
 *     <th scope="row">{@link java.net.StandardSocketOptions#SO_LINGER SO_LINGER}</th>
 *     <td>Linger on close if data is present (when configured in blocking mode only)</td>
 *   </tr>
 *   <tr>
 *     <th scope="row">{@link java.net.StandardSocketOptions#TCP_NODELAY TCP_NODELAY}</th>
 *     <td>Disable the Nagle algorithm</td>
 *   </tr>
 * </tbody>
 * </table>
 * </blockquote>
 * Additional (implementation specific) options may also be supported.
 */
public interface SocketChannel extends NetworkChannel, BufferChannel {

    /**
     * Returns whether or not this channel's network socket is connected.
     *
     * @return {@code true} if, and only if, this channel's network socket is {@link #isOpen open} and connected
     */
    boolean isConnected();

    /**
     * Returns whether or not a connection operation is in progress on this channel.
     *
     * @return {@code true} if, and only if, a connection operation has been initiated on this channel but not yet
     *     completed by invoking the {@link #finishConnect finishConnect} method
     */
    boolean isConnectionPending();

    /**
     * Returns the remote address to which this channel's socket is connected.
     *
     * @return the remote address; {@code null} if the channel's socket is not connected
     */
    String getRemoteAddress();

    /**
     * Connects this channel's socket.
     *
     * <p>If this channel is in non-blocking mode then an invocation of this method initiates a non-blocking connection
     * operation.
     * If the connection is established immediately, as can happen with a local connection, then this method returns
     * {@code true}.
     * Otherwise this method returns {@code false} and the connection operation must later be completed by invoking the
     * {@link #finishConnect finishConnect} method.
     *
     * <p>If this channel is in blocking mode then an invocation of this method will block until the connection is
     * established or an I/O error occurs.
     *
     * <p>This method may be invoked at any time.
     * If a read or write operation upon this channel is invoked while an invocation of this method is in progress then
     * that operation will first block until this invocation is complete.
     * If a connection attempt is initiated but fails, that is, if an invocation of this method throws a checked
     * exception, then the channel will be closed.
     *
     * @param address the remote address to which this channel is to be connected
     * @return {@code true} if a connection was established, {@code false} if this channel is in non-blocking mode and
     *     the connection operation is in progress
     * @throws IOException if an I/O error occurs
     */
    boolean connect(String address) throws IOException;

    /**
     * Finishes the process of connecting a socket channel.
     *
     * <p>A non-blocking connection operation is initiated by placing a socket channel in non-blocking mode and then
     * invoking its {@link #connect connect} method.
     * Once the connection is established, or the attempt has failed, the socket channel will become connectable and
     * this method may be invoked to complete the connection sequence.
     * If the connection operation failed then invoking this method will cause an appropriate {@link IOException} to be
     * thrown.
     *
     * <p>If this channel is already connected then this method will not block and will immediately return {@code true}.
     * If this channel is in non-blocking mode then this method will return {@code false} if the connection process is
     * not yet complete.
     * If this channel is in blocking mode then this method will block until the connection either completes or fails,
     * and will always either return {@code true} or throw a checked exception describing the failure.
     *
     * <p>This method may be invoked at any time.
     * If a read or write operation upon this channel is invoked while an invocation of this method is in progress then
     * that operation will first block until this invocation is complete.
     * If a connection attempt fails, that is, if an invocation of this method throws a checked exception, then the
     * channel will be closed.
     *
     * @return {@code true} if, and only if, this channel's socket is now connected
     * @throws IOException if an I/O error occurs
     */
    boolean finishConnect() throws IOException;

    /**
     * Sets a {@code listener} to be invoked when a client can complete the connection to a remote peer by invoking
     * {@link #finishConnect}.
     * The listener will be repeatedly invoked until the connection is complete.
     * Setting the listener to null will clear the listener.
     *
     * @param listener the listener
     * @throws IllegalStateException if the {@code listener} is not null and the channel has been closed
     */
    void setConnectListener(Runnable listener);
}
