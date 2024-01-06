package com.core.infrastructure.io;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.net.SocketOption;

/**
 * A selectable channel for datagram-oriented sockets.
 *
 * <p>A datagram channel is created by invoking the {@link Selector#createDatagramChannel()} method.
 * It is not possible to create a channel for an arbitrary, pre-existing datagram socket.
 * A newly-created datagram channel is open but not connected.
 * A datagram channel need not be connected in order for the {@link #send send} and {@link #receive receive} methods to
 * be used.
 * A datagram channel may be connected, by invoking its {@link #connect connect} method, in order to avoid the overhead
 * of the security checks are otherwise performed as part of every send and receive operation.
 * A datagram channel must be connected in order to use the {@link #read(MutableDirectBuffer, int, int) read} and
 * {@link #write(DirectBuffer, int, int) write} methods, since those methods do not accept or return socket addresses.
 *
 * <p>Once connected, a datagram channel remains connected until it is disconnected or closed.
 * Whether or not a datagram channel is connected may be determined by invoking its {@link #isConnected isConnected}
 * method.
 *
 * <p>Socket options are configured using the {@link #setOption(SocketOption,Object) setOption} method.
 * A datagram channel to an Internet Protocol socket supports the following options:
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
 *     <th scope="row">{@link java.net.StandardSocketOptions#SO_REUSEADDR SO_REUSEADDR}</th>
 *     <td>Re-use address</td>
 *   </tr>
 *   <tr>
 *     <th scope="row">{@link java.net.StandardSocketOptions#SO_BROADCAST SO_BROADCAST}</th>
 *     <td>Allow transmission of broadcast datagrams</td>
 *   </tr>
 *   <tr>
 *     <th scope="row">{@link java.net.StandardSocketOptions#IP_TOS IP_TOS}</th>
 *     <td>The Type of Service (ToS) octet in the Internet Protocol (IP) header</td>
 *   </tr>
 *   <tr>
 *     <th scope="row">{@link java.net.StandardSocketOptions#IP_MULTICAST_IF IP_MULTICAST_IF}</th>
 *     <td>The network interface for Internet Protocol (IP) multicast datagrams</td>
 *   </tr>
 *   <tr>
 *     <th scope="row">{@link java.net.StandardSocketOptions#IP_MULTICAST_TTL IP_MULTICAST_TTL}</th>
 *     <td>The <em>time-to-live</em> for Internet Protocol (IP) multicast datagrams</td>
 *   </tr>
 *   <tr>
 *     <th scope="row">{@link java.net.StandardSocketOptions#IP_MULTICAST_LOOP IP_MULTICAST_LOOP}</th>
 *     <td>Loopback for Internet Protocol (IP) multicast datagrams</td>
 *   </tr>
 * </tbody>
 * </table>
 * </blockquote>
 * Additional (implementation specific) options may also be supported.
 */
public interface DatagramChannel extends NetworkChannel, BufferChannel {

    /**
     * Receives a datagram via this channel.
     *
     * <p>If a datagram is immediately available, or if this channel is in blocking mode and one eventually becomes
     * available, then the datagram is copied into the given buffer and the number of bytes copied is returned.
     * If this channel is in non-blocking mode and a datagram is not immediately available then this method immediately
     * returns {@code 0}.
     *
     * <p>The datagram is transferred into the given buffer starting at the specified {@code index}, as if by a regular
     * {@link ReadableBufferChannel#read(MutableDirectBuffer, int, int) read} operation.
     * If there are fewer bytes remaining in the buffer than are required to hold the datagram then the remainder of the
     * datagram is silently discarded.
     *
     * @param buffer the buffer into which the datagram is to be transferred
     * @param index the first byte of the buffer to be transferred
     * @param length the maximum number of bytes to be tranferred
     * @return the number of bytes read
     * @throws IOException if an I/O error occurs
     */
    int receive(MutableDirectBuffer buffer, int index, int length) throws IOException;

    /**
     * Equivalent to {@link #receive(MutableDirectBuffer, int, int)}  receive(buffer, 0, buffer.capacity())}.
     *
     * @param buffer the buffer into which bytes are to be transferred
     * @return the number of bytes read, possibly zero, or {@code -1} if the channel has reached end-of-stream
     * @throws IOException if an I/O error occurs
     */
    default int receive(MutableDirectBuffer buffer) throws IOException {
        return receive(buffer, 0, buffer.capacity());
    }

    /**
     * Sends a datagram via this channel.
     *
     * <p>If this channel is in non-blocking mode and there is sufficient room in the underlying output buffer, or if
     * this channel is in blocking mode and sufficient room becomes available, then the remaining bytes in the given
     * buffer are transmitted as a single datagram to the given target address.
     *
     * <p>The datagram is transferred from the byte buffer as if by a regular
     * {@link WritableBufferChannel#write(DirectBuffer, int, int) write} operation.
     *
     * @param buffer the buffer containing the datagram to be sent
     * @param index the first byte of the buffer to be sent
     * @param length the number of bytes in the buffer to send
     * @param address the address to which the datagram is to be sent
     * @return the number of bytes sent, which will be either the number of bytes that were remaining in the source
     *     buffer when this method was invoked or, if this channel is non-blocking, may be zero if there was
     *     insufficient room for the datagram in the underlying output buffer
     * @throws IOException if an I/O error occurs
     */
    int send(DirectBuffer buffer, int index, int length, String address) throws IOException;

    /**
     * Equivalent to {@link #send(DirectBuffer, int, int, String)}  send(buffer, 0, buffer.capacity(), address)}.
     *
     * @param buffer the buffer containing the datagram to be sent
     * @param address the address to which the datagram is to be sent
     * @return the number of bytes sent, which will be either the number of bytes that were remaining in the source
     *     buffer when this method was invoked or, if this channel is non-blocking, may be zero if there was
     *     insufficient room for the datagram in the underlying output buffer
     * @throws IOException if an I/O error occurs
     */
    default int send(DirectBuffer buffer, String address) throws IOException {
        return send(buffer, 0, buffer.capacity(), address);
    }

    /**
     * Sends a datagram via this channel to the last address that sent a datagram to this channel that was read with
     * {@link #receive(MutableDirectBuffer, int, int) receive}.
     *
     * <p>If this channel is in non-blocking mode and there is sufficient room in the underlying output buffer, or if
     * this channel is in blocking mode and sufficient room becomes available, then the remaining bytes in the given
     * buffer are transmitted as a single datagram to the given target address.
     *
     * <p>The datagram is transferred from the byte buffer as if by a regular
     * {@link WritableBufferChannel#write(DirectBuffer, int, int) write} operation.
     *
     * <p>An I/O error will be sent if {@link #receive(MutableDirectBuffer, int, int) receive} has never been invoked.
     *
     * @param buffer the buffer containing the datagram to be sent
     * @param index the first byte of the buffer to be sent
     * @param length the number of bytes in the buffer to send
     * @return the number of bytes sent, which will be either the number of bytes that were remaining in the source
     *     buffer when this method was invoked or, if this channel is non-blocking, may be zero if there was
     *     insufficient room for the datagram in the underlying output buffer
     * @throws IOException if an I/O error occurs or {@code receive} has never been invoked
     */
    int reply(DirectBuffer buffer, int index, int length) throws IOException;

    /**
     * Connects this channel's socket.
     *
     * <p>The channel's socket is configured so that it only receives datagrams from, and sends datagrams to, the given
     * remote <i>peer</i> address.
     * Once connected, datagrams may not be received from or sent to any other address.
     * A datagram socket remains connected until it is explicitly disconnected or until it is closed.
     *
     * <p>This method may be invoked at any time.
     * It will not have any effect on read or write operations that are already in progress at the moment that it is
     * invoked.
     * If this channel's socket is not bound then this method will first cause the socket to be bound to an address that
     * is assigned automatically, as if invoking the {@link #bind bind} method with a parameter of {@code null}.
     *
     * @param address the remote address to which this channel is to be connected
     * @throws IOException if an I/O error occurs
     */
    void connect(String address) throws IOException;

    /**
     * Disconnects this channel's socket.
     *
     * <p>The channel's socket is configured so that it can receive datagrams from, and sends datagrams to, any remote
     * address.
     *
     * <p>This method may be invoked at any time.
     * It will not have any effect on read or write operations that are already in progress at the moment that it is
     * invoked.
     *
     * <p>If this channel's socket is not connected, or if the channel is closed, then invoking this method has no
     * effect.
     *
     * @throws IOException if an I/O error occurs
     */
    void disconnect() throws IOException;

    /**
     * Joins a multicast group to begin receiving all datagrams sent to the group.
     *
     * <p>If this channel is currently a member of the group on the given interface to receive all datagrams then
     * invoking this method has no effect.
     * Otherwise this channel joins the group.
     *
     * <p>A multicast channel may join several multicast groups, including the same group on more than one interface.
     * An implementation may impose a limit on the number of groups that may be joined at the same time.
     *
     * @param address the multicast address to join*
     * @throws IOException if an I/O error occurs
     */
    void join(String address) throws IOException;

    /**
     * Returns the remote address to which this channel's socket is connected.
     *
     * @return the remote address; {@code null} if the channel's socket is not connected
     */
    String getRemoteAddress();

    /**
     * Returns the remote address to which this channel last received a datagram with the
     * {@link #receive(MutableDirectBuffer, int, int) receive} method.
     *
     * @return the remote address; {@code null} if the channel's socket is not connected
     */
    String getLastReceiveAddress();

    /**
     * Returns whether or not this channel's socket is connected.
     *
     * @return {@code true} if, and only if, this channel's socket is {@link #isOpen open} and connected
     */
    boolean isConnected();
}
