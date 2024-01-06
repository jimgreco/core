package com.core.platform.io;

import com.core.infrastructure.io.IoListener;

/**
 * A marker interface for higher-level protocols that use an SSL socket.
 */
public interface SslProtocolClient {

    /**
     * Returns true if the protocol is connected.
     *
     * <p>Typically, this means the socket is connected, the SSL handshake is complete, and any higher-level protocol
     * initialization is complete.
     *
     * @return true if the protocol is connected
     */
    boolean isConnected();

    /**
     * Returns the exception thrown that caused the connection to fail.
     *
     * @return the exception thrown that caused the connection to fail
     */
    Exception getConnectionFailedException();

    /**
     * Returns the reason the connection failed.
     *
     * @return the reason the connection failed
     */
    String getConnectionFailedReason();

    /**
     * Sets a listener for I/O events.
     *
     * @param listener a listener for I/O events
     */
    void setIoListener(IoListener listener);

    /**
     * Sets the size of the write buffer.
     *
     * @param size the size of the write buffer
     */
    void setWriteBufferSize(int size);

    /**
     * Sets the side of the read buffer.
     *
     * @param size the side of the read buffer
     */
    void setReadBufferSize(int size);

    /**
     * Sets a listener to be invoked when the protocol is connected and ready for application use.
     *
     * <p>Typically, this means the socket is connected, the SSL handshake is complete, and any higher-level protocol
     * initialization is complete.
     *
     * @param listener the listener
     */
    void setConnectedListener(Runnable listener);

    /**
     * Sets a listener to be invoked when the protocol connect fails.
     *
     * <p>A socket can fail for a varietal of reasons including counterparty disconnect, an exception thrown during
     * I/O, protocol instructions (e.g., Logout), or a timeout on connection attempt.
     * The reason and exception are described with {@link #getConnectionFailedReason()} and
     * {@link #getConnectionFailedException()}.
     * If no reason is specified then the user can assume a counterparty disconnect.
     *
     * <p>The user should invoke {@link #close()} to reset the state of the socket and begin reconnection, if
     * applicable.
     *
     * @param listener the listener
     */
    void setConnectionFailedListener(Runnable listener);

    /**
     * Sets the timeout, in nanoseconds, before the protocol will give up its connection attempt.
     *
     * @param connectTimeout the timeout
     */
    void setConnectTimeout(long connectTimeout);

    /**
     * Sets the timeout, in nanoseconds, before the protocol will attempt to reconnect to the server
     * after {@code close} is invoked.
     *
     * @param reconnectTimeout the timeout
     */
    void setReconnectTimeout(long reconnectTimeout);

    /**
     * Disables reconnection attempts.
     */
    void disableReconnect();

    /**
     * Enables a reconnection attempt after {@code close} is invoked.
     */
    void enableReconnect();

    /**
     * Closes the protocol.
     *
     * <p>Typically, this involves closing the underlying socket and resetting the state.
     */
    void close();
}
