package com.core.infrastructure.io;

/**
 * An extension of {@code SocketChannel} that supports communicating over TLS.
 */
public interface SslSocketChannel extends SocketChannel {

    /**
     * TLS version 1.2.
     */
    String TlsV12 = "TLSv1.2";

    void setHandshakeCompleteListener(Runnable listener);

    void setConnectionFailedListener(Runnable listener);

    void setIoListener(IoListener listener);

    /**
     * Returns true if the SSL handshake is complete.
     *
     * @return true if the SSL handshake is complete
     */
    boolean isHandshakeComplete();

    /**
     * Returns true if the socket is currently executing the SSL handshake.
     *
     * @return true if the socket is currently executing the SSL handshake
     */
    boolean isHandshaking();

    String getConnectionFailedReason();

    Exception getConnectionFailedException();
}
