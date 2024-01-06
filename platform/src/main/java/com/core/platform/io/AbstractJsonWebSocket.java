package com.core.platform.io;

import com.core.infrastructure.Json;
import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.encoding.EncoderUtils;
import com.core.infrastructure.encoding.MutableObjectEncoder;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.IoListener;
import com.core.infrastructure.io.SocketChannel;
import com.core.infrastructure.time.Scheduler;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.nio.channels.NotYetConnectedException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * An abstract class containing common functionality for WebSockets to communicate over JSON.
 *
 * @see JsonWebSocketClient
 * @see JsonWebSocketServerClient
 */
public abstract class AbstractJsonWebSocket {

    protected static final long PING_DURATION = TimeUnit.SECONDS.toNanos(1);
    protected static final long PONG_DURATION = TimeUnit.SECONDS.toNanos(10);


    protected final Runnable cachedPing;
    protected final Runnable cachedPong;
    protected final Scheduler scheduler;
    protected final Json json;
    protected final MutableObjectEncoder jsonEncoder;
    protected final Runnable cachedRead;
    protected final byte[] mask;
    protected Runnable connectionFailedListener;
    protected IoListener ioListener;
    protected long pingTaskId;
    protected long pongTaskId;
    protected Exception connectionFailedException;
    protected String connectionFailedReason;
    protected boolean enablePing;
    protected boolean enableUnsolicitedPong;
    protected Consumer<Json.Value> readListener;
    protected MutableDirectBuffer readBuffer;
    protected MutableDirectBuffer writeBuffer;
    protected int readBufferLength;
    protected int writeBufferLength;

    /**
     * Creates an instance of the {@code AbstractJsonWebSocket} with the specified initial read and write buffer sizes.
     *
     * @param scheduler a scheduler of events for ping/pong
     * @param readBufferSize the initial read buffer size
     * @param writeBufferSize the initial write buffer size
     */
    protected AbstractJsonWebSocket(Scheduler scheduler, int readBufferSize, int writeBufferSize) {
        this.scheduler = scheduler;
        cachedPing = this::ping;
        cachedPong = this::unsolicitedPong;
        cachedRead = this::onRead;
        enablePing = true;
        mask = new byte[4];
        jsonEncoder = EncoderUtils.createJsonEncoder();
        readBuffer = BufferUtils.allocateDirect(readBufferSize);
        writeBuffer = BufferUtils.allocateDirect(writeBufferSize);
        json = new Json();
        jsonEncoder.setFinishRootLevelListener(this::onJsonWrite);
    }

    /**
     * Returns true if the socket is connected and the WebSocket handshake is complete.
     *
     * @return true if the socket is connected and the WebSocket handshake is complete
     */
    public abstract boolean isConnected();

    /**
     * Returns the exception thrown that caused the connection to fail.
     *
     * @return the exception thrown that caused the connection to fail
     */
    protected abstract Exception getConnectionFailedException();

    /**
     * Returns the reason the connection failed.
     *
     * @return the reason the connection failed
     */
    protected abstract String getConnectionFailedReason();

    /**
     * Returns the socket channel.
     *
     * @return the socket channel
     */
    protected abstract SocketChannel getChannel();

    /**
     * Gets a packet formatted to send a ping.
     *
     * @return a packet formatted to send a ping
     */
    protected abstract DirectBuffer getPingPacket();

    /**
     * Gets a packet formatted to send a pong.
     *
     * @return a packet formatted to send a pong
     */
    protected abstract DirectBuffer getPongPacket();

    /**
     * Sets a listener to be invoked when the connection has failed.
     *
     * <p>The specified {@code listener} should invoke {@link #close()} on the socket.
     *
     * @param listener a listener to be invoked when the connection has failed
     */
    public void setConnectionFailedListener(Runnable listener) {
        connectionFailedListener = listener;
    }

    /**
     * Sets the listener used for logging I/O events.
     *
     * @param listener the listener used for logging I/O events
     */
    public void setIoListener(IoListener listener) {
        ioListener = listener;
    }

    /**
     * Sets the read buffer size.
     *
     * @param size the read buffer size
     */
    public void setReadBufferSize(int size) {
        readBuffer = BufferUtils.allocateDirect(size);
    }

    /**
     * Sets the write buffer size.
     *
     * @param size the write buffer size
     */
    public void setWriteBufferSize(int size) {
        writeBuffer = BufferUtils.allocateDirect(size);
    }

    /**
     * Sets a listener for JSON values read by the socket.
     *
     * @param listener a listener for JSON values read by the socket
     */
    public void setReadListener(Consumer<Json.Value> listener) {
        readListener = listener;
    }

    /**
     * Invoked when the socket has read data.
     */
    protected abstract void onRead();

    /**
     * Invoked when the JSON encoder has finished encoding a complete object.
     *
     * <p>The subclass format the data appropriately and write to the socket.
     */
    protected abstract void onJsonWrite();

    /**
     * Closes the connection.
     */
    public abstract void close();

    /**
     * Returns a JSON object encoder to be encoded in a text frame.
     *
     * @return a JSON object encoder to be encoded in a text frame
     */
    public abstract ObjectEncoder json();

    /**
     * Sends a ping.
     */
    protected void ping() {
        try {
            if (enablePing && isConnected()) {
                pingTaskId = scheduler.scheduleIn(pingTaskId, PING_DURATION, cachedPing, "WebSocketClient:ping", 0);

                if (ioListener != null) {
                    ioListener.onWriteEvent("WebSocket ping");
                }

                getChannel().write(getPingPacket());
            } else {
                pingTaskId = 0;
            }
        } catch (IOException e) {
            connectionFailedException = e;
            connectionFailedReason = "error writing to socket";
            onConnectionFailed();
        } catch (NotYetConnectedException e) {
            connectionFailedException = e;
            connectionFailedReason = "not yet connected to socket";
            onConnectionFailed();
        }
    }

    /**
     * Sends an unsolicited pong.
     */
    protected void unsolicitedPong() {
        try {
            if (enableUnsolicitedPong && isConnected()) {
                pongTaskId = scheduler.scheduleIn(pongTaskId, PONG_DURATION, cachedPong, "WebSocketClient:pong", 0);

                if (ioListener != null) {
                    ioListener.onWriteEvent("WebSocket pong");
                }

                getChannel().write(getPongPacket());
            } else {
                pongTaskId = 0;
            }
        } catch (IOException e) {
            connectionFailedException = e;
            connectionFailedReason = "error writing to socket";
            onConnectionFailed();
        } catch (NotYetConnectedException e) {
            connectionFailedException = e;
            connectionFailedReason = "not yet connected to socket";
            onConnectionFailed();
        }
    }

    /**
     * Invoked when the connection has failed.
     *
     * <p>This method will invoke the connection failed listener specified with
     * {@link #setConnectionFailedListener(Runnable) setConnectionFailedListener}.
     */
    protected void onConnectionFailed() {
        if (ioListener != null) {
            ioListener.onConnectionFailed(getConnectionFailedReason(), getConnectionFailedException());
        }

        if (connectionFailedListener != null) {
            connectionFailedListener.run();
        }
    }

    /**
     * Enables sending a ping every 1 second if the specified {@code enablePing} parameter is set to true; otherwise
     * no ping is sent.
     *
     * @param enablePing true if a ping is sent every 1 second by the client
     */
    public void enablePing(boolean enablePing) {
        this.enablePing = enablePing;

        if (enablePing && isConnected()) {
            pingTaskId = scheduler.scheduleIn(pingTaskId, PING_DURATION, cachedPing, "WebSocketClient:ping", 0);
        } else {
            pingTaskId = scheduler.cancel(pingTaskId);
        }
    }

    /**
     * Enables sending a pong every 10 seconds if the specified {@code enableUnsolicitedPong} parameter is set to true;
     * otherwise no pong is sent until a ping is received from the counterparty.
     *
     * @param enableUnsolicitedPong true if a pong is to be sent every 10 seconds by the client
     */
    public void enableUnsolicitedPong(boolean enableUnsolicitedPong) {
        this.enableUnsolicitedPong = enableUnsolicitedPong;

        if (enableUnsolicitedPong && isConnected()) {
            pongTaskId = scheduler.scheduleIn(pongTaskId, PONG_DURATION, cachedPong, "JsonWebSocketClient:pong", 0);
        } else {
            pongTaskId = scheduler.cancel(pongTaskId);
        }
    }
}
