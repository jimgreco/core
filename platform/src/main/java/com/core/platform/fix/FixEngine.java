package com.core.platform.fix;

import com.core.platform.io.SslProtocolClient;
import org.agrona.DirectBuffer;

import java.util.function.Consumer;

/**
 * A FIX engine allows the user to connect to a single counterparty and exchange FIX messages.
 * The implementation class determines whether the FIX engine is a client or server.
 *
 * <p>For FIX clients, the {@link #startConnecting(String, String) startConnecting} method will initiate a connection
 * attempt by the client.
 * The client will continue to attempt to reconnect, until a connection is successfully established.
 * If the server uses SSL, an SSL handshake is initiated by the client and negotiated.
 * If disconnected, the client will attempt to reconnect until {@link #stopConnecting()} is invoked.
 *
 * <p>For FIX servers, the {@link #startConnecting(String, String) startConnecting} method will open the TCP port and
 * wait for connections from the client.
 * If SSL is used, then an SSL handshake is negotiated between the client and server.
 * If disconnected, the server will accept a new client connection until {@link #stopConnecting()} is invoked.
 *
 * <p>For both the client and server implementations, the listener specified with
 * {@link #setConnectedListener(Runnable) setConnectedListener} will be invoked after connecting and the SSL protocol
 * is negotiated.
 * The listener specified with {@link #setDisconnectedListener(Runnable) setDisconnectedListener} is invoked when the
 * client or server is disconnected.
 * Counterparties can be disconnected manually using {@link #disconnect()}.
 *
 * <p>Inbound messages can be subscribed to using the
 * {@link #setMessageListener(DirectBuffer, Consumer) setMessageListener} method.
 * Fully validated messages with be dispatched to the specified listener.
 * The next expected inbound sequence number is available through the {@link #getInboundMsgSeqNum()}.
 *
 * <p>Outbound messages can be created using the {@link #createMessage(DirectBuffer) createMessage} method.
 * The engine writes all header fields, including the BeginString[8], BodyLength[9], MsgType[35], SenderCompID[49],
 * TargetCompID[56], MsgSeqNum[34], SendingTime[52], and Checksum[10] fields, and returns a {@link FixMsgWriter} to
 * write the other header and application fields.
 * The session configuration (i.e., BeginString[8], SenderCompID[49], and TargetCompID[56]) can be edited on the
 * session configuration object returned through {@link #getSessionConfiguration()}.
 * The next outbound sequence number is available through the {@link #getOutboundMsgSeqNum()}.
 *
 * @see FixMsg
 * @see FixMsgWriter
 */
public interface FixEngine extends SslProtocolClient {

    /**
     * Starts the connection process with the FIX counterparty.
     * If the engine is a client, the engine will begin initiating an SSL socket connection with the counterparty.
     * If the engine is a server, the engine will bind and receive connections from the counterparty.
     *
     * <p>Once the sockets are connected, the SSL handshake must be completed.
     * Then, the listener specified in {@link #setConnectedListener(Runnable) setConnectedListener} will be invoked.
     * If {@link #enableReconnect()} has been invoked then the client will continue to attempt to connect
     * until {@link #disableReconnect()} is invoked.
     */
    void connect();

    /**
     * Returns the session configuration for the FIX engine.
     *
     * @return the session configuration for the FIX engine
     */
    FixSessionConfiguration getSessionConfiguration();

    /**
     * Returns true if a valid Logon[A] message has been exchanged with the counterparty.
     *
     * @return true if a valid Logon[A] message has been exchanged with the counterparty
     */
    boolean isLogon();

    /**
     * Returns the next inbound MsgSeqNum[34] expected from the counterparty.
     *
     * @return the next inbound MsgSeqNum[34] expected from the counterparty
     */
    int getInboundMsgSeqNum();

    /**
     * Returns the last inbound MsgSeqNum[34] received from the counterparty.
     *
     * @return the last inbound MsgSeqNum[34] received from the counterparty
     */
    default int getLastInboundMsgSeqNum() {
        return getInboundMsgSeqNum() - 1;
    }

    /**
     * Returns the next MsgSeqNum[34] that will be sent to the counterparty.
     *
     * @return the next MsgSeqNum[34] that will be sent to the counterparty
     */
    int getOutboundMsgSeqNum();

    /**
     * Returns the last MsgSeqNum[34] sent to the counterparty.
     *
     * @return the last MsgSeqNum[34] sent to the counterparty
     */
    default int getLastOutboundMsgSeqNum() {
        return getOutboundMsgSeqNum() - 1;
    }

    /**
     * Creates a new FIX message and returns a FIX message writer.
     * The message will not be finalized and sent until {@link FixMsgWriterImpl#send()} is invoked.
     *
     * @param msgType the MsgType[35] value of the FIX message
     * @param <T> the message type enumerator
     * @return the FIX message writer
     * @implSpec the default implementation invokes {@code createMessage(msgType.value())}
     */
    default <T extends Enum<T>> FixMsgWriter createMessage(FixEnum<T> msgType) {
        return createMessage(msgType.getValue());
    }

    /**
     * Creates a new FIX message and returns a FIX message writer.
     * The message will not be finalized and sent until {@link FixMsgWriterImpl#send()} is invoked.
     *
     * @param msgType the MsgType[35] value of the FIX message
     * @return the FIX message writer
     */
    FixMsgWriter createMessage(DirectBuffer msgType);

    /**
     * Sets the specified {@code listener} to be invoked when messages with the specified message type are received
     * from counterparty.
     *
     * @param msgType the MsgType[35] value of the FIX message
     * @param listener the FIX message listener
     * @param <T> the message type enumerator
     * @return this
     * @implSpec the default implementation invokes {@code setMessageListener(msgType.value(), listener)}
     */
    default <T extends Enum<T>> FixEngine setMessageListener(FixEnum<T> msgType, Consumer<FixMsg> listener) {
        return setMessageListener(msgType.getValue(), listener);
    }

    /**
     * Sets the specified {@code listener} to be invoked when messages with the specified message type are received
     * from counterparty.
     *
     * @param msgType the MsgType[35] value of the FIX message
     * @param listener the FIX message listener
     * @return this
     */
    FixEngine setMessageListener(DirectBuffer msgType, Consumer<FixMsg> listener);
}
