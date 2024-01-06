package com.core.platform.fix;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Directory;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.EncoderUtils;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.IoListener;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.io.SslSocketChannel;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.metrics.MetricFactory;
import com.core.infrastructure.time.Scheduler;
import com.core.infrastructure.time.Time;
import com.core.platform.fix.schema.EncryptMethod;
import com.core.platform.fix.schema.ExecType;
import com.core.platform.fix.schema.Fix42;
import com.core.platform.fix.schema.GapFillFlag;
import com.core.platform.fix.schema.MsgType;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * The {@code StatelessFixClient} is a {@code FixEngine1} client implementation that resets the inbound and outbound
 * MsgSeqNum[34] to 1 on each connection and does not provide a mechanism to replay messages to the server.
 *
 * <p>The implementation provides default implementations for the following messages.
 * <ul>
 *     <li>Logon[A]: Completes the logon handshake, requests missing inbound messages with a ResendRequest[2], and
 *         dispatches the message to a listener
 *     <li>Heartbeat[0]: Does nothing
 *     <li>TestRequest[1]: Returns a Heartbeat[0] message with the TestReqId[112] specified in the message, and
 *         dispatches the message to a listener
 *     <li>ResendRequest[2]: Sends a SequenceReset[4] to the current outbound sequence number, and dispatches the
 *         message to a listener
 *     <li>Reject[3]: Does nothing
 *     <li>SequenceReject[4]: Resets the inbound sequence number, and dispatches the message to a listener
 *     <li>Logout[5]: the FIX engine is disconnected, and dispatches the message to a listener
 * </ul>
 *
 * <p>The client also provides heartbeat management.
 * <ul>
 *     <li>A TestRequest[1] is sent if the client does not receive a message from the server after the HeartBtInt[108]
 *         specified in the Logon[A] message
 *     <li>The client disconnects if the client does not receive a message from the server after 2x HeartBtInt[108]
 *     <li>The client sends a Heartbeat[0] if the client does not send a message for HeartBtInt[108]
 * </ul>
 */
public class StatelessFixClient implements FixEngine, Encodable {

    private static final long DEFAULT_RECONNECT_TIMEOUT = TimeUnit.SECONDS.toNanos(5);

    private final Selector selector;
    private final Scheduler scheduler;
    private final Time time;

    private final FixMsgWriterImpl writer;
    private final FixDispatcher dispatcher;
    private final FixLexer lexer;
    @Directory(path = "config")
    private final FixSessionConfiguration sessionConfiguration;

    private final Runnable cachedRead;
    private final Runnable cachedSendHeartbeat;
    private final Runnable cachedConnect;
    private final Runnable cachedReconnect;
    private final Runnable cachedHandshakeComplete;
    private final Runnable cachedConnectionFailed;
    private final Runnable cachedConnectTimeout;

    private MutableDirectBuffer readBuffer;
    private int readBufferLength;

    private IoListener ioListener;

    private long heartbeatIntervalNanos;
    private long heartbeatTaskId;
    private boolean resetSeqNum;
    private long outboundMsgTime;
    private int outboundMsgSeqNum;
    private DirectBuffer outboundMsgType;

    private Consumer<FixMsg> testRequestListener;
    private Consumer<FixMsg> resendRequestListener;
    private Consumer<FixMsg> sequenceResetListener;
    private Consumer<FixMsg> logoutListener;
    private Consumer<FixMsg> logonListener;

    private SslSocketChannel channel;

    private boolean reconnect;
    private long reconnectTimeout;
    private long connectTimeout;
    @Property(write = true, read = false)
    private String address;
    private long reconnectTaskId;
    private long connectTaskId;

    private Runnable connectListener;
    private Runnable connectionFailedListener;

    private Exception connectionFailedException;
    private String connectionFailedReason;
    private SSLContext sslContext;
    @Property(write = true)
    private boolean sendTestRequests;

    /**
     * Constructs a {@code StatelessFixClient} with the specified parameters.
     *
     * @param selector a selector to create asynchronous TCP sockets
     * @param logFactory a factory to create logs
     * @param scheduler a real-time task scheduler
     * @param time a real-time time source
     * @param metricFactory a factory to create metrics
     * @param address the address to connect to
     */
    public StatelessFixClient(
            Selector selector, LogFactory logFactory, Scheduler scheduler, Time time, MetricFactory metricFactory,
            String address) {
        this.selector = Objects.requireNonNull(selector, "selector is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler is null");
        this.time = Objects.requireNonNull(time, "time is null");
        Objects.requireNonNull(metricFactory, "metricFactory is null");
        this.address = Objects.requireNonNull(address, "address is null");

        readBuffer = BufferUtils.allocateDirect(32 * 1024);

        cachedRead = this::onRead;
        cachedConnect = this::onConnected;
        cachedHandshakeComplete = this::onHandshakeComplete;
        cachedConnectionFailed = this::onConnectionFailed;
        cachedReconnect = this::connect;
        cachedSendHeartbeat = this::sendHeartbeat;
        cachedConnectTimeout = this::onConnectTimeout;

        sessionConfiguration = new FixSessionConfiguration();
        outboundMsgSeqNum = 1;
        resetSeqNum = true;
        reconnectTimeout = DEFAULT_RECONNECT_TIMEOUT;
        connectTimeout = DEFAULT_RECONNECT_TIMEOUT;

        writer = new FixMsgWriterImpl(time, sessionConfiguration, this::onCommit);
        dispatcher = new FixDispatcher(time, logFactory, this::onFixMsg);
        lexer = new FixLexer(logFactory, new FixParser(logFactory, dispatcher, sessionConfiguration));

        dispatcher.setListener(MsgType.LOGON.getValue(), this::onLogon);
        dispatcher.setListener(MsgType.TEST_REQUEST.getValue(), this::onTestRequest);
        dispatcher.setListener(MsgType.RESEND_REQUEST.getValue(), this::onResendRequest);
        dispatcher.setListener(MsgType.SEQUENCE_RESET.getValue(), this::onSequenceReset);
        dispatcher.setListener(MsgType.LOGOUT.getValue(), this::onLogout);

        dispatcher.setListener(MsgType.HEARTBEAT.getValue(), fixMsg -> {});
        dispatcher.setListener(MsgType.REJECT.getValue(), fixMsg -> {});

        metricFactory.registerGaugeMetric("FIX_InboundMsgSeqNum", this::getInboundMsgSeqNum);
        metricFactory.registerGaugeMetric("FIX_OutboundMsgSeqNum", this::getOutboundMsgSeqNum);
        metricFactory.registerSwitchMetric("FIX_Connected", this::isConnected);
        metricFactory.registerSwitchMetric("FIX_Logon", this::isLogon);
    }

    @Override
    public boolean isConnected() {
        return channel != null && channel.isHandshakeComplete();
    }

    @Override
    public Exception getConnectionFailedException() {
        return channel.getConnectionFailedException() == null
                ? connectionFailedException : channel.getConnectionFailedException();
    }

    @Override
    public String getConnectionFailedReason() {
        return channel.getConnectionFailedReason() == null
                ? connectionFailedReason : channel.getConnectionFailedReason();
    }

    @Override
    public void setReadBufferSize(int size) {
        readBuffer = BufferUtils.allocateDirect(size);
    }

    @Override
    public void setWriteBufferSize(int size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setIoListener(IoListener listener) {
        ioListener = listener;
    }

    @Override
    public void setConnectedListener(Runnable listener) {
        this.connectListener = listener;
    }

    @Override
    public void setConnectionFailedListener(Runnable listener) {
        connectionFailedListener = listener;
    }

    @Override
    public void setConnectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    @Override
    public void setReconnectTimeout(long timeout) {
        reconnectTimeout = timeout;
    }

    @Override
    public void disableReconnect() {
        reconnect = false;
    }

    @Override
    public void enableReconnect() {
        reconnect = true;
    }

    @Override
    public void connect() {
        try {
            if (channel == null) {
                this.address = address;

                reconnectTaskId = scheduler.cancel(reconnectTaskId);
                connectTaskId = scheduler.scheduleIn(connectTaskId, connectTimeout, cachedConnectTimeout,
                        "StatelessFixClient:connect", 0);

                if (ioListener != null) {
                    var buf = ioListener.borrowBuffer();
                    var length = buf.putStringWithoutLengthAscii(0, "FIX socket connecting: ");
                    length += buf.putStringWithoutLengthAscii(length, address);
                    ioListener.onConnectionEvent(buf, 0, length);
                }

                channel = selector.createSslSocketChannel(sslContext);
                channel.configureBlocking(false);
                channel.setReadListener(cachedRead);
                channel.setConnectListener(cachedConnect);
                channel.setHandshakeCompleteListener(cachedHandshakeComplete);
                channel.setConnectionFailedListener(cachedConnectionFailed);
                channel.connect(address);
            }
        } catch (Exception e) {
            connectionFailedReason = "error creating channel";
            connectionFailedException = e;
            onConnectionFailed();
        }
    }

    private void onConnectTimeout() {
        connectTimeout = 0;
        connectionFailedReason = "connection never completed";
        onConnectionFailed();
    }

    /**
     * Loads a password protected SSL trust store at the given path.
     *
     * @param keystorePath the path the key/trust store file
     * @param password the password protecting the trust store file
     * @throws KeyStoreException if an error occurs creating the key manager factory
     * @throws IOException if an I/O error occurs
     * @throws CertificateException if certificate can't be loaded
     * @throws NoSuchAlgorithmException if the JVM is very badly configured
     * @throws UnrecoverableKeyException if the password is wrong
     * @throws KeyManagementException if the key manager cannot be loaded
     */
    @Command
    public void loadSslTrustStore(String keystorePath, String password) throws KeyStoreException, IOException,
            CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException, KeyManagementException {
        var keystore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (var in = new FileInputStream(keystorePath)) {
            keystore.load(in, password.toCharArray());
        }
        var keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keystore, password.toCharArray());

        var trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keystore);

        sslContext = SSLContext.getInstance(SslSocketChannel.TlsV12);
        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());
    }

    /**
     * Loads a password protected SSL key store at the given path.
     *
     * @param keystorePath the path the key/trust store file
     * @param password the password protecting the trust store file
     * @throws KeyStoreException if an error occurs creating the key manager factory
     * @throws IOException if an I/O error occurs
     * @throws CertificateException if certificate can't be loaded
     * @throws NoSuchAlgorithmException if the JVM is very badly configured
     * @throws UnrecoverableKeyException if the password is wrong
     * @throws KeyManagementException if the key manager cannot be loaded
     */
    @Command
    public void loadSslKeyStore(String keystorePath, String password) throws KeyStoreException, IOException,
            CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException, KeyManagementException {
        var keystore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (var in = new FileInputStream(keystorePath)) {
            keystore.load(in, password.toCharArray());
        }
        var keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keystore, password.toCharArray());

        // This mess is to accommodate Reactive, who (strangely) insist on a client cert, but do not provide a
        // server cert that can be validated against their server name
        var trustManager = new TrustManager[] {
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                }
        };
        sslContext = SSLContext.getInstance(SslSocketChannel.TlsV12);
        sslContext.init(keyManagerFactory.getKeyManagers(), trustManager, new SecureRandom());
    }

    @Command
    @Override
    public void close() {
        try {
            if (channel == null) {
                return;
            }

            if (resetSeqNum) {
                outboundMsgSeqNum = 1;
            }
            dispatcher.logoff(resetSeqNum);
            outboundMsgTime = 0;
            connectTaskId = scheduler.cancel(connectTaskId);
            heartbeatTaskId = scheduler.cancel(heartbeatTaskId);
            readBufferLength = 0;
            connectionFailedReason = null;
            connectionFailedException = null;

            if (ioListener != null) {
                ioListener.onConnectionEvent("FIX socket closed");
            }

            if (reconnect) {
                if (ioListener != null) {
                    var buf = ioListener.borrowBuffer();
                    var length = buf.putStringWithoutLengthAscii(0, "FIX socket reconnect in ");
                    length += buf.putIntAscii(length, (int) TimeUnit.NANOSECONDS.toSeconds(reconnectTimeout));
                    length += buf.putStringWithoutLengthAscii(length, "s");
                    ioListener.onConnectionEvent(buf, 0, length);
                }
                reconnectTaskId = scheduler.scheduleIn(
                        reconnectTaskId, reconnectTimeout, cachedReconnect, "StatelessFixClient:reconnect", 0);
            } else {
                reconnectTaskId = scheduler.cancel(reconnectTaskId);
            }

            var ch = channel;
            channel = null;
            ch.close();
        } catch (IOException e) {
            connectionFailedReason = "I/O error closing socket";
            connectionFailedException = e;
            onConnectionFailed();
        }
    }

    /**
     * Send logout message.
     */
    @Command
    public void logout() {
        createMessage(MsgType.LOGOUT)
                .send();
    }

    @Override
    public FixSessionConfiguration getSessionConfiguration() {
        return sessionConfiguration;
    }

    private void onConnected() {
        if (ioListener != null) {
            ioListener.onConnectionEvent("FIX socket connected");
        }
    }

    private void onLogon(FixMsg fixMsg) {
        if (ioListener != null) {
            ioListener.onConnectionEvent("FIX Logon[A] complete");
        }

        var msgSeqNum = fixMsg.getValueAsInteger(Fix42.MSG_SEQ_NUM);
        if (msgSeqNum + 1 != dispatcher.getInboundMsgSeqNum()) {
            createMessage(MsgType.RESEND_REQUEST)
                    .putInteger(Fix42.BEGIN_SEQ_NO, dispatcher.getInboundMsgSeqNum())
                    .putInteger(Fix42.END_SEQ_NO, 0)
                    .send();
        }

        var heartbeatInterval = fixMsg.getValueAsInteger(
                Fix42.HEART_BT_INT, sessionConfiguration.getHeartbeatInterval());
        heartbeatIntervalNanos = TimeUnit.SECONDS.toNanos(heartbeatInterval);

        var timerInterval = heartbeatInterval / 2;
        if (timerInterval <= 0) {
            timerInterval = 1;
        }

        connectTaskId = scheduler.cancel(connectTaskId);
        heartbeatTaskId = scheduler.scheduleEvery(
                heartbeatTaskId, TimeUnit.SECONDS.toNanos(timerInterval), cachedSendHeartbeat,
                "StatelessFixClient:heartbeat", 0);
        dispatcher.logon();

        if (!resetSeqNum && msgSeqNum != getLastInboundMsgSeqNum()) {
            createMessage(MsgType.RESEND_REQUEST)
                    .putInteger(Fix42.BEGIN_SEQ_NO, getInboundMsgSeqNum())
                    .putInteger(Fix42.END_SEQ_NO, 0)
                    .send();
        }

        if (logonListener != null) {
            logonListener.accept(fixMsg);
        }
    }

    private void onTestRequest(FixMsg fixMsg) {
        var testReqId = fixMsg.getValue(Fix42.TEST_REQ_ID);
        var response = createMessage(MsgType.HEARTBEAT);
        if (testReqId != null) {
            response.putBuffer(Fix42.TEST_REQ_ID, testReqId);
        }
        response.send();

        if (testRequestListener != null) {
            testRequestListener.accept(fixMsg);
        }
    }

    private void onResendRequest(FixMsg fixMsg) {
        var endSeqNo = fixMsg.getValueAsInteger(Fix42.END_SEQ_NO);
        var newSeqNo = (endSeqNo == 0 ? outboundMsgSeqNum : endSeqNo) + 1;

        createMessage(MsgType.SEQUENCE_RESET)
                .putEnum(Fix42.GAP_FILL_FLAG, GapFillFlag.YES)
                .putInteger(Fix42.NEW_SEQ_NO, newSeqNo)
                .send();

        if (resendRequestListener != null) {
            resendRequestListener.accept(fixMsg);
        }
    }

    private void onSequenceReset(FixMsg fixMsg) {
        var newSeqNo = fixMsg.getValueAsInteger(Fix42.NEW_SEQ_NO);
        setInboundMsgSeqNum((int) newSeqNo);

        if (sequenceResetListener != null) {
            sequenceResetListener.accept(fixMsg);
        }
    }

    private void onLogout(FixMsg fixMsg) {
        if (logoutListener != null) {
            logoutListener.accept(fixMsg);
        }

        onConnectionFailed();
    }

    private void sendHeartbeat() {
        var time = this.time.nanos();

        // check sent message, send a heartbeat if too long
        if (time > outboundMsgTime + heartbeatIntervalNanos / 2) {
            createMessage(MsgType.HEARTBEAT).send();
        }

        // check received message, send a TestRequest[1] or disconnect if too long
        if (time > dispatcher.getInboundMsgTime() + 2 * heartbeatIntervalNanos) {
            connectionFailedReason = "FIX no message received from server, disconnecting";
            onConnectionFailed();
        } else if (dispatcher.getInboundMsgTime() + heartbeatIntervalNanos > time && sendTestRequests) {
            createMessage(MsgType.TEST_REQUEST).putInteger(Fix42.TEST_REQ_ID, time).send();
        }
    }

    private void onHandshakeComplete() {
        if (ioListener != null) {
            ioListener.onConnectionEvent("FIX socket SSL handshake complete");
        }

        if (resetSeqNum) {
            outboundMsgSeqNum = 1;
        }
        dispatcher.logoff(resetSeqNum);

        if (connectListener == null) {
            createMessage(MsgType.LOGON)
                    .putInteger(Fix42.HEART_BT_INT, sessionConfiguration.getHeartbeatInterval())
                    .putEnum(Fix42.ENCRYPT_METHOD, EncryptMethod.NONE)
                    .send();
        } else {
            connectListener.run();
        }
    }

    private void onConnectionFailed() {
        if (ioListener != null) {
            ioListener.onConnectionFailed(getConnectionFailedReason(), getConnectionFailedException());
        }

        if (connectionFailedListener != null) {
            connectionFailedListener.run();
        }
    }

    private void onRead() {
        try {
            var len = channel.read(readBuffer, readBufferLength, readBuffer.capacity() - readBufferLength);
            if (len == -1) {
                onConnectionFailed();
                return;
            }
            readBufferLength += len;

            var offset = 0;
            while (offset < readBufferLength && isConnected()) {
                var lexed = lexer.lex(readBuffer, offset, readBufferLength - offset);
                if (lexed == -1) {
                    onConnectionFailed();
                    return;
                } else if (lexed == 0) {
                    break;
                }
                offset += lexed;
            }

            if (isConnected()) {
                var length = readBufferLength - offset;
                BufferUtils.compact(readBuffer, readBufferLength, length);
                readBufferLength = length;
            }
        } catch (IOException e) {
            connectionFailedReason = "I/O read error";
            connectionFailedException = e;
            onConnectionFailed();
        }
    }

    @Override
    public FixMsgWriter createMessage(DirectBuffer msgType) {
        this.outboundMsgType = msgType;
        return writer.start(msgType, outboundMsgSeqNum);
    }

    private void onCommit(DirectBuffer message) {
        try {
            if (isConnected()) {
                if (ioListener != null && ioListener.isDebug()) {
                    var buf = ioListener.borrowBuffer();

                    var length = 0;
                    var msgType = MsgType.valueOf(outboundMsgType);
                    if (msgType != null) {
                        length += buf.putStringWithoutLengthAscii(length, "[");
                        length += buf.putStringWithoutLengthAscii(length, msgType.name());
                        length += buf.putStringWithoutLengthAscii(length, "]");
                    }

                    length += buf.putStringWithoutLengthAscii(length, " ");

                    for (var i = 0; i < message.capacity(); i++) {
                        var b = message.getByte(i);
                        if (b == FixUtils.SOH) {
                            b = '|';
                        }
                        buf.putByte(length++, b);
                    }

                    ioListener.onWriteEvent(buf, 0, length);
                }

                outboundMsgType = null;
                outboundMsgTime = time.nanos();

                channel.write(message, 0, message.capacity());
                outboundMsgSeqNum++;
            }
        } catch (IOException e) {
            connectionFailedReason = "error writing to socket";
            connectionFailedException = e;
            onConnectionFailed();
        }
    }

    private void onFixMsg(FixMsg fixMsg) {
        if (ioListener != null && ioListener.isDebug()) {
            var buf = ioListener.borrowBuffer();

            var len = 0;
            var msgTypeEnum = fixMsg.getValueAsEnum(Fix42.MSG_TYPE, MsgType.PARSER);
            if (msgTypeEnum != null) {
                len += buf.putStringWithoutLengthAscii(len, "[");
                len += buf.putStringWithoutLengthAscii(len, msgTypeEnum.name());

                if (msgTypeEnum == MsgType.EXECUTION_REPORT) {
                    var execTypeEnum = fixMsg.getValueAsEnum(Fix42.EXEC_TYPE, ExecType.PARSER);
                    if (execTypeEnum != null) {
                        len += buf.putStringWithoutLengthAscii(len, "][");
                        len += buf.putStringWithoutLengthAscii(len, execTypeEnum.name());
                    }
                }

                len += buf.putStringWithoutLengthAscii(len, "]");
            }

            len += buf.putStringWithoutLengthAscii(len, " ");
            var buffer = fixMsg.getBuffer();
            for (var i = 0; i < buffer.capacity(); i++) {
                var b = buffer.getByte(i);
                if (b == FixUtils.SOH) {
                    b = (byte) '|';
                }
                buf.putByte(len++, b);
            }

            ioListener.onReadEvent(buf, 0, len);
        }
    }

    /**
     * Returns true if FIX Logon[A] messages have been exchanged.
     *
     * @return true if FIX Logon[A] messages have been exchanged
     */
    public boolean isLogon() {
        return dispatcher.isLogon();
    }

    @Override
    public int getInboundMsgSeqNum() {
        return dispatcher.getInboundMsgSeqNum();
    }

    /**
     * Sets the next inbound sequence number expected from the counterparty.
     *
     * @param inboundSeqNum the next inbound sequence number expected from the counterparty
     */
    @Command
    public void setInboundMsgSeqNum(int inboundSeqNum) {
        dispatcher.setInboundSeqNum(inboundSeqNum);
    }

    @Override
    public int getOutboundMsgSeqNum() {
        return outboundMsgSeqNum;
    }

    /**
     * Sets the next outbound sequence number to be sent.
     *
     * @param outboundMsgSeqNum the next outbound sequence number to be sent
     */
    @Command
    public void setOutboundSeqNum(int outboundMsgSeqNum) {
        this.outboundMsgSeqNum = outboundMsgSeqNum;
    }

    /**
     * Sets whether the inbound and outbound MsgSeqNum[34] will be reset upon disconnection/reconnection.
     *
     * @param resetSeqNum true if the inbound and outbound MsgSeqNum[34] will be reset upon disconnection/reconnection
     */
    @Command
    public void setResetSeqNum(boolean resetSeqNum) {
        this.resetSeqNum = resetSeqNum;
    }

    @Override
    public StatelessFixClient setMessageListener(DirectBuffer msgType, Consumer<FixMsg> listener) {
        if (MsgType.LOGON.getValue().equals(msgType)) {
            logonListener = listener;
        } else if (MsgType.RESEND_REQUEST.getValue().equals(msgType)) {
            resendRequestListener = listener;
        } else if (MsgType.TEST_REQUEST.getValue().equals(msgType)) {
            testRequestListener = listener;
        } else if (MsgType.SEQUENCE_RESET.getValue().equals(msgType)) {
            sequenceResetListener = listener;
        } else if (MsgType.LOGOUT.getValue().equals(msgType)) {
            logoutListener = listener;
        } else {
            dispatcher.setListener(msgType, listener);
        }
        return this;
    }

    @Command(path = "state", readOnly = true)
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("session").object(sessionConfiguration)
                .string("address").string(address)
                .string("connected").bool(isConnected())
                .string("logon").bool(isLogon())
                .string("inboundMsgTime").number(dispatcher.getInboundMsgTime(), EncoderUtils.TIME_MILLIS_ENCODER)
                .string("outboundMsgTime").number(outboundMsgTime, EncoderUtils.TIME_MILLIS_ENCODER)
                .string("inboundMsgSeqNum").number(dispatcher.getInboundMsgSeqNum())
                .string("outboundMsgSeqNum").number(outboundMsgSeqNum)
                .closeMap();
    }

    @Override
    public String toString() {
        return toEncodedString();
    }
}