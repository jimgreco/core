package com.core.platform.fix;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.io.IoListener;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.time.Time;
import com.core.platform.fix.schema.Fix42;
import com.core.platform.fix.schema.MsgType;
import org.agrona.DirectBuffer;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.BDDAssertions.then;

public class TestFixClient implements FixEngine {

    private final FixSessionConfiguration sessionConfiguration;
    private final Map<DirectBuffer, Consumer<FixMsg>> msgTypeListeners;
    private final List<FixMsg> outboundMessages;
    private final FixMsgWriterImpl outboundWriter;
    private final Log log;
    private boolean autoConnect;
    private boolean connected;
    private Runnable connectedListener;
    private Runnable connectionFailedListener;
    private boolean logon;
    private int inboundSeqNum;
    private int outboundSeqNum;
    private boolean resetSeqNum;

    public TestFixClient(
            LogFactory logFactory, Time time, FixVersion fixVersion, String senderCompId, String targetCompId) {
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(time, "time is null");

        log = logFactory.create(TestFixClient.class);
        sessionConfiguration = new FixSessionConfiguration(fixVersion, senderCompId, targetCompId);
        msgTypeListeners = new UnifiedMap<>();
        outboundMessages = new CoreList<>();
        outboundWriter = new FixMsgWriterImpl(time, sessionConfiguration, buffer -> {
            var fixMsg = FixMsg.parse(BufferUtils.toAsciiString(buffer));

            log.info().append(sessionConfiguration.getSenderCompIdAsBuffer())
                    .append(" -> ").append(sessionConfiguration.getTargetCompIdAsBuffer())
                    .append(": ").append(fixMsg.toString())
                    .commit();

            outboundMessages.add(fixMsg);
            outboundSeqNum++;
        });
        outboundSeqNum = 1;
        inboundSeqNum = 1;
        autoConnect = true;
        resetSeqNum = true;
    }

    public void setResetSeqNum(boolean resetSeqNum) {
        this.resetSeqNum = resetSeqNum;
    }

    @Override
    public void connect() {
        if (connected) {
            throw new IllegalStateException("cannot connect twice");
        }

        logon = false;
        if (resetSeqNum) {
            inboundSeqNum = 1;
            outboundSeqNum = 1;
        }
        if (autoConnect) {
            connected = true;
            if (connectedListener != null) {
                connectedListener.run();
            }
        }
    }

    @Override
    public void close() {
        logon = false;
        if (connected) {
            connected = false;
            if (connectionFailedListener != null) {
                connectionFailedListener.run();
            }
        }
    }

    @Override
    public void setConnectionFailedListener(Runnable listener) {
        connectionFailedListener = listener;
    }

    @Override
    public void setConnectTimeout(long connectTimeout) {

    }

    @Override
    public void setReconnectTimeout(long reconnectTimeout) {

    }

    @Override
    public void disableReconnect() {

    }

    @Override
    public void enableReconnect() {

    }

    @Override
    public void setConnectedListener(Runnable listener) {
        connectedListener = listener;
    }

    @Override
    public FixSessionConfiguration getSessionConfiguration() {
        return sessionConfiguration;
    }

    @Override
    public boolean isLogon() {
        return logon;
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public Exception getConnectionFailedException() {
        return null;
    }

    @Override
    public String getConnectionFailedReason() {
        return null;
    }

    @Override
    public void setIoListener(IoListener listener) {

    }

    @Override
    public void setWriteBufferSize(int size) {

    }

    @Override
    public void setReadBufferSize(int size) {

    }

    @Override
    public int getInboundMsgSeqNum() {
        return inboundSeqNum;
    }

    @Override
    public int getOutboundMsgSeqNum() {
        return outboundSeqNum;
    }

    @Override
    public FixMsgWriter createMessage(DirectBuffer msgType) {
        return outboundWriter.start(msgType, outboundSeqNum);
    }

    @Override
    public FixEngine setMessageListener(DirectBuffer msgType, Consumer<FixMsg> listener) {
        msgTypeListeners.put(BufferUtils.copy(msgType), listener);
        return this;
    }

    public void setAutoConnect(boolean autoConnect) {
        this.autoConnect = autoConnect;
    }

    public void removeAllSent() {
        outboundMessages.clear();
    }

    public FixMsg removeSent() {
        return outboundMessages.remove(0);
    }

    public void validateNoMoreSent() {
        then(outboundMessages.isEmpty()).isTrue();
    }

    public void validateSent(String fixMsgString) {
        var fixMsg = removeSent();

        var fields = fixMsgString.split("\\|");
        for (var field : fields) {
            var tagValue = field.split("=");
            var tag = Integer.parseInt(tagValue[0]);
            var expected = tagValue[1];
            var actual = fixMsg.getValue(tag);
            if (expected.equals("null")) {
                if (actual == null) {
                    continue;
                } else {
                    fail("value for tag where not expected: " + tag);
                }
            } else if (actual == null) {
                fail("missing value for tag: " + tag);
            }
            var actualStr = BufferUtils.toAsciiString(actual);
            if (!expected.equals("*")) {
                then(actualStr).withFailMessage(
                        "incorrect value for tag: tag=" + tag
                                + ", expected=" + expected
                                + ", actual=" + actualStr).isEqualTo(expected);
            }
        }
    }

    public void dispatchReceived(String fixMsgString) {
        var fixMsg = FixMsg.parse(fixMsgString);

        log.info().append(sessionConfiguration.getSenderCompIdAsBuffer())
                .append(" <- ").append(sessionConfiguration.getTargetCompIdAsBuffer())
                .append(": ").append(fixMsg.toString())
                .commit();

        var msgType = fixMsg.getValue(Fix42.MSG_TYPE);

        if (MsgType.LOGON.getValue().equals(msgType)) {
            logon = true;
        }

        inboundSeqNum = (int) fixMsg.getValueAsInteger(Fix42.MSG_SEQ_NUM, inboundSeqNum) + 1;

        var listener = msgTypeListeners.get(msgType);
        if (listener != null) {
            listener.accept(fixMsg);
        }
    }
}
