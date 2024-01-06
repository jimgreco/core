package com.core.platform.fix;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreMap;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.time.Time;
import com.core.platform.fix.schema.Fix42;
import com.core.platform.fix.schema.MsgType;
import org.agrona.DirectBuffer;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

class FixDispatcher implements FixParser.Dispatcher {

    private final Time time;
    private final Log log;
    private final Map<DirectBuffer, Consumer<FixMsg>> msgTypeToListener;
    private final Consumer<FixMsg> allMsgListener;
    private int inboundMsgSeqNum;
    private long inboundMsgTime;
    private boolean logon;

    FixDispatcher(Time time, LogFactory logFactory, Consumer<FixMsg> allMsgListener) {
        this.time = Objects.requireNonNull(time, "time is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        this.allMsgListener = Objects.requireNonNull(allMsgListener, "allMsgListener is null");

        log = logFactory.create(FixDispatcher.class);
        msgTypeToListener = new CoreMap<>();
        inboundMsgSeqNum = 1;
    }

    boolean isLogon() {
        return logon;
    }

    long getInboundMsgTime() {
        return inboundMsgTime;
    }

    int getInboundMsgSeqNum() {
        return inboundMsgSeqNum;
    }

    void setInboundSeqNum(int newSeqNo) {
        this.inboundMsgSeqNum = newSeqNo;
    }

    void setListener(DirectBuffer msgType, Consumer<FixMsg> listener) {
        msgTypeToListener.put(BufferUtils.copy(msgType), listener);
    }

    void logon() {
        this.logon = true;
    }

    void logoff(boolean resetInboundSeqNum) {
        logon = false;
        inboundMsgTime = 0;
        if (resetInboundSeqNum) {
            inboundMsgSeqNum = 1;
        }
    }

    @Override
    public boolean onFixMsg(FixMsg fixMsg) {
        inboundMsgTime = time.nanos();

        var msgType = fixMsg.getValue(Fix42.MSG_TYPE);
        final var listener = msgTypeToListener.get(msgType);

        var msgSeqNum = fixMsg.getValueAsInteger(Fix42.MSG_SEQ_NUM);
        if (msgSeqNum < inboundMsgSeqNum) {
            FixUtils.logFix(log.warn()
                            .append("MsgSeqNum[34] lower than expected: expected=").append(inboundMsgSeqNum)
                            .append(", received=").append(msgSeqNum)
                            .append(", fix="), fixMsg)
                    .commit();
            return false;
        }

        if (logon) {
            if (msgSeqNum != inboundMsgSeqNum) {
                FixUtils.logFix(log.warn()
                        .append("invalid MsgSeqNum[34]: expected=").append(inboundMsgSeqNum)
                        .append(", received=").append(msgSeqNum)
                        .append(", fix="), fixMsg)
                        .commit();
                return false;
            }

            inboundMsgSeqNum++;

            allMsgListener.accept(fixMsg);
            if (listener == null) {
                FixUtils.logFix(log.warn()
                        .append("unhandled FIX Logon[A]: ").append(msgType), fixMsg)
                        .commit();
                return false;
            } else {
                listener.accept(fixMsg);
            }
            return true;
        } else {
            if (!MsgType.LOGON.getValue().equals(msgType)) {
                FixUtils.logFix(log.warn()
                        .append("first message received must be Logon[A]: msgType=").append(msgType)
                        .append(", fix="), fixMsg)
                        .commit();
                return false;
            }

            if (msgSeqNum == inboundMsgSeqNum) {
                inboundMsgSeqNum++;
            }

            allMsgListener.accept(fixMsg);
            if (listener == null) {
                FixUtils.logFix(log.warn()
                        .append("unhandled FIX message: msgType=").append(msgType)
                        .append(", fix="), fixMsg)
                        .commit();
            } else {
                listener.accept(fixMsg);
            }
            return true;
        }
    }
}
