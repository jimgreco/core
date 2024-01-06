package com.core.infrastructure.io;

import com.core.infrastructure.MemoryUnit;
import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.log.Log;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.Objects;

/**
 * A logger for the I/O listener.
 */
public class IoLogger implements IoListener {

    private final Log log;
    private final MutableDirectBuffer buffer;
    private final String prefix;

    /**
     * Creates a new {@code WebSocketLogger} with the specified {@code log}.
     *
     * @param log the log
     * @param prefix the prefix to use before each statement
     */
    public IoLogger(Log log, String prefix) {
        this.log = Objects.requireNonNull(log, "log is null");
        Objects.requireNonNull(prefix, "prefix is null");
        this.prefix = "[" + prefix + "]";
        buffer = BufferUtils.allocate((int) MemoryUnit.KILOBYTES.toBytes(16));
    }

    @Override
    public boolean isDebug() {
        return log.isDebug();
    }

    @Override
    public MutableDirectBuffer borrowBuffer() {
        return buffer;
    }

    @Override
    public void onWriteEvent(String text) {
        if (log.isDebug()) {
            var send = "[SEND] ";
            if (text != null && text.length() > 0 && text.charAt(0) == '[') {
                send = "[SEND]";
            }
            log.debug().append(prefix).append(send).append(text).commit();
        }
    }

    @Override
    public void onWriteEvent(DirectBuffer buffer, int offset, int length) {
        if (log.isDebug()) {
            var send = "[SEND] ";
            if (length > 0 && buffer.getByte(offset) == '[') {
                send = "[SEND]";
            }
            log.debug().append(prefix).append(send).append(buffer, offset, length).commit();
        }
    }

    @Override
    public void onReadEvent(String text) {
        if (log.isDebug()) {
            var recv = "[RECV] ";
            if (text != null && text.length() > 0 && text.charAt(0) == '[') {
                recv = "[RECV]";
            }
            log.debug().append(prefix).append(recv).append(text).commit();
        }
    }

    @Override
    public void onReadEvent(DirectBuffer buffer, int offset, int length) {
        if (log.isDebug()) {
            var recv = "[RECV] ";
            if (length > 0 && buffer.getByte(offset) == '[') {
                recv = "[RECV]";
            }
            log.debug().append(prefix).append(recv).append(buffer, offset, length).commit();
        }
    }

    @Override
    public void onConnectionEvent(String text) {
        var recv = "[CONN] ";
        if (text != null && text.length() > 0 && text.charAt(0) == '[') {
            recv = "[CONN]";
        }
        log.info().append(prefix).append(recv).append(text).commit();
    }

    @Override
    public void onConnectionEvent(DirectBuffer buffer, int offset, int length) {
        var event = "[CONN] ";
        if (length > 0 && buffer.getByte(offset) == '[') {
            event = "[CONN]";
        }
        log.info().append(prefix).append(event).append(buffer, offset, length).commit();
    }

    @Override
    public void onConnectionFailed(String reason, Exception exception) {
        if (reason == null) {
            log.info().append(prefix).append("[CONN] Counterparty disconnect").commit();
        } else if (exception == null) {
            log.warn().append(prefix).append("[CONN] ").append(reason).commit();
        } else {
            log.warn().append(prefix).append("[CONN] ").append(reason)
                    .append(": ").append(exception)
                    .commit();
        }
    }
}
