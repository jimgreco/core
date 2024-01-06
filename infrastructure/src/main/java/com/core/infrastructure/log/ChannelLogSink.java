package com.core.infrastructure.log;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.io.WritableBufferChannel;
import com.core.infrastructure.MemoryUnit;
import com.core.infrastructure.time.TimestampDecimals;
import com.core.infrastructure.time.TimestampFormatter;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

/**
 * A log sink that writes to a {@code WritableBufferChannel}.
 */
public class ChannelLogSink implements LogSink {

    // TODO: can't make a wrapped buffer grow
    private static final int INITIAL_MSG_SIZE = (int) MemoryUnit.KILOBYTES.toBytes(1024);

    private final WritableBufferChannel channel;
    private final MutableDirectBuffer buffer;
    private final MutableDirectBuffer wrapper;
    private final TimestampFormatter timestampFormatter;
    private int headerLength;

    /**
     * Creates a {@code ChannelLogSink} with the specified buffer channel to write log statements to, millisecond
     * timestamp precision and the system default time zone.
     *
     * @param channel the channel to write log statements to
     */
    public ChannelLogSink(WritableBufferChannel channel) {
        this(channel, TimestampDecimals.MILLISECONDS, ZoneId.systemDefault());
    }

    /**
     * Creates a {@code ChannelLogSink} with the specified buffer channel to write log statements to, timestamp
     * precision, and the specified time zone.
     *
     * @param channel the channel to write log statements to
     * @param timestampDecimals the timestamp precision
     * @param zoneId the zone to write timestamps at
     */
    public ChannelLogSink(
            WritableBufferChannel channel, TimestampDecimals timestampDecimals, ZoneId zoneId) {
        this.channel = Objects.requireNonNull(channel);
        timestampFormatter = new TimestampFormatter(zoneId, timestampDecimals);
        buffer = BufferUtils.allocateExpandable(INITIAL_MSG_SIZE);
        wrapper = BufferUtils.mutableEmptyBuffer();
    }

    @Override
    public MutableDirectBuffer start(LogLevel logLevel, DirectBuffer logId, long timestamp) {
        var length = timestampFormatter.writeDateTime(buffer, 0, timestamp);
        buffer.putByte(length++, (byte) ' ');

        buffer.putBytes(length, logLevel.getNameAsBuffer(), 0, logLevel.getNameAsBuffer().capacity());
        length += logLevel.getNameAsBuffer().capacity();
        buffer.putByte(length++, (byte) ' ');

        buffer.putBytes(length, logId, 0, logId.capacity());
        length += logId.capacity();
        buffer.putByte(length++, (byte) ' ');

        wrapper.wrap(buffer, length, buffer.capacity() - length);
        headerLength = length;
        return wrapper;
    }

    @SuppressWarnings("PMD.EmptyCatchBlock")
    @Override
    public void commit(int length) {
        try {
            channel.write(buffer, 0, length + headerLength);
        } catch (IOException e) {
            // cannot do anything when the log fails to write
        }
    }

    @SuppressWarnings("PMD.EmptyCatchBlock")
    @Override
    public void dump(DirectBuffer buffer, int index, int length) {
        try {
            var position = index;
            var capacity = index + length;
            while (position < capacity) {
                position += channel.write(buffer, position, capacity - position);
            }
        } catch (IOException e) {
            // cannot do anything when the log fails to write
        }
    }
}
