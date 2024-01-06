package com.core.infrastructure.time;

import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Formatter utilities for writing timestamps to buffers.
 */
public class TimestampFormatter {

    private static final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
    private static final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
    private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
    private static final DateTimeFormatter FIRST_PART_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .appendLiteral('T')
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .toFormatter();

    private final ZoneId zoneId;
    private final TimestampDecimals timestampDecimals;

    private long startOfHourNanos;
    private long endOfHourNanos;
    private DirectBuffer dateTimeHour;
    private DirectBuffer zoneOffset;

    /**
     * Constructs a {@code TimestampFormatter} with the specified time zone and the timestamp decimal format.
     *
     * @param zoneId the time zone
     * @param timestampDecimals the timestamp decimal format
     */
    public TimestampFormatter(ZoneId zoneId, TimestampDecimals timestampDecimals) {
        this.zoneId = Objects.requireNonNull(zoneId, "zoneId is null");
        this.timestampDecimals = Objects.requireNonNull(timestampDecimals, "timestampDecimals is null");
    }

    /**
     * Writes the specified {@code timestamp} to the {@code buffer} as an ISO date time.
     * The ISO date time output has the following format: {@code yyyy-MM-ddTHH:mm:ss[.SSSSSSSSS](Z|[+-]HH:mm:ss)}.
     * The timestamp is specified as the number of nanoseconds since epoch (January 1, 1970).
     *
     * @param buffer the buffer to write the time to
     * @param index the first byte in the buffer to write the time to
     * @param timestamp the timestamp to write
     * @return the number of bytes written to the buffer
     */
    public int writeDateTime(MutableDirectBuffer buffer, int index, long timestamp) {
        calibrateDateTime(timestamp);

        // date and hour
        buffer.putBytes(index, dateTimeHour, 0, dateTimeHour.capacity());
        var position = index + dateTimeHour.capacity();

        // minutes/seconds/sub-seconds
        position = writeMinutesSecondsSubseconds(buffer, timestamp, position);

        // timezone
        buffer.putBytes(position, zoneOffset, 0, zoneOffset.capacity());
        position += zoneOffset.capacity();

        return position - index;
    }

    /**
     * Writes the specified {@code timestamp} to the {@code buffer} as time.
     * The ISO time output has the following format: {@code HH:mm:ss[.SSS]}.
     * The timestamp is specified as the number of nanoseconds since epoch (January 1, 1970).
     *
     * @param buffer the buffer to write the time to
     * @param index the first byte in the buffer to write the time to
     * @param timestamp the timestamp to write
     * @return the number of bytes written to the buffer
     */
    public int writeTime(MutableDirectBuffer buffer, int index, long timestamp) {
        calibrateDateTime(timestamp);

        // hour
        var position = index;
        buffer.putBytes(position, dateTimeHour, dateTimeHour.capacity() - 3, 3);
        position += 3;

        // minutes/seconds/sub-seconds
        position = writeMinutesSecondsSubseconds(buffer, timestamp, position);

        return position - index;
    }

    private void calibrateDateTime(long timestamp) {
        if (timestamp < startOfHourNanos || timestamp >= endOfHourNanos) {
            // calibrate time
            var instant = Instant.ofEpochSecond(
                    timestamp / NANOS_PER_SECOND, timestamp % NANOS_PER_SECOND);
            var zonedDateTime = ZonedDateTime.ofInstant(instant, zoneId);

            zoneOffset = BufferUtils.fromAsciiString(zoneId.getRules().getOffset(instant).getId());
            startOfHourNanos = TimeUnit.SECONDS.toNanos(zonedDateTime.truncatedTo(ChronoUnit.HOURS).toEpochSecond());
            endOfHourNanos = startOfHourNanos + NANOS_PER_HOUR;
            dateTimeHour = BufferUtils.fromAsciiString(FIRST_PART_FORMATTER.format(zonedDateTime));
        }
    }

    private int writeMinutesSecondsSubseconds(MutableDirectBuffer buffer, long timestamp, int position) {
        // minutes
        var time = timestamp - startOfHourNanos;
        var minutes = time / NANOS_PER_MINUTE;
        buffer.putNaturalPaddedIntAscii(position, 2, (int) minutes);
        position += 2;
        buffer.putByte(position++, (byte) ':');

        // seconds
        time -= minutes * NANOS_PER_MINUTE;
        var seconds = time / NANOS_PER_SECOND;
        buffer.putNaturalPaddedIntAscii(position, 2, (int) seconds);
        position += 2;

        // sub-seconds
        if (timestampDecimals.getDecimals() != 0) {
            buffer.putByte(position++, (byte) '.');
            time -= seconds * NANOS_PER_SECOND;
            time /= timestampDecimals.getNanos();
            buffer.putNaturalPaddedIntAscii(position, timestampDecimals.getDecimals(), (int) time);
            position += timestampDecimals.getDecimals();
        }
        return position;
    }
}
