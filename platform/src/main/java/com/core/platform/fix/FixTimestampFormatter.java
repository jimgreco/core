package com.core.platform.fix;

import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

/**
 * Formatter utilities for writing FIX timestamps to buffers.
 */
public class FixTimestampFormatter {

    private static final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
    private static final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
    private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
    private static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
    private static final DateTimeFormatter FIRST_PART_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 4, SignStyle.NOT_NEGATIVE)
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .appendLiteral('-')
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .toFormatter();


    private long startOfHourNanos;
    private long endOfHourNanos;
    private DirectBuffer dateTimeHour;

    /**
     * Constructs an empty {@code FixTimestampFormatter}.
     */
    public FixTimestampFormatter() {
    }

    /**
     * Writes a FIX date to the buffer for the specified timestamp, which is the number of nanoseconds since epoch
     * (January 1, 1970).
     * A FIX date has the following format: "yyyyMMdd".
     * The timezone is UTC.
     *
     * @param buffer the buffer
     * @param index the first byte of the buffer to write
     * @param timestamp the timestamp
     * @return the number of bytes written
     */
    public int writeDate(MutableDirectBuffer buffer, int index, long timestamp) {
        calibrateDate(timestamp);

        // write date by lopping off the separators and hour bytes
        var length = dateTimeHour.capacity() - 4;
        buffer.putBytes(index, dateTimeHour, 0, length);
        return length;
    }

    /**
     * Writes a FIX date-time to the buffer for the specified timestamp, which is the number of nanoseconds since epoch
     * (January 1, 1970).
     * A FIX date has the following format: "yyyyMMdd-HH:mm:ss.SSS".
     * The timezone is UTC.
     *
     * @param buffer the buffer
     * @param index the first byte of the buffer to write
     * @param timestamp the timestamp
     * @return the number of bytes written
     */
    public int writeDateTime(MutableDirectBuffer buffer, int index, long timestamp) {
        calibrateDate(timestamp);

        // date and hour
        buffer.putBytes(index, dateTimeHour, 0, dateTimeHour.capacity());
        var position = index + dateTimeHour.capacity();

        // minutes
        var time = timestamp - startOfHourNanos;
        var minutes = time / NANOS_PER_MINUTE;
        buffer.putNaturalPaddedIntAscii(position, 2, (int) minutes);
        position += 2;

        // seconds
        buffer.putByte(position++, (byte) ':');
        time -= minutes * NANOS_PER_MINUTE;
        var seconds = time / NANOS_PER_SECOND;
        buffer.putNaturalPaddedIntAscii(position, 2, (int) seconds);
        position += 2;

        buffer.putByte(position++, (byte) '.');
        time -= seconds * NANOS_PER_SECOND;
        time /= NANOS_PER_MILLISECOND;
        buffer.putNaturalPaddedIntAscii(position, 3, (int) time);
        position += 3;

        return position - index;
    }

    private void calibrateDate(long timestamp) {
        if (timestamp < startOfHourNanos || timestamp >= endOfHourNanos) {
            // calibrate time
            var instant = Instant.ofEpochSecond(
                    timestamp / NANOS_PER_SECOND, timestamp % NANOS_PER_SECOND);
            var zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);

            startOfHourNanos = TimeUnit.SECONDS.toNanos(zonedDateTime.truncatedTo(ChronoUnit.HOURS).toEpochSecond());
            endOfHourNanos = startOfHourNanos + NANOS_PER_HOUR;
            dateTimeHour = BufferUtils.fromAsciiString(FIRST_PART_FORMATTER.format(zonedDateTime));
        }
    }
}
