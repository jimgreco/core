package com.core.infrastructure.time;

import com.core.infrastructure.buffer.BufferNumberUtils;
import org.agrona.DirectBuffer;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;

/**
 * The {@code DateTimeParser} provides utility methods for parsing a UTC date-time in the ISO 8601 format.
 * (e.g., 2021-08-03T02:20:08.123Z, 2021-08-03T02:20.123456:08+00:00, 2021-08-03T02:20:08-00:00)
 *
 * <p>Only UTC date-times are supported.
 * All other timezones are unparseable.
 */
public final class UtcDateTimeParser {

    private static final int MONTH_OFFSET = "2021-".length();
    private static final int DAY_OFFSET = "2021-05-".length();
    private static final int HOURS_OFFSET = "2021-05-04T".length();
    private static final int MINUTES_OFFSET = "2021-05-04T09:".length();
    private static final int SECONDS_OFFSET = "2021-05-04T09:30:".length();
    private static final int SECONDS_LENGTH = "2021-05-04T09:30:00".length();

    private static final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
    private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
    private static final long NANOS_PER_MILLI = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long NANOS_PER_MICRO = TimeUnit.MICROSECONDS.toNanos(1);

    private int cachedDate;
    private int cachedHours;
    private long startOfHoursNanos;

    /**
     * Constructs an empty {@code UtcDateTimeParser}.
     */
    public UtcDateTimeParser() {
    }

    /**
     * Parses the {@code buffer} containing a date-time and returns the number of nanoseconds since Unix epoch.
     * The date-time is in ISO 8601 format  (e.g., 2021-08-03T02:20:08.123Z, 2021-08-03T02:20.123456:08+00:00).
     * The subseconds component can be in seconds (0 digits), milliseconds (decimal point + 3 digits), microseconds
     * (decimal point + 6 digits), or nanoseconds (decimal point + 9 digits)
     * Returns -1 if the date-time cannot be parsed.
     *
     * @param buffer the buffer containing a date-time
     * @return nanoseconds since Unix epoch
     */
    public long parseDateTime(DirectBuffer buffer) {
        return parseDateTime(buffer, 0, buffer.capacity());
    }

    /**
     * Parses the {@code buffer} containing a date-time and returns the number of nanoseconds since Unix epoch.
     * The date-time is in ISO 8601 format (e.g., 2021-08-03T02:20:08.123Z, 2021-08-03T02:20.123456:08+00:00).
     * The subseconds component can be in seconds (0 digits), milliseconds (decimal point + 3 digits), microseconds
     * (decimal point + 6 digits), or nanoseconds (decimal point + 9 digits)
     * Returns -1 if the date-time cannot be parsed.
     *
     * @param buffer the buffer containing a date-time
     * @param index the first byte of the date-time in the buffer
     * @param length the length of the date-time
     * @return nanoseconds since Unix epoch
     */
    public long parseDateTime(DirectBuffer buffer, int index, int length) {
        if (length < SECONDS_LENGTH + 1) {
            return -1;
        }

        var year = (int) BufferNumberUtils.parseAsLong(buffer, index, 4);
        var month = (int) BufferNumberUtils.parseAsLong(buffer, index + MONTH_OFFSET, 2);
        var dayOfMonth = (int) BufferNumberUtils.parseAsLong(buffer, index + DAY_OFFSET, 2);
        var hours = (int) BufferNumberUtils.parseAsLong(buffer, index + HOURS_OFFSET, 2);
        final var minutes = (int) BufferNumberUtils.parseAsLong(buffer, index + MINUTES_OFFSET, 2);
        final var seconds = (int) BufferNumberUtils.parseAsLong(buffer, index + SECONDS_OFFSET, 2);

        var subseconds = 0L;
        var subsecondsLength = 0;
        var end = index + length;
        var position = index + SECONDS_LENGTH;
        var character = buffer.getByte(position);
        if (character == '.') {
            position++;

            while (position < end) {
                character = buffer.getByte(position);
                if (character >= '0' && character <= '9') {
                    subseconds *= 10;
                    subseconds += character - '0';
                    subsecondsLength++;
                    position++;
                } else {
                    break;
                }
            }

            // require timestamp to be 3, 6, or 9 bytes
            if (subsecondsLength == 3) {
                subseconds *= NANOS_PER_MILLI;
            } else if (subsecondsLength == 6) {
                subseconds *= NANOS_PER_MICRO;
            } else if (subsecondsLength != 9) {
                return -1;
            }
        }
        var timeZoneLength = end - position;
        if (timeZoneLength == 0) {
            return -1;
        } else if (timeZoneLength == 1) {
            if (buffer.getByte(position) != 'Z') {
                return -1;
            }
        } else {
            while (position < end) {
                character = buffer.getByte(position);
                if (character != '+' && character != '-' && character != ':' && character != '0') {
                    return  -1;
                }
                position++;
            }
        }

        var date = year * 10000 + month * 100 + dayOfMonth;
        if (cachedDate != date || cachedHours != hours) {
            if (!DatestampUtils.isValidDate(year, month, dayOfMonth)) {
                return -1;
            }
            if (hours < 0 || hours >= 24) {
                return -1;
            }

            startOfHoursNanos = NANOS_PER_SECOND * ZonedDateTime.of(
                    year, month, dayOfMonth, hours, 0, 0, 0, ZoneOffset.UTC).toEpochSecond();
            cachedDate = date;
            cachedHours = hours;
        }

        if (minutes < 0 || minutes >= 60 || seconds < 0 || seconds > 60) {
            // 60 seconds for leap seconds!
            return -1;
        }

        return startOfHoursNanos + NANOS_PER_MINUTE * minutes + NANOS_PER_SECOND * seconds + subseconds;
    }
}
