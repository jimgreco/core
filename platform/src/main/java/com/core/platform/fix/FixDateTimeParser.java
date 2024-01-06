package com.core.platform.fix;

import com.core.infrastructure.buffer.BufferNumberUtils;
import com.core.infrastructure.time.DatestampUtils;
import org.agrona.DirectBuffer;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;

final class FixDateTimeParser {

    private static final int DATETIME_DATE_OFFSET = 0;
    private static final int DATETIME_HOURS_OFFSET = "20210504-".length();
    private static final int DATETIME_MINUTES_OFFSET = "20210504-09:".length();
    private static final int DATETIME_SECONDS_OFFSET = "20210504-09:30:".length();
    private static final int DATETIME_SUBSECONDS_OFFSET = "20210504-09:30:00.".length();

    private static final int DATETIME_DATE_LENGTH = "20210504".length();
    private static final int DATETIME_SECONDS_LENGTH = "20210504-09:30:00".length();
    private static final int DATETIME_MILLISECONDS_LENGTH = "20210504-09:30:00.000".length();
    private static final int DATETIME_MICROSECONDS_LENGTH = "20210504-09:30:00.000000".length();
    private static final int DATETIME_NANOSECONDS_LENGTH = "20210504-09:30:00.000000000".length();

    private static final int TIME_HOURS_OFFSET = 0;
    private static final int TIME_MINUTES_OFFSET = "09:".length();
    private static final int TIME_SECONDS_OFFSET = "09:30:".length();
    private static final int TIME_SUBSECONDS_OFFSET = "09:30:00.".length();

    private static final int TIME_SECONDS_LENGTH = "09:30:00".length();
    private static final int TIME_MILLISECONDS_LENGTH = "09:30:00.000".length();
    private static final int TIME_MICROSECONDS_LENGTH = "09:30:00.000000".length();
    private static final int TIME_NANOSECONDS_LENGTH = "09:30:00.000000000".length();

    private static final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
    private static final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
    private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
    private static final long NANOS_PER_MILLI = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long NANOS_PER_MICRO = TimeUnit.MICROSECONDS.toNanos(1);

    private int cachedDate;
    private int cachedHours;
    private long startOfHoursNanos;

    FixDateTimeParser() {
    }

    long parseDateTime(DirectBuffer buffer, int index, int length) {
        if (length < DATETIME_SECONDS_LENGTH) {
            return -1;
        }

        var date = (int) BufferNumberUtils.parseAsLong(buffer, index + DATETIME_DATE_OFFSET, DATETIME_DATE_LENGTH);
        var hours = (int) BufferNumberUtils.parseAsLong(buffer, index + DATETIME_HOURS_OFFSET, 2);
        var minutes = (int) BufferNumberUtils.parseAsLong(buffer, index + DATETIME_MINUTES_OFFSET, 2);
        var seconds = (int) BufferNumberUtils.parseAsLong(buffer, index + DATETIME_SECONDS_OFFSET, 2);

        var subseconds = -1L;
        var subsecondsMultiplier = -1L;
        if (length == DATETIME_MILLISECONDS_LENGTH) {
            subseconds = BufferNumberUtils.parseAsLong(buffer, index + DATETIME_SUBSECONDS_OFFSET, 3);
            subsecondsMultiplier = NANOS_PER_MILLI;
        } else if (length == DATETIME_MICROSECONDS_LENGTH) {
            subseconds = BufferNumberUtils.parseAsLong(buffer, index + DATETIME_SUBSECONDS_OFFSET, 6);
            subsecondsMultiplier = NANOS_PER_MICRO;
        } else if (length == DATETIME_NANOSECONDS_LENGTH)  {
            subseconds = BufferNumberUtils.parseAsLong(buffer, index + DATETIME_SUBSECONDS_OFFSET, 9);
            subsecondsMultiplier = 1;
        } else if (length == DATETIME_SECONDS_LENGTH) {
            subseconds = 0;
            subsecondsMultiplier = 0;
        }

        if (date == -1 || minutes < 0 || minutes >= 60 || seconds < 0 || seconds > 60 || subseconds == -1) {
            // 60 seconds for leap seconds!
            return -1;
        }

        if (cachedDate != date || cachedHours != hours) {
            var year = (int) BufferNumberUtils.parseAsLong(buffer, index, 4);
            var month = (int) BufferNumberUtils.parseAsLong(buffer, index + 4, 2);
            var dayOfMonth = (int) BufferNumberUtils.parseAsLong(buffer, index + 6, 2);

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

        return startOfHoursNanos
                + NANOS_PER_MINUTE * minutes
                + NANOS_PER_SECOND * seconds
                + subsecondsMultiplier * subseconds;
    }

    static int parseDate(DirectBuffer buffer, int index, int length) {
        if (length != DATETIME_DATE_LENGTH) {
            return -1;
        }

        var year = (int) BufferNumberUtils.parseAsLong(buffer, index, 4);
        var month = (int) BufferNumberUtils.parseAsLong(buffer, index + 4, 2);
        var dayOfMonth = (int) BufferNumberUtils.parseAsLong(buffer, index + 6, 2);

        if (DatestampUtils.isValidDate(year, month, dayOfMonth)) {
            return (int) DatestampUtils.toEpochDay(year, month, dayOfMonth);
        } else {
            return -1;
        }
    }

    static long parseTime(DirectBuffer buffer, int index, int length) {
        if (length < TIME_SECONDS_LENGTH) {
            return -1;
        }

        var hours = (int) BufferNumberUtils.parseAsLong(buffer, index + TIME_HOURS_OFFSET, 2);
        var minutes = (int) BufferNumberUtils.parseAsLong(buffer, index + TIME_MINUTES_OFFSET, 2);
        var seconds = (int) BufferNumberUtils.parseAsLong(buffer, index + TIME_SECONDS_OFFSET, 2);

        var subseconds = -1L;
        var subsecondsMultiplier = -1L;
        if (length == TIME_MILLISECONDS_LENGTH) {
            subseconds = BufferNumberUtils.parseAsLong(buffer, index + TIME_SUBSECONDS_OFFSET, 3);
            subsecondsMultiplier = NANOS_PER_MILLI;
        } else if (length == TIME_MICROSECONDS_LENGTH) {
            subseconds = BufferNumberUtils.parseAsLong(buffer, index + TIME_SUBSECONDS_OFFSET, 6);
            subsecondsMultiplier = NANOS_PER_MICRO;
        } else if (length == TIME_NANOSECONDS_LENGTH)  {
            subseconds = BufferNumberUtils.parseAsLong(buffer, index + TIME_SUBSECONDS_OFFSET, 9);
            subsecondsMultiplier = 1;
        } else if (length == TIME_SECONDS_LENGTH) {
            subseconds = 0;
            subsecondsMultiplier = 0;
        }

        if (hours < 0 || hours >= 24
                || minutes < 0 || minutes >= 60
                || seconds < 0 || seconds > 60
                || subseconds == -1) {
            // 60 seconds for leap seconds!
            return -1;
        }

        return hours * NANOS_PER_HOUR
                + minutes * NANOS_PER_MINUTE
                + seconds * NANOS_PER_SECOND
                + subseconds * subsecondsMultiplier;
    }
}
