package com.core.infrastructure.time;

import com.core.infrastructure.buffer.BufferUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.BDDAssertions.then;

public class UtcDateTimeParserTest {

    private UtcDateTimeParser parser;

    @BeforeEach
    void before_each() {
        parser = new UtcDateTimeParser();
    }

    @Test
    void parse_timestamp_with_offset() {
        var buffer = BufferUtils.fromAsciiString("xxxx2021-06-02T09:30:00.123Zxxxx");

        var actual = parser.parseDateTime(buffer, 4, 24);

        then(actual).isEqualTo(TimeUnit.MILLISECONDS.toNanos(1622626200123L));
    }

    @Test
    void parse_timestamp_seconds() {
        var buffer = BufferUtils.fromAsciiString("2021-06-02T09:30:00Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(TimeUnit.SECONDS.toNanos(1622626200L));
    }

    @Test
    void parse_timestamp_seconds_with_timezone() {
        var buffer = BufferUtils.fromAsciiString("2021-06-02T09:30:00+00:00");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(TimeUnit.SECONDS.toNanos(1622626200L));
    }

    @Test
    void parse_timestamp_seconds_with_negative_timezone() {
        var buffer = BufferUtils.fromAsciiString("2021-06-02T09:30:00-00:00");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(TimeUnit.SECONDS.toNanos(1622626200L));
    }

    @Test
    void parse_timestamp_with_non_utc_timezone_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("2021-06-02T09:30:00-04:00");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }

    @Test
    void parse_timestamp_with_non_invalid_timezone_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("2021-06-02T09:30:00-0a:00");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }

    @Test
    void parse_timestamp_with_cached_time() {
        var buffer = BufferUtils.fromAsciiString("2021-06-02T09:30:00Z");
        parser.parseDateTime(buffer);
        buffer = BufferUtils.fromAsciiString("2021-06-02T09:30:55.123Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(TimeUnit.MILLISECONDS.toNanos(1622626255123L));
    }

    @Test
    void parse_timestamp_with_different_date() {
        var buffer = BufferUtils.fromAsciiString("2021-06-02T09:30:00Z");
        parser.parseDateTime(buffer);
        buffer = BufferUtils.fromAsciiString("2021-06-03T09:30:00Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(TimeUnit.SECONDS.toNanos(1622712600L));
    }

    @Test
    void parse_timestamp_with_different_hour() {
        var buffer = BufferUtils.fromAsciiString("2021-06-02T09:30:00Z");
        parser.parseDateTime(buffer);
        buffer = BufferUtils.fromAsciiString("2021-06-02T10:30:00.123Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(TimeUnit.MILLISECONDS.toNanos(1622629800123L));
    }

    @Test
    void parse_timestamp_milliseconds() {
        var buffer = BufferUtils.fromAsciiString("2021-06-02T09:30:00.123Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(TimeUnit.MILLISECONDS.toNanos(1622626200123L));
    }

    @Test
    void parse_timestamp_microseconds() {
        var buffer = BufferUtils.fromAsciiString("2021-06-02T09:30:00.123456Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(TimeUnit.MICROSECONDS.toNanos(1622626200123456L));
    }

    @Test
    void parse_timestamp_nanoseconds() {
        var buffer = BufferUtils.fromAsciiString("2021-06-02T09:30:00.123456789Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(1622626200123456789L);
    }

    @Test
    void parse_invalid_date_length_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("2021-02-29T09:30:00Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }

    @Test
    void parse_leapYear_dayOfMonth() {
        var buffer = BufferUtils.fromAsciiString("2020-02-29T09:30:00Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(TimeUnit.SECONDS.toNanos(1582968600L));
    }

    @Test
    void parse_invalid_nonLeapYear_dayOfMonth_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("2021-02-29T09:30:00Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }

    @Test
    void parse_invalid_dayOfMonth_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("2021-04-31T09:30:00Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }

    @Test
    void parse_invalid_dayOfMonth2_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("2021-05-32T09:30:00Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }

    @Test
    void parse_invalid_dayOfMonth3_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("2021-05-00T09:30:00Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }

    @Test
    void parse_invalid_month_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("2021-00-31T09:30:00Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }

    @Test
    void parse_invalid_month2_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("2021-13-01T09:30:00Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }

    @Test
    void parse_invalid_hour_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("2021-01-31T24:30:00Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }

    @Test
    void parse_invalid_hour2_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("2021-01-31T1a:30:00Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }

    @Test
    void parse_invalid_minutes_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("202-10-131T09:60:00Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }

    @Test
    void parse_invalid_minutes2_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("2021-01-31T09:a3:00Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }

    @Test
    void parse_invalid_seconds_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("2021-01-31T09:00:61Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }

    @Test
    void parse_invalid_seconds2_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("2021-01-31T09:30:5aZ");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }

    @Test
    void parse_invalid_milliseconds_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("2021-01-31T09:30:00.1a3Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }

    @Test
    void parse_invalid_microseconds_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("2021-01-31T09:30:00.1234a6Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }

    @Test
    void parse_invalid_nanoseconds_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("2021-01-31T09:30:00.1234567a9Z");

        var actual = parser.parseDateTime(buffer);

        then(actual).isEqualTo(-1);
    }
}
