package com.core.platform.fix;

import com.core.infrastructure.buffer.BufferUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.BDDAssertions.then;

public class FixDateTimeParserTest {

    @Nested
    class DatestampParsingTests {

        @Test
        void parse_date_with_offset() {
            var buffer = BufferUtils.fromAsciiString("xxxx20210602xxxxx");

            then(FixDateTimeParser.parseDate(buffer, 4, 8)).isEqualTo(18780);
        }

        @Test
        void parse_date() {
            var buffer = BufferUtils.fromAsciiString("20210602");

            then(FixDateTimeParser.parseDate(buffer, 0, 8)).isEqualTo(18780);
        }

        @Test
        void parse_invalid_date_length_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210229");

            then(FixDateTimeParser.parseDate(buffer, 5, 7)).isEqualTo(-1);
        }

        @Test
        void parse_leapYear_dayOfMonth() {
            var buffer = BufferUtils.fromAsciiString("20200229");

            then(FixDateTimeParser.parseDate(buffer, 0, 8)).isEqualTo(18321);
        }

        @Test
        void parse_invalid_nonLeapYear_dayOfMonth_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210229");

            then(FixDateTimeParser.parseDate(buffer, 0, 8)).isEqualTo(-1);
        }

        @Test
        void parse_invalid_dayOfMonth_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210431");

            then(FixDateTimeParser.parseDate(buffer, 0, 8)).isEqualTo(-1);
        }

        @Test
        void parse_invalid_dayOfMonth2_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210532");

            then(FixDateTimeParser.parseDate(buffer, 0, 8)).isEqualTo(-1);
        }

        @Test
        void parse_invalid_dayOfMonth3_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210500");

            then(FixDateTimeParser.parseDate(buffer, 0, 8)).isEqualTo(-1);
        }

        @Test
        void parse_invalid_month_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210031");

            then(FixDateTimeParser.parseDate(buffer, 0, 8)).isEqualTo(-1);
        }

        @Test
        void parse_invalid_month2_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20211301");

            then(FixDateTimeParser.parseDate(buffer, 0, 8)).isEqualTo(-1);
        }

        @Test
        void parse_invalid_year_character_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20a11201");

            then(FixDateTimeParser.parseDate(buffer, 0, 8)).isEqualTo(-1);
        }

        @Test
        void parse_invalid_month_character_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20211a01");

            then(FixDateTimeParser.parseDate(buffer, 0, 8)).isEqualTo(-1);
        }

        @Test
        void parse_invalid_day_character_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("202112a1");

            then(FixDateTimeParser.parseDate(buffer, 0, 8)).isEqualTo(-1);
        }
    }

    @Nested
    class TimestampParsingTests {

        private FixDateTimeParser parser;

        @BeforeEach
        void before_each() {
            parser = new FixDateTimeParser();
        }

        @Test
        void parse_timestamp_with_offset() {
            var buffer = BufferUtils.fromAsciiString("xxxx20210602-09:30:00.123xxxx");

            var actual = parser.parseDateTime(buffer, 4, 21);

            then(actual).isEqualTo(TimeUnit.MILLISECONDS.toNanos(1622626200123L));
        }

        @Test
        void parse_timestamp_seconds() {
            var buffer = BufferUtils.fromAsciiString("20210602-09:30:00");

            var actual = parser.parseDateTime(buffer, 0, 17);

            then(actual).isEqualTo(TimeUnit.SECONDS.toNanos(1622626200L));
        }

        @Test
        void parse_timestamp_with_cached_time() {
            var buffer = BufferUtils.fromAsciiString("20210602-09:30:00");
            parser.parseDateTime(buffer, 0, 17);
            buffer = BufferUtils.fromAsciiString("20210602-09:30:55.123");

            var actual = parser.parseDateTime(buffer, 0, 21);

            then(actual).isEqualTo(TimeUnit.MILLISECONDS.toNanos(1622626255123L));
        }

        @Test
        void parse_timestamp_with_different_date() {
            var buffer = BufferUtils.fromAsciiString("20210602-09:30:00");
            parser.parseDateTime(buffer, 0, 17);
            buffer = BufferUtils.fromAsciiString("20210603-09:30:00");

            var actual = parser.parseDateTime(buffer, 0, 17);

            then(actual).isEqualTo(TimeUnit.SECONDS.toNanos(1622712600L));
        }

        @Test
        void parse_timestamp_with_different_hour() {
            var buffer = BufferUtils.fromAsciiString("20210602-09:30:00");
            parser.parseDateTime(buffer, 0, 17);
            buffer = BufferUtils.fromAsciiString("20210602-10:30:00.123");

            var actual = parser.parseDateTime(buffer, 0, 21);

            then(actual).isEqualTo(TimeUnit.MILLISECONDS.toNanos(1622629800123L));
        }

        @Test
        void parse_timestamp_milliseconds() {
            var buffer = BufferUtils.fromAsciiString("20210602-09:30:00.123");

            var actual = parser.parseDateTime(buffer, 0, 21);

            then(actual).isEqualTo(TimeUnit.MILLISECONDS.toNanos(1622626200123L));
        }

        @Test
        void parse_timestamp_microseconds() {
            var buffer = BufferUtils.fromAsciiString("20210602-09:30:00.123456");

            var actual = parser.parseDateTime(buffer, 0, 24);

            then(actual).isEqualTo(TimeUnit.MICROSECONDS.toNanos(1622626200123456L));
        }

        @Test
        void parse_timestamp_nanoseconds() {
            var buffer = BufferUtils.fromAsciiString("20210602-09:30:00.123456789");

            var actual = parser.parseDateTime(buffer, 0, 27);

            then(actual).isEqualTo(1622626200123456789L);
        }

        @Test
        void parse_invalid_date_length_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210229-09:30:00");

            var actual = parser.parseDateTime(buffer, 0, 17);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_leapYear_dayOfMonth() {
            var buffer = BufferUtils.fromAsciiString("20200229-09:30:00");

            var actual = parser.parseDateTime(buffer, 0, 17);

            then(actual).isEqualTo(TimeUnit.SECONDS.toNanos(1582968600L));
        }

        @Test
        void parse_invalid_nonLeapYear_dayOfMonth_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210229-09:30:00");

            var actual = parser.parseDateTime(buffer, 0, 17);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_dayOfMonth_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210431-09:30:00");

            var actual = parser.parseDateTime(buffer, 0, 17);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_dayOfMonth2_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210532-09:30:00");

            var actual = parser.parseDateTime(buffer, 0, 17);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_dayOfMonth3_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210500-09:30:00");

            var actual = parser.parseDateTime(buffer, 0, 17);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_month_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210031-09:30:00");

            var actual = parser.parseDateTime(buffer, 0, 17);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_month2_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20211301-09:30:00");

            var actual = parser.parseDateTime(buffer, 0, 17);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_hour_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210131-24:30:00");

            var actual = parser.parseDateTime(buffer, 0, 17);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_hour2_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210131-1a:30:00");

            var actual = parser.parseDateTime(buffer, 0, 17);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_minutes_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210131-09:60:00");

            var actual = parser.parseDateTime(buffer, 0, 17);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_minutes2_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210131-09:a3:00");

            var actual = parser.parseDateTime(buffer, 0, 17);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_seconds_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210131-09:00:61");

            var actual = parser.parseDateTime(buffer, 0, 17);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_seconds2_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210131-09:30:5a");

            var actual = parser.parseDateTime(buffer, 0, 17);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_milliseconds_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210131-09:30:00.1a3");

            var actual = parser.parseDateTime(buffer, 0, 21);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_microseconds_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210131-09:30:00.1234a6");

            var actual = parser.parseDateTime(buffer, 0, 24);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_nanoseconds_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("20210131-09:30:00.1234567a9");

            var actual = parser.parseDateTime(buffer, 0, 27);

            then(actual).isEqualTo(-1);
        }
    }

    @Nested
    class TimeParsingTests {

        @Test
        void parse_time_with_offset() {
            var buffer = BufferUtils.fromAsciiString("xxxx09:30:00.123xxxx");

            var actual = FixDateTimeParser.parseTime(buffer, 4, 12);

            then(actual).isEqualTo(TimeUnit.MILLISECONDS.toNanos(34_200_123L));
        }

        @Test
        void parse_time_seconds() {
            var buffer = BufferUtils.fromAsciiString("09:30:00");

            var actual = FixDateTimeParser.parseTime(buffer, 0, 8);

            then(actual).isEqualTo(TimeUnit.SECONDS.toNanos(34_200));
        }

        @Test
        void parse_time_microseconds() {
            var buffer = BufferUtils.fromAsciiString("09:30:00.123456");

            var actual = FixDateTimeParser.parseTime(buffer, 0, 15);

            then(actual).isEqualTo(TimeUnit.MICROSECONDS.toNanos(34_200_123456L));
        }

        @Test
        void parse_time_nanoseconds() {
            var buffer = BufferUtils.fromAsciiString("09:30:00.123456789");

            var actual = FixDateTimeParser.parseTime(buffer, 0, 18);

            then(actual).isEqualTo(34_200_123456789L);
        }

        @Test
        void parse_invalid_hour_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("24:30:00");

            var actual = FixDateTimeParser.parseTime(buffer, 0, 8);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_hour2_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("1a:30:00");

            var actual = FixDateTimeParser.parseTime(buffer, 0, 8);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_minutes_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("09:60:00");

            var actual = FixDateTimeParser.parseTime(buffer, 0, 8);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_minutes2_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("09:a3:00");

            var actual = FixDateTimeParser.parseTime(buffer, 0, 8);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_seconds_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("09:00:61");

            var actual = FixDateTimeParser.parseTime(buffer, 0, 8);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_seconds2_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("09:30:5a");

            var actual = FixDateTimeParser.parseTime(buffer, 0, 8);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_milliseconds_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("09:30:00.1a3");

            var actual = FixDateTimeParser.parseTime(buffer, 0, 12);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_microseconds_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("09:30:00.1234a6");

            var actual = FixDateTimeParser.parseTime(buffer, 0, 15);

            then(actual).isEqualTo(-1);
        }

        @Test
        void parse_invalid_nanoseconds_returns_neg1() {
            var buffer = BufferUtils.fromAsciiString("09:30:00.1234567a9");

            var actual = FixDateTimeParser.parseTime(buffer, 0, 18);

            then(actual).isEqualTo(-1);
        }
    }
}
