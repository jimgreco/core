package com.core.infrastructure.time;

import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static org.assertj.core.api.BDDAssertions.then;

public class TimestampFormatterTest {

    private TimestampFormatter formatter;
    private MutableDirectBuffer buffer;

    @BeforeEach
    void before_each() {
        buffer = BufferUtils.allocate(100);
    }

    @Nested
    class DateTimeTests {

        @Test
        void format_second_timestamp() {
            formatter = new TimestampFormatter(ZoneId.of("-05:00"), TimestampDecimals.SECONDS);

            var length = formatter.writeDateTime(buffer, 17, 1611931768_123456789L);

            then(BufferUtils.toAsciiString(buffer, 17, length)).isEqualTo("2021-01-29T09:49:28-05:00");
        }

        @Test
        void format_millisecond_timestamp() {
            formatter = new TimestampFormatter(ZoneId.of("-05:00"), TimestampDecimals.MILLISECONDS);

            var length = formatter.writeDateTime(buffer, 17, 1611931768_123456789L);

            then(BufferUtils.toAsciiString(buffer, 17, length)).isEqualTo("2021-01-29T09:49:28.123-05:00");
        }

        @Test
        void format_microsecond_timestamp() {
            formatter = new TimestampFormatter(ZoneId.of("Z"), TimestampDecimals.MICROSECONDS);

            var length = formatter.writeDateTime(buffer, 26, 1611931768_123456789L);

            then(BufferUtils.toAsciiString(buffer, 26, length)).isEqualTo("2021-01-29T14:49:28.123456Z");
        }

        @Test
        void format_nanosecond_timestamp() {
            formatter = new TimestampFormatter(ZoneId.of("+01:00"), TimestampDecimals.NANOSECONDS);

            var length = formatter.writeDateTime(buffer, 12, 1611931768_123456789L);

            then(BufferUtils.toAsciiString(buffer, 12, length)).isEqualTo("2021-01-29T15:49:28.123456789+01:00");
        }
    }

    @Nested
    class TimeTests {

        @Test
        void format_second_timestamp() {
            formatter = new TimestampFormatter(ZoneId.of("-05:00"), TimestampDecimals.SECONDS);

            var length = formatter.writeTime(buffer, 17, 1611931768_123456789L);

            then(BufferUtils.toAsciiString(buffer, 17, length)).isEqualTo("09:49:28");
        }

        @Test
        void format_millisecond_timestamp() {
            formatter = new TimestampFormatter(ZoneId.of("-05:00"), TimestampDecimals.MILLISECONDS);

            var length = formatter.writeTime(buffer, 17, 1611931768_123456789L);

            then(BufferUtils.toAsciiString(buffer, 17, length)).isEqualTo("09:49:28.123");
        }

        @Test
        void format_microsecond_timestamp() {
            formatter = new TimestampFormatter(ZoneId.of("Z"), TimestampDecimals.MICROSECONDS);

            var length = formatter.writeTime(buffer, 26, 1611931768_123456789L);

            then(BufferUtils.toAsciiString(buffer, 26, length)).isEqualTo("14:49:28.123456");
        }

        @Test
        void format_nanosecond_timestamp() {
            formatter = new TimestampFormatter(ZoneId.of("+01:00"), TimestampDecimals.NANOSECONDS);

            var length = formatter.writeTime(buffer, 12, 1611931768_123456789L);

            then(BufferUtils.toAsciiString(buffer, 12, length)).isEqualTo("15:49:28.123456789");
        }
    }
}
