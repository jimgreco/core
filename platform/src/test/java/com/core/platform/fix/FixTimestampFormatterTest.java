package com.core.platform.fix;

import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

public class FixTimestampFormatterTest {

    private FixTimestampFormatter formatter;
    private MutableDirectBuffer buffer;

    @BeforeEach
    void before_each() {
        formatter = new FixTimestampFormatter();
        buffer = BufferUtils.allocate(1024);
    }

    @Test
    void format_as_UTC_FIX_date() {
        var bytes = formatter.writeDate(buffer, 5, 1621951065123456789L);

        var string = BufferUtils.toAsciiString(buffer, 5, bytes);
        then(string).isEqualTo("20210525");
    }

    @Test
    void format_as_UTC_FIX_timestamp() {
        var bytes = formatter.writeDateTime(buffer, 5, 1621951065123456789L);

        var string = BufferUtils.toAsciiString(buffer, 5, bytes);
        then(string).isEqualTo("20210525-13:57:45.123");
    }
}
