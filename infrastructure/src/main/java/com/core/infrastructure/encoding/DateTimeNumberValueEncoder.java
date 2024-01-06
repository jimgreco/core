package com.core.infrastructure.encoding;

import com.core.infrastructure.time.TimestampDecimals;
import com.core.infrastructure.time.TimestampFormatter;
import org.agrona.MutableDirectBuffer;

import java.time.ZoneId;

/**
 * The {@code TimestampNumberValueEncoder} writes a timestamp, encoded as nanoseconds since epoch, January 1, 1970, in
 * the ISO date time output (i.e., {@code yyyy-MM-ddTHH:mm:ss[.SSSSSSSSS](Z|[+-]HH:mm:ss)}).
 */
public class DateTimeNumberValueEncoder implements NumberValueEncoder {

    private final TimestampFormatter timestampFormatter;

    /**
     * Constructs a {@code TimestampNumberValueEncoder} with the specified time zone and number of timestamp decimals.
     *
     * @param zoneId the time zone to display
     * @param timestampDecimals the number of decimals in the timestamp
     */
    public DateTimeNumberValueEncoder(ZoneId zoneId, TimestampDecimals timestampDecimals) {
        timestampFormatter = new TimestampFormatter(zoneId, timestampDecimals);
    }

    @Override
    public int encode(ValueEncoder encoder, MutableDirectBuffer buffer, int index, long number) {
        if (number == 0) {
            return encoder.writeNull(buffer, index);
        } else {
            var length = encoder.writePreString(buffer, index, false);
            length += timestampFormatter.writeDateTime(buffer, index + length, number);
            length += encoder.writePostString(buffer, index + length, false);
            return length;
        }
    }
}
