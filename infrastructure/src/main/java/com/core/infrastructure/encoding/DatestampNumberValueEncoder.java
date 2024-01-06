package com.core.infrastructure.encoding;

import com.core.infrastructure.time.DatestampUtils;
import org.agrona.MutableDirectBuffer;

class DatestampNumberValueEncoder implements NumberValueEncoder {

    DatestampNumberValueEncoder() {
    }

    @Override
    public int encode(ValueEncoder encoder, MutableDirectBuffer buffer, int index, long number) {
        var position = index;
        if (number == 0) {
            encoder.writeNull(buffer, index);
        } else {
            position += encoder.writePreString(buffer, position, false);
            position += DatestampUtils.putAsDate(buffer, position, number, true);
            position += encoder.writePostString(buffer, position, false);
        }
        return position - index;
    }
}
