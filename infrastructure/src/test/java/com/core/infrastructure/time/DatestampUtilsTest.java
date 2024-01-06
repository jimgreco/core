package com.core.infrastructure.time;

import com.core.infrastructure.buffer.BufferUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

public class DatestampUtilsTest {

    @Test
    void toEpochDay_of_day_before_epoch_is_neg1() {
        then(DatestampUtils.toEpochDay(19691231)).isEqualTo(-1);
    }

    @Test
    void toEpochDay_of_epoch_is_zero() {
        then(DatestampUtils.toEpochDay(19700101)).isEqualTo(0);
    }

    @Test
    void toEpochDay() {
        then(DatestampUtils.toEpochDay(20210602)).isEqualTo(18780);
    }

    @Test
    void putAsDate_with_dashes() {
        var buffer = BufferUtils.allocate(15);

        var bytes = DatestampUtils.putAsDate(buffer, 2, 18780, true);

        then(BufferUtils.toAsciiString(buffer, 2, bytes)).isEqualTo("2021-06-02");
    }

    @Test
    void putAsDate_without_dashes() {
        var buffer = BufferUtils.allocate(15);

        var bytes = DatestampUtils.putAsDate(buffer, 2, 18780, false);

        then(BufferUtils.toAsciiString(buffer, 2, bytes)).isEqualTo("20210602");
    }
}
