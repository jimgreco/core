package com.core.infrastructure.time;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.BDDAssertions.then;

public class ManualTimeTest {

    @Test
    public void toString_outputs_iso_date_time() {
        var dateTime = LocalDateTime.of(2020, 12, 14, 9, 30, 0, 123456789);
        var manualTime = new ManualTime(ZonedDateTime.of(dateTime, ZoneOffset.ofHours(1)));
        manualTime.advanceTime(Duration.ofSeconds(35));

        var actual = manualTime.toString();

        then(actual).isEqualTo("'2020-12-14T09:30:35.123+01:00'");
    }

    @Test
    void setNanos_and_advanceTime() {
        var manualTime = new ManualTime();
        manualTime.setNanos(100);

        manualTime.advanceTime(Duration.ofSeconds(44));

        then(manualTime.nanos()).isEqualTo(100 + TimeUnit.SECONDS.toNanos(44));
    }
}
