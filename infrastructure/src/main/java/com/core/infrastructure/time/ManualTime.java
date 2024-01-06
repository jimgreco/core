package com.core.infrastructure.time;

import com.core.infrastructure.command.Command;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.encoding.DateTimeNumberValueEncoder;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A {@code Time} object where the time can be manually set and advanced.
 */
public class ManualTime implements Time, Encodable {

    private final ZoneId zoneId;
    private long nanos;

    /**
     * Creates a {@code ManualTime} with the time set to the epoch (January 1, 1970) in the system timezone.
     */
    public ManualTime() {
        nanos = 0;
        zoneId = ZoneId.systemDefault();
    }

    /**
     * Creates a {@code ManualTime} object with the initial time set to the current date at the specified {@code time}
     * in the system timezone.
     *
     * @param time the initial time
     */
    public ManualTime(LocalTime time) {
        this(ZonedDateTime.of(LocalDateTime.of(LocalDate.now(), time), ZoneId.systemDefault()));
    }

    /**
     * Creates a {@code ManualTime} object with the initial time set to the specified {@code date} at midnight in the
     * system timezone.
     *
     * @param date the initial date
     */
    public ManualTime(LocalDate date) {
        this(ZonedDateTime.of(LocalDateTime.of(date, LocalTime.of(0, 0)), ZoneId.systemDefault()));
    }

    /**
     * Creates a {@code ManualTime} object with the initial time to the specified parameter.
     *
     * @param time the initial time
     */
    public ManualTime(ZonedDateTime time) {
        Objects.requireNonNull(time);
        this.nanos = TimeUnit.SECONDS.toNanos(time.toEpochSecond()) + time.getNano();
        zoneId = time.getZone();
    }

    /**
     * Sets the time in nanoseconds since epoch (January 1, 1970).
     *
     * @param nanos the time
     */
    public void setNanos(long nanos) {
        this.nanos = nanos;
    }

    /**
     * Advances the time by the specified duration.
     *
     * @param duration the duration to advance the time by
     */
    public void advanceTime(Duration duration) {
        nanos += duration.toNanos();
    }

    @Command(readOnly = true)
    @Override
    public long nanos() {
        return nanos;
    }

    @Override
    public void updateTime() {
        // do nothing
    }

    @Override
    public String toString() {
        return toEncodedString();
    }

    @Command(path = "status", readOnly = true)
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.number(nanos, new DateTimeNumberValueEncoder(zoneId, TimestampDecimals.MILLISECONDS));
    }
}
