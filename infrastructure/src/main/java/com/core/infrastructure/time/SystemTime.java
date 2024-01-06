package com.core.infrastructure.time;

import com.core.infrastructure.command.Command;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.EncoderUtils;
import com.core.infrastructure.encoding.ObjectEncoder;

import java.util.concurrent.TimeUnit;

/**
 * A {@code Time} object that returns the system time.
 */
public class SystemTime implements Time, Encodable {

    private static final long NANOS_PER_MILLI = TimeUnit.MILLISECONDS.toNanos(1);

    /**
     * Creates an empty {@code SystemTime}.
     */
    public SystemTime() {
    }

    @Command(readOnly = true)
    @Override
    public long nanos() {
        return NANOS_PER_MILLI * System.currentTimeMillis();
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
        encoder.number(nanos(), EncoderUtils.MILLISECOND_ENCODER);
    }
}
