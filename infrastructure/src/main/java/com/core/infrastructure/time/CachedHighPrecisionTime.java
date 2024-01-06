package com.core.infrastructure.time;

import com.core.infrastructure.command.Command;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.EncoderUtils;
import com.core.infrastructure.encoding.ObjectEncoder;
import jdk.internal.misc.VM;

import java.util.concurrent.TimeUnit;

/**
 * A {@code Time} object that uses the VM's high-precision timer.
 *
 * <p>The timestamp is cached in the {@link #updateTime()}.
 * Subsequent calls to {@link #nanos()} will return the cached value.
 */
public class CachedHighPrecisionTime implements Time, Encodable {

    private final long offset;
    private final long timestampOffset;
    private long cachedTime;

    /**
     * Creates an empty {@code CachedHighPrecisionTime}.
     */
    public CachedHighPrecisionTime() {
        offset = System.currentTimeMillis() / 1000 - 1024;
        timestampOffset = TimeUnit.SECONDS.toNanos(offset);
        updateTime();
    }

    @Override
    public void updateTime() {
        cachedTime = timestampOffset + VM.getNanoTimeAdjustment(offset);
    }

    @Command(readOnly = true)
    @Override
    public long nanos() {
        return cachedTime;
    }

    @Override
    public String toString() {
        return toEncodedString();
    }

    @Command(path = "status", readOnly = true)
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.number(nanos(), EncoderUtils.NANOSECOND_ENCODER);
    }
}
