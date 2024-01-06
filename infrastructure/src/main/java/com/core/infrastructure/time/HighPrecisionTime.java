package com.core.infrastructure.time;

import com.core.infrastructure.command.Command;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.EncoderUtils;
import com.core.infrastructure.encoding.ObjectEncoder;
import jdk.internal.misc.VM;

import java.util.concurrent.TimeUnit;

/**
 * A {@code Time} object that uses the VM's high-precision timer.
 */
public class HighPrecisionTime implements Time, Encodable {

    private final long offset;
    private final long timestampOffset;

    /**
     * Creates an empty {@code HighPrecisionTime}.
     */
    public HighPrecisionTime() {
        offset = System.currentTimeMillis() / 1000 - 1024;
        timestampOffset = TimeUnit.SECONDS.toNanos(offset);
    }

    @Command(readOnly = true)
    @Override
    public long nanos() {
        long adjustment = VM.getNanoTimeAdjustment(offset);
        return timestampOffset + adjustment;
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
        encoder.number(nanos(), EncoderUtils.NANOSECOND_ENCODER);
    }
}
