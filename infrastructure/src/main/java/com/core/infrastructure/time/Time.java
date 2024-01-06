package com.core.infrastructure.time;

/**
 * A source of time in nanoseconds since epoch.
 */
public interface Time {

    /**
     * Returns the time in nanoseconds since epoch (January 1, 1970).
     *
     * @return the time in nanoseconds since epoch (January 1, 1970)
     */
    long nanos();

    /**
     * Updates the time source with the current time.
     *
     * <p>This is a no-op for some {@code Time} implementations.
     */
    void updateTime();

    /**
     * Creates a new {@code Time} object that references the system-time as a source of timestamps.
     * Initially, it will attempt to return a {@code HighPrecisionTime} object.
     * If the JVM is not configured correctly (i.e., "--add-opens java.base/jdk.internal.misc=ALL-UNNAMED" is not added
     * to the VM options), then {@code SystemTime} is returned.
     *
     * @return the system time object
     */
    static Time createSystemTime() {
        Time time = new CachedHighPrecisionTime();
        try {
            time.nanos();
        } catch (IllegalAccessError e) {
            time = new SystemTime();
        }
        return time;
    }
}
