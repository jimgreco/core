package com.core.infrastructure.time;

/**
 * The number of timestamp decimals.
 */
public enum TimestampDecimals {

    /**
     * Zero timestamp decimals.
     */
    SECONDS(0, 1_000_000_000),
    /**
     * Three timestamp decimals.
     */
    MILLISECONDS(3, 1_000_000),
    /**
     * Six timestamp decimals.
     */
    MICROSECONDS(6, 1_000),
    /**
     * Nine timestamp decimals.
     */
    NANOSECONDS(9, 1);

    private final int decimals;
    private final int nanos;

    TimestampDecimals(int decimals, int nanos) {
        this.decimals = decimals;
        this.nanos = nanos;
    }

    /**
     * Returns the number of decimals in this time unit.
     *
     * @return the number of decimals in this time unit
     */
    public int getDecimals() {
        return decimals;
    }

    /**
     * Returns the number of nanoseconds in this time unit.
     *
     * @return the number of nanoseconds in this time unit
     */
    public int getNanos() {
        return nanos;
    }
}
