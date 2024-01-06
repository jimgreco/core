package com.core.infrastructure;

/**
 * A {@code MemoryUnit} represents memory sizes at a given unit of granularity and provides utility methods to convert
 * across units.
 * A {@code MemoryUnit} does not maintain memory size information, but only helps organize and use memory size
 * representations that may be maintained separately across various contexts.
 * A byte is the smallest unit of memory sizes.
 * A kilobyte is defined as 1024 bytes.
 * A megabyte is defined as 1024 kilobytes.
 * A gigabyte is defined as 1024 megabytes.
 *
 * <p>A {@code Memory} is mainly used to inform memory size-based methods how a given memory size parameter should be
 * interpreted.
 */
public enum MemoryUnit {

    /**
     * Memory unit representing 1 byte.
     */
    BYTES(1),
    /**
     * Memory unit representing 1 kilobyte = 1024 bytes.
     */
    KILOBYTES(1024),
    /**
     * Memory unit representing 1 megabyte = 1024 kilobytes.
     */
    MEGABYTES(1024 * 1024),
    /**
     * Memory unit representing 1 gigabyte = 1024 megabytes.
     */
    GIGABYTES(1024 * 1024 * 1024);

    private final long scale;

    MemoryUnit(long scale) {
        this.scale = scale;
    }

    /**
     * Equivalent to {@link #convert(long, MemoryUnit) convert(value, BYTES)}.
     *
     * @param value the memory size
     * @return the converted memory size
     */
    public long toBytes(long value) {
        return scale * value;
    }

    /**
     * Equivalent to {@link #convert(long, MemoryUnit) convert(value, KILOBYTES)}.
     *
     * @param value the memory size
     * @return the converted memory size
     */
    public long toKilobytes(long value) {
        return convert(value, MemoryUnit.KILOBYTES);
    }

    /**
     * Equivalent to {@link #convert(long, MemoryUnit) convert(value, MEGABYTES)}.
     *
     * @param value the memory size
     * @return the converted memory size
     */
    public long toMegabytes(long value) {
        return convert(value, MemoryUnit.MEGABYTES);
    }

    /**
     * Equivalent to {@link #convert(long, MemoryUnit) convert(value, GIGABYTES)}.
     *
     * @param value the memory size
     * @return the converted memory size
     */
    public long toGigabytes(long value) {
        return convert(value, MemoryUnit.GIGABYTES);
    }

    /**
     * Converts the given memory size in this unit to the given unit.
     * Conversions from finer to coarser granularities truncate, so lose precision.
     * For example, converting {@code 1023} bytes to kilobytes results in {@code 0}.
     * Overflows of conversions from coarser to finer granularities with arguments that would numerically overflow are
     * not handled.
     *
     * @param sourceMemory the memory size in the given {@code destUnit}
     * @param destUnit the unit of the {@code sourceDuration} argument
     * @return the converted memory size in this unit
     */
    public long convert(long sourceMemory, MemoryUnit destUnit) {
        if (scale < destUnit.scale) {
            return sourceMemory / (destUnit.scale / scale);
        } else {
            return sourceMemory * (scale / destUnit.scale);
        }
    }
}
