package com.core.infrastructure;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

public class MemoryUnitTest {

    @Test
    void bytes_conversion() {
        then(MemoryUnit.BYTES.toBytes(123)).isEqualTo(123);
        then(MemoryUnit.BYTES.toKilobytes(123)).isEqualTo(0);
        then(MemoryUnit.BYTES.toKilobytes(2049)).isEqualTo(2);
        then(MemoryUnit.BYTES.toMegabytes(10 * 1024 * 1024)).isEqualTo(10);
        then(MemoryUnit.BYTES.toGigabytes(5L * 1024 * 1024 * 1024 + 5)).isEqualTo(5);
    }

    @Test
    void kilobytes_conversion() {
        then(MemoryUnit.KILOBYTES.toBytes(123)).isEqualTo(123 * 1024);
        then(MemoryUnit.KILOBYTES.toKilobytes(123)).isEqualTo(123);
        then(MemoryUnit.KILOBYTES.toMegabytes(1024 - 5)).isEqualTo(0);
        then(MemoryUnit.KILOBYTES.toMegabytes(10 * 1024 + 3)).isEqualTo(10);
        then(MemoryUnit.KILOBYTES.toGigabytes(5L * 1024 * 1024 + 5)).isEqualTo(5);
    }

    @Test
    void megabytes_conversion() {
        then(MemoryUnit.MEGABYTES.toBytes(123)).isEqualTo(123 * 1024 * 1024);
        then(MemoryUnit.MEGABYTES.toKilobytes(123)).isEqualTo(123 * 1024);
        then(MemoryUnit.MEGABYTES.toMegabytes(10)).isEqualTo(10);
        then(MemoryUnit.MEGABYTES.toGigabytes(1023)).isEqualTo(0);
        then(MemoryUnit.MEGABYTES.toGigabytes(5L * 1024 + 5)).isEqualTo(5);
    }

    @Test
    void gigabytes_conversion() {
        then(MemoryUnit.GIGABYTES.toBytes(123)).isEqualTo(123 * 1024 * 1024 * 1024L);
        then(MemoryUnit.GIGABYTES.toKilobytes(123)).isEqualTo(123 * 1024 * 1024);
        then(MemoryUnit.GIGABYTES.toMegabytes(10)).isEqualTo(10 * 1024);
        then(MemoryUnit.GIGABYTES.toGigabytes(13)).isEqualTo(13);
    }
}
