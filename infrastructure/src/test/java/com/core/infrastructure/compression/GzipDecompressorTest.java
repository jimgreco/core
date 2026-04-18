package com.core.infrastructure.compression;

import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipException;

import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.BDDAssertions.thenThrownBy;

public class GzipDecompressorTest {

    private GzipDecompressor decompressor;

    @BeforeEach
    void before_each() {
        decompressor = new GzipDecompressor();
    }

    @Test
    void decompress_returns_original_bytes() throws Exception {
        var original = "hello world, this is a test string".getBytes();
        var compressed = gzip(original);

        var decompressedBuffer = new UnsafeBuffer(new byte[1024]);
        var length = decompressor.decompress(new UnsafeBuffer(compressed), decompressedBuffer);

        then(length).isEqualTo(original.length);
        then(BufferUtils.toAsciiString(decompressedBuffer, 0, length))
                .isEqualTo(new String(original));
    }

    @Test
    void decompress_with_fextra_flag_skips_extra_field() throws Exception {
        var original = "hello world".getBytes();
        var compressed = gzip(original);
        var extraBytes = new byte[] {0x42, 0x43, 0x44, 0x45};
        var withFextra = insertFextraField(compressed, extraBytes);

        var decompressedBuffer = new UnsafeBuffer(new byte[1024]);
        var length = decompressor.decompress(new UnsafeBuffer(withFextra), decompressedBuffer);

        then(length).isEqualTo(original.length);
        then(BufferUtils.toAsciiString(decompressedBuffer, 0, length))
                .isEqualTo(new String(original));
    }

    @Test
    void decompress_truncated_throws() {
        var tooSmall = new byte[10];
        thenThrownBy(() -> decompressor.decompress(new UnsafeBuffer(tooSmall), new UnsafeBuffer(new byte[64])))
                .isInstanceOf(ZipException.class);
    }

    private static byte[] gzip(byte[] data) throws Exception {
        var baos = new ByteArrayOutputStream();
        try (var gos = new GZIPOutputStream(baos)) {
            gos.write(data);
        }
        return baos.toByteArray();
    }

    private static byte[] insertFextraField(byte[] gzip, byte[] extra) {
        // gzip header is 10 bytes fixed; we insert XLEN (2) + extra after it
        var xlen = extra.length;
        var result = new byte[gzip.length + 2 + xlen];
        System.arraycopy(gzip, 0, result, 0, 10);
        // set FEXTRA flag (bit 2) on FLG byte at offset 3
        result[3] = (byte) (result[3] | 0x04);
        // XLEN little-endian
        result[10] = (byte) (xlen & 0xff);
        result[11] = (byte) ((xlen >>> 8) & 0xff);
        // extra bytes
        System.arraycopy(extra, 0, result, 12, xlen);
        // remainder of original gzip starting at offset 10
        System.arraycopy(gzip, 10, result, 12 + xlen, gzip.length - 10);
        return result;
    }
}
