package com.core.infrastructure.compression;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import java.util.zip.ZipException;

/**
 * The {@code GzipDecompressor} is a {@code Decompressor} that decompresses data in the GZIP file format.
 *
 * <p>See the <a href="https://datatracker.ietf.org/doc/html/rfc1952">GZIP File Format</a> for more information.
 */
public class GzipDecompressor implements Decompressor {

    private static final int ID1_OFFSET = 0;
    private static final int ID2_OFFSET = 1;
    private static final int CM_OFFSET = 2;
    private static final int FLG_OFFSET = 3;
    private static final int XLEN_OFFSET = 10;

    private static final byte ID1_VALUE = 0x1f;
    private static final byte ID2_VALUE = (byte) 0x8b;
    private static final byte CM_DEFLATE = 8;
    //private static final byte FLG_FTEXT = 0x01;
    private static final byte FLG_FHCRC = 0x02;
    private static final byte FLG_FEXTRA = 0x04;
    private static final byte FLG_FNAME = 0x08;
    private static final byte FLG_FCOMMENT = 0x10;

    private final Inflater inflator;

    public GzipDecompressor() {
        inflator = new Inflater(true);
    }

    @SuppressWarnings("PMD.EmptyWhileStmt")
    @Override
    public int decompress(DirectBuffer compressed, MutableDirectBuffer decompressed)
            throws IOException, DataFormatException {
        Objects.requireNonNull(compressed);

        if (compressed.capacity() < 18) {
            throw new ZipException("compressed not large enough for header and trailer: " + compressed.capacity());
        }
        var id1 = compressed.getByte(ID1_OFFSET);
        if (id1 != ID1_VALUE) {
            throw new ZipException("invalid ID1 value: expected=" + ID1_VALUE
                    + ", received=" + id1);
        }
        var id2 = compressed.getByte(ID2_OFFSET);
        if (id2 != ID2_VALUE) {
            throw new ZipException("invalid ID2 value: expected=" + ID2_VALUE
                    + ", received=" + id2);
        }
        var cm = compressed.getByte(CM_OFFSET);
        if (cm != CM_DEFLATE) {
            throw new ZipException("invalid CM value: expected=" + CM_DEFLATE
                    + ", received=" + cm);
        }

        var offset = XLEN_OFFSET;
        var flg = compressed.getByte(FLG_OFFSET);
        if ((flg & FLG_FEXTRA) != 0) {
            var xlen = compressed.getShort(XLEN_OFFSET, ByteOrder.LITTLE_ENDIAN);
            offset += xlen;
        }
        if ((flg & FLG_FNAME) != 0) {
            while (offset < compressed.capacity() && compressed.getByte(offset++) != 0) {
                // search for null terminated string
            }
        }
        if ((flg & FLG_FCOMMENT) != 0) {
            while (offset < compressed.capacity() && compressed.getByte(offset++) != 0) {
                // search for null terminated string
            }
        }
        if ((flg & FLG_FHCRC) != 0) {
            // skip the header CRC
            offset += 2;
        }

        var startOfContent = offset;
        var endOfContent = compressed.capacity() - 8;
        if (endOfContent < startOfContent) {
            throw new ZipException("compressed not large enough for CRC32 and ISIZE fields: " + compressed.capacity());
        }

        inflator.reset();
        inflator.setInput(compressed.byteArray(),
                compressed.wrapAdjustment() + startOfContent,
                endOfContent - startOfContent);
        var decompressedLength = inflator.inflate(
                decompressed.byteArray(), decompressed.wrapAdjustment(), decompressed.capacity());

        // skip the trailer CRC
        //var crc32 = compressed.getInt(compressed.capacity() - 8, ByteOrder.LITTLE_ENDIAN);

        var isize = compressed.getInt(compressed.capacity() - 4, ByteOrder.LITTLE_ENDIAN);
        if (isize != decompressedLength) {
            throw new ZipException("invalid ISIZE: expected=" + decompressed + ", received=" + isize);
        }

        return decompressedLength;
    }
}
