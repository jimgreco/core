package com.core.infrastructure.compression;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.util.zip.DataFormatException;

/**
 * A {@code Decompressor} is an interface for reading compressed data.
 */
public interface Decompressor {

    /**
     * Decompresses the specified {@code compressed} buffer and returns the number of bytes written to the
     * {@code decompressed} buffer.
     *
     * @param compressed the compressed buffer
     * @param decompressed the decompressed buffer
     * @return the number of bytes written to the {@code decompressed} buffer
     * @throws IOException if an I/O error occurs
     * @throws DataFormatException if the data is not in the expected compression format
     */
    int decompress(DirectBuffer compressed, MutableDirectBuffer decompressed)
            throws IOException, DataFormatException;
}
