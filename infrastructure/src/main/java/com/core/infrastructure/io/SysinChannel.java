package com.core.infrastructure.io;

import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;

/**
 * The {@code SysinChannel} exposes {@code System.in} as a {@code ReadableBufferChannel}.
 *
 * <p>{@code System.in} operates in a blocking mode so the channel spins up a thread that reads bytes from the stream.
 * Each byte read is copied to an internal buffer that can be read with the
 * {@link #read(MutableDirectBuffer, int, int) read} method.
 * The read listener, set with {@link #setReadListener(Runnable) setReadListener} method is also invoked until all data
 * is read out of the buffer.
 */
public class SysinChannel implements ReadableBufferChannel {

    private final MutableDirectBuffer sysInBuffer;
    private volatile int writePosition;
    private volatile boolean open;
    private volatile Runnable readListener;
    private volatile int readPosition;

    /**
     * Creates a {@code SysInChannel}.
     */
    public SysinChannel() {
        open = true;
        sysInBuffer = new ExpandableDirectByteBuffer();
        Executors.newSingleThreadExecutor().execute(() -> {

            try {
                while (open) {
                    var readByte = (byte) System.in.read();
                    sysInBuffer.putByte(writePosition, readByte);
                    writePosition++;
                    invokeReadListener();
                }
            } catch (IOException e) {
                open = false;
            }
        });
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() throws IOException {
        open = false;
    }

    @Override
    public int read(MutableDirectBuffer buffer, int index, int length) throws IOException {
        if (!open) {
            throw new IOException("closed");
        }
        var position = readPosition;
        var lengthToRead = Math.min(writePosition - position, length);
        buffer.putBytes(index, sysInBuffer, position, lengthToRead);
        readPosition += lengthToRead;
        return lengthToRead;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        var bytes = 0L;
        for (var i = offset; i < length; i++) {
            bytes += read(dsts[i]);
        }
        return bytes;
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return read(dsts, 0, dsts.length);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        var position = readPosition;
        var lengthToRead = Math.min(writePosition - position, dst.remaining());
        sysInBuffer.getBytes(position, dst, lengthToRead);
        return lengthToRead;
    }

    @Override
    public void setReadListener(Runnable listener) {
        this.readListener = listener;
        invokeReadListener();
    }

    private void invokeReadListener() {
        while (readListener != null && writePosition - readPosition > 0) {
            readListener.run();
        }
    }
}
