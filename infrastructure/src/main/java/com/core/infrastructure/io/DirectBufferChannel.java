package com.core.infrastructure.io;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.MemoryUnit;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@code SeekableBufferChannel} that is backed by an expandable buffer.
 */
public class DirectBufferChannel implements SeekableBufferChannel, Encodable {

    private final MutableDirectBuffer buffer;
    private boolean open;
    private int position;
    private int size;
    private Runnable readListener;
    private Runnable writeListener;

    /**
     * Creates a {@code DirectBufferChannel} with the initial contents set to the string parameter, a {@code size()}
     * equal to the length of the string, and a {@code position()} of 0.
     *
     * @param string the initial contents of the channel
     */
    public DirectBufferChannel(String string) {
        this(string.length());
        this.buffer.putStringWithoutLengthAscii(0, string);
        size = string.length();
    }

    /**
     * Creates a {@code DirectBufferFileChannel} with an initial size of 1kb.
     */
    public DirectBufferChannel() {
        this((int) MemoryUnit.KILOBYTES.toBytes(1));
    }

    /**
     * Creates a {@code DirectBufferFileChannel} with the specified initial size.
     *
     * @param initialSize the initial size of the buffer
     */
    public DirectBufferChannel(int initialSize) {
        open = true;
        buffer = BufferUtils.allocateExpandable(initialSize);
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() throws IOException {
        open = false;
        readListener = null;
        writeListener = null;
    }
    
    @Override
    public int read(MutableDirectBuffer buffer, int index, int lenth) throws IOException {
        var lengthRead = read(position, buffer, index, lenth);
        if (lengthRead != -1) {
            position += lengthRead;
        }
        return lengthRead;
    }

    @Override
    public int read(long filePosition, MutableDirectBuffer buffer, int index, int length) throws IOException {
        checkClosed();
        if (filePosition >= size) {
            return -1;
        }

        var lengthToRead = Math.min(size - (int) filePosition, length);
        this.buffer.getBytes((int) filePosition, buffer, index, lengthToRead);
        return lengthToRead;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        checkClosed();
        var bytes = 0L;
        for (var i = offset; i < length; i++) {
            bytes += read(dsts[i]);
        }
        return bytes;
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        checkClosed();
        return read(dsts, 0, dsts.length);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        checkClosed();
        if (position >= size) {
            return -1;
        }

        var bytes = Math.min(dst.remaining(), size - position);
        buffer.getBytes(position, dst, bytes);
        position += bytes;
        return bytes;
    }

    @Override
    public int write(DirectBuffer buffer, int index, int length) throws IOException {
        var bytesWritten = write(position, buffer, index, length);
        position += bytesWritten;
        return bytesWritten;
    }

    @Override
    public int write(long filePosition, DirectBuffer buffer, int index, int length) throws IOException {
        checkClosed();
        this.buffer.putBytes((int) filePosition, buffer, index, length);
        size = Math.max((int) filePosition + length, size);
        return length;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        checkClosed();
        var bytes = 0L;
        for (var i = offset; i < length; i++) {
            bytes += write(srcs[i]);
        }
        return bytes;
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        checkClosed();
        return write(srcs, 0, srcs.length);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        checkClosed();
        var bytes = src.remaining();
        buffer.putBytes(position, src, bytes);
        position += bytes;
        size = Math.max(this.position, size);
        return bytes;
    }

    @Override
    public void setReadListener(Runnable listener) {
        this.readListener = listener;
        while (readListener != null) {
            readListener.run();
        }
    }

    @Override
    public void setWriteListener(Runnable listener) {
        this.writeListener = listener;
        while (writeListener != null) {
            writeListener.run();
        }
    }

    @Override
    public long position() throws IOException {
        checkClosed();
        return position;
    }

    @Override
    public DirectBufferChannel position(long position) throws IOException {
        checkClosed();
        if (position < 0) {
            throw new IllegalArgumentException("negative position: " + position);
        }

        this.position = (int) position;
        size = Math.max(this.position, size);
        return this;
    }

    @Override
    public long size() throws IOException {
        checkClosed();
        return size;
    }

    @Override
    public DirectBufferChannel truncate(long size) throws IOException {
        checkClosed();
        if (size < 0) {
            throw new IllegalArgumentException("negative size: " + size);
        }

        var newSize = (int) size;
        if (newSize < this.size) {
            this.size = newSize;
            if (position > newSize) {
                position = newSize;
            }
        }

        return this;
    }

    private void checkClosed() throws IOException {
        if (!open) {
            throw new IOException("closed");
        }
    }

    /**
     * Returns the underlying buffer.
     *
     * @return the underlying buffer
     */
    public MutableDirectBuffer getBuffer() {
        return buffer;
    }

    @Override
    public String toString() {
        return toEncodedString();
    }

    @Command(path = "status", readOnly = true)
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("type").string(getClass().getSimpleName())
                .string("open").bool(open)
                .string("position").number(position)
                .string("size").object(size)
                .closeMap();
    }
}
