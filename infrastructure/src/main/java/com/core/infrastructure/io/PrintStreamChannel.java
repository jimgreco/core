package com.core.infrastructure.io;

import org.agrona.DirectBuffer;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;

/**
 * A {@code WritableBufferChannel} that writes to a {@code PrintStream}.
 */
public class PrintStreamChannel implements WritableBufferChannel {

    private final PrintStream printStream;
    private boolean open;
    private Runnable writeListener;

    /**
     * Creates a {@code PrintStreamChannel} with the specified {@code printStream} that will be written to.
     * The channel defaults to opened.
     *
     * @param printStream the print stream
     */
    public PrintStreamChannel(PrintStream printStream) {
        this.printStream = printStream;
        open = true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() {
        open = false;
        writeListener = null;
    }

    @Override
    public int write(DirectBuffer buffer, int index, int length) throws IOException {
        if (!open) {
            throw new IOException("closed");
        }

        printStream.append(buffer.getStringWithoutLengthAscii(index, length));
        return length;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        var bytes = 0;
        for (var i = offset; i < offset + length; i++) {
            bytes += write(srcs[i]);
        }
        return bytes;
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, srcs.length);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        var bytes = new byte[src.remaining()];
        src.get(bytes);
        printStream.append(new String(bytes));
        return bytes.length;
    }

    @Override
    public void setWriteListener(Runnable listener) {
        this.writeListener = listener;
        while (writeListener != null) {
            writeListener.run();
        }
        writeListener = null;
    }
}
