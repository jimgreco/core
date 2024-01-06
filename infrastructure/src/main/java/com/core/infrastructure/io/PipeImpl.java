package com.core.infrastructure.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

class PipeImpl implements Pipe {

    private final SourceImpl source;
    private final SinkImpl sink;

    PipeImpl(Selector selector, java.nio.channels.Pipe pipe) {
        source = new SourceImpl(selector, pipe);
        sink = new SinkImpl(selector, pipe);
    }

    @Override
    public SourceChannel getSource() {
        return source;
    }

    @Override
    public SinkChannel getSink() {
        return sink;
    }

    private static class SourceImpl implements SourceChannel, NioSelector.SelectableChannel {

        private final java.nio.channels.Pipe.SourceChannel channel;
        private final Selector selector;
        private SelectionKey selectionKey;
        private Runnable readListener;

        private SourceImpl(Selector selector, java.nio.channels.Pipe pipe) {
            this.selector = selector;
            this.channel = pipe.source();
        }

        @Override
        public boolean isOpen() {
            return channel.isOpen();
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

        @Override
        public void configureBlocking(boolean block) throws IOException {
            channel.configureBlocking(block);
            if (!block) {
                selectionKey = channel.register(selector, 0, this);
            }
        }

        @Override
        public boolean isBlocking() {
            return channel.isBlocking();
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            return channel.read(dsts, offset, length);
        }

        @Override
        public long read(ByteBuffer[] dsts) throws IOException {
            return channel.read(dsts);
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return channel.read(dst);
        }

        @Override
        public void setReadListener(Runnable readListener) {
            this.readListener = readListener;
            if (selectionKey.isValid()) {
                if (readListener == null) {
                    selectionKey.interestOpsAnd(~SelectionKey.OP_READ);
                } else {
                    selectionKey.interestOpsOr(SelectionKey.OP_READ);
                }
            }
        }

        @Override
        public void onRead() {
            readListener.run();
        }

        @Override
        public void onWrite() {
            // does not write
        }

        @Override
        public void onAccept() {
            // does not accept
        }

        @Override
        public void onConnect() {
            // does not connect
        }
    }

    private static class SinkImpl implements SinkChannel, NioSelector.SelectableChannel {

        private final java.nio.channels.Pipe.SinkChannel channel;
        private final Selector selector;
        private SelectionKey selectionKey;
        private Runnable writeListener;

        private SinkImpl(Selector selector, java.nio.channels.Pipe pipe) {
            this.selector = selector;
            this.channel = pipe.sink();
        }

        @Override
        public boolean isOpen() {
            return channel.isOpen();
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

        @Override
        public void configureBlocking(boolean block) throws IOException {
            channel.configureBlocking(block);
            if (!block) {
                selectionKey = channel.register(selector, 0, this);
            }
        }

        @Override
        public boolean isBlocking() {
            return channel.isBlocking();
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            return channel.write(srcs, offset, length);
        }

        @Override
        public long write(ByteBuffer[] srcs) throws IOException {
            return channel.write(srcs);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            return channel.write(src);
        }

        @Override
        public void setWriteListener(Runnable writeListener) {
            this.writeListener = writeListener;
            if (selectionKey.isValid()) {
                if (writeListener == null) {
                    selectionKey.interestOpsAnd(~SelectionKey.OP_WRITE);
                } else {
                    selectionKey.interestOpsOr(SelectionKey.OP_WRITE);
                }
            }
        }

        @Override
        public void onRead() {
            // does not read
        }

        @Override
        public void onWrite() {
            writeListener.run();
        }

        @Override
        public void onAccept() {
            // does not accept
        }

        @Override
        public void onConnect() {
            // does not connect
        }
    }
}
