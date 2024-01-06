package com.core.infrastructure.io;

import com.core.infrastructure.collections.ByteBufferPool;
import com.core.infrastructure.time.Scheduler;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.nio.channels.SelectionKey;
import java.nio.channels.spi.SelectorProvider;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A {@code Selector} that operates the JDK NIO {@code Selector}.
 *
 * @see java.nio.channels.Selector
 */
public class NioSelector implements Selector {

    private static final SSLContext DEFAULT_SSL_CONTEXT;

    static {
        try {
            DEFAULT_SSL_CONTEXT = SSLContext.getInstance(SslSocketChannel.TlsV12);
            DEFAULT_SSL_CONTEXT.init(null, null, new SecureRandom());
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new IllegalArgumentException();
        }
    }

    private static final long NANOS_PER_MILLI = TimeUnit.MILLISECONDS.toNanos(1);
    private final java.nio.channels.Selector selector;
    private final Consumer<SelectionKey> onSelect;
    private final SelectorProvider selectorProvider;

    private final ByteBufferPool bufferPool;
    private Scheduler scheduler;

    /**
     * Creates a {@code NioSelectService} with the system default selector provider.
     *
     * @throws IOException if an I/O error occurs opening the selector
     */
    public NioSelector() throws IOException {
        this(SelectorProvider.provider());
    }

    /**
     * Creates a {@code NioSelectService} with the specified selector provider.
     *
     * @param selectorProvider the selector provider
     * @throws IOException if an I/O error occurs opening the selector
     */
    public NioSelector(SelectorProvider selectorProvider) throws IOException {
        this.selectorProvider = Objects.requireNonNull(selectorProvider);
        selector = selectorProvider.openSelector();
        onSelect = this::onSelect;

        bufferPool = new ByteBufferPool(true);
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public PipeImpl createPipe() throws IOException {
        var pipe = selectorProvider.openPipe();
        return new PipeImpl(selector, pipe);
    }

    @Override
    public DatagramChannel createDatagramChannel() throws IOException {
        var channel = selectorProvider.openDatagramChannel(StandardProtocolFamily.INET);
        return new DatagramChannelImpl(selector, channel);
    }

    @Override
    public SocketChannel createSocketChannel() throws IOException {
        var channel = selectorProvider.openSocketChannel();
        return new SocketChannelImpl(selector, channel);
    }

    @Override
    public SslSocketChannel createSslSocketChannel() throws IOException {
        return createSslSocketChannel(null);
    }

    @Override
    public SslSocketChannel createSslSocketChannel(SSLContext sslContext) throws IOException {
        if (sslContext == null) {
            sslContext = DEFAULT_SSL_CONTEXT;
        }
        var channel = selectorProvider.openSocketChannel();
        return new SslSocketChannelImpl(selector, channel, sslContext, scheduler, bufferPool);
    }

    @Override
    public ServerSocketChannel createServerSocketChannel() throws IOException {
        var channel = selectorProvider.openServerSocketChannel();
        return new ServerSocketChannelImpl(selector, channel);
    }

    @Override
    public void selectNow() throws IOException {
        selector.selectNow(onSelect);
    }

    @Override
    public void select() throws IOException {
        selector.select(onSelect);
    }

    @Override
    public void select(long timeout) throws IOException {
        selector.select(onSelect, timeout / NANOS_PER_MILLI);
    }

    @Override
    public void close() throws IOException {
        selector.close();
    }

    private void onSelect(SelectionKey selectionKey) {
        var attachment = (SelectableChannel) selectionKey.attachment();

        if (selectionKey.isValid() && selectionKey.isReadable()) {
            attachment.onRead();
        }

        if (selectionKey.isValid() && selectionKey.isWritable()) {
            attachment.onWrite();
        }

        if (selectionKey.isValid() && selectionKey.isConnectable()) {
            attachment.onConnect();
        }

        if (selectionKey.isValid() && selectionKey.isAcceptable()) {
            attachment.onAccept();
        }
    }

    interface SelectableChannel {

        void onRead();

        void onWrite();

        void onAccept();

        void onConnect();
    }
}
