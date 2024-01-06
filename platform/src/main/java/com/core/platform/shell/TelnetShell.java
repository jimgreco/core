package com.core.platform.shell;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.collections.ObjectPool;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.EncoderUtils;
import com.core.infrastructure.encoding.MutableObjectEncoder;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.io.ServerSocketChannel;
import com.core.infrastructure.io.SocketChannel;
import com.core.infrastructure.io.WritableBufferChannel;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.time.Scheduler;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A telnet shell allows for TCP clients to instantiate new shell instances.
 */
public class TelnetShell implements Encodable {

    private static final DirectBuffer NEWLINE = BufferUtils.fromAsciiString("\n");

    private final Scheduler scheduler;
    private final Selector selector;
    private final Log log;
    private final Shell shell;

    @Property
    private final ObjectPool<TelnetAsyncCommandContext> asyncPool;
    @Property
    private final ObjectPool<TelnetShellContextHandler> shellHandlerPool;
    @Property
    private final List<TelnetShellContextHandler> activeShellHandlers;

    private final Runnable cachedOnClientAccept;

    private ServerSocketChannel serverSocket;

    /**
     * Creates an empty {@code TelnetShell}.
     *
     * @param scheduler the system time scheduler for scheduling refresh tasks
     * @param selector the selector used to create TCP server socket channels
     * @param logFactory a factory to create logs
     * @param shell the shel
     */
    public TelnetShell(Scheduler scheduler, Selector selector, LogFactory logFactory, Shell shell) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler is null");
        this.selector = Objects.requireNonNull(selector, "selectService is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        this.shell = Objects.requireNonNull(shell, "shell is null");

        log = logFactory.create(getClass());

        asyncPool = new ObjectPool<>(TelnetAsyncCommandContext::new);
        shellHandlerPool = new ObjectPool<>(TelnetShellContextHandler::new);
        activeShellHandlers = new CoreList<>();

        cachedOnClientAccept = this::onClientAccept;
    }

    /**
     * Opens a TCP server socket channel and binds the channel to the specified address.
     *
     * @param bindAddress the address
     * @throws IOException if an I/O error occurs
     */
    @Command
    public void open(String bindAddress) throws IOException {
        if (serverSocket != null) {
            throw new IOException("already open");
        }
        log.info().append("opening server socket: ").append(bindAddress).commit();

        serverSocket = selector.createServerSocketChannel();
        serverSocket.configureBlocking(false);
        serverSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverSocket.bind(bindAddress);
        serverSocket.setAcceptListener(cachedOnClientAccept);
    }

    /**
     * Closes the TCP server socket channel and any open telnet sessions.
     *
     * @throws IOException if an I/O error occurs
     */
    @Command
    public void close() throws IOException {
        if (serverSocket != null) {
            log.info().append("closing server socket: ").append(serverSocket.getLocalAddress()).commit();

            while (!activeShellHandlers.isEmpty()) {
                var shellHandler = activeShellHandlers.get(activeShellHandlers.size() - 1);
                shellHandler.shellContext.close();
            }

            serverSocket.close();
            serverSocket = null;
        }
    }

    @Override
    @Command(path = "status")
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("open").bool(serverSocket != null)
                .string("instances").number(activeShellHandlers.size())
                .closeMap();
    }

    private void onClientAccept() {
        try {
            var clientSocket = serverSocket.accept();
            clientSocket.configureBlocking(false);
            clientSocket.setOption(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE);
            log.info().append("accepted client socket: ").append(clientSocket.getRemoteAddress()).commit();

            var shellHandler = shellHandlerPool.borrowObject();
            shellHandler.clientSocket = clientSocket;
            shellHandler.shellContext = shell.open(
                    clientSocket, clientSocket, true, false, shellHandler);
            activeShellHandlers.add(shellHandler);
        } catch (IOException e) {
            log.warn().append("could not accept connection: ").append(e).commit();
        }
    }

    private class TelnetShellContextHandler implements ShellContextHandler, Encodable {

        private final MutableObjectEncoder encoder;
        private final List<TelnetAsyncCommandContext> asyncCommandContexts;

        ShellContext shellContext;
        SocketChannel clientSocket;

        TelnetShellContextHandler() {
            encoder = EncoderUtils.createTextEncoder();
            asyncCommandContexts = new CoreList<>();
        }

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.openMap()
                    .string("clientAddress").string(clientSocket.getRemoteAddress())
                    .string("asyncCommandContexts").number(asyncCommandContexts.size())
                    .closeMap();
        }

        @Override
        public void onClosed() {
            while (!asyncCommandContexts.isEmpty()) {
                asyncCommandContexts.get(asyncCommandContexts.size() - 1).stop();
            }

            shellContext = null;
            clientSocket = null;

            activeShellHandlers.remove(this);
            shellHandlerPool.returnObject(this);
        }

        @Override
        public MutableObjectEncoder getObjectEncoder() {
            return encoder;
        }

        @Override
        public AsyncCommandContext borrowAsyncCommandContext() {
            var async = asyncPool.borrowObject();
            async.shellContextHandler = this;
            async.clientSocket = clientSocket;
            asyncCommandContexts.add(async);
            return async;
        }

        void onStopped(TelnetAsyncCommandContext asyncCommandContext) {
            asyncCommandContexts.remove(asyncCommandContext);
            asyncPool.returnObject(asyncCommandContext);
        }
    }

    private class TelnetAsyncCommandContext implements AsyncCommandContext {

        private final MutableDirectBuffer writeBuffer;
        private final MutableObjectEncoder objectEncoder;

        private final Runnable cachedOnFinishRootLevel;
        private final Runnable cachedRefreshTask;

        private TelnetShellContextHandler shellContextHandler;
        private WritableBufferChannel clientSocket;

        private boolean open;
        private Consumer<AsyncCommandContext> stopListener;
        private long objectId;
        private long refreshTaskId;
        private Consumer<AsyncCommandContext> refreshTask;
        private Object associatedObject;

        TelnetAsyncCommandContext() {
            objectEncoder = EncoderUtils.createTextEncoder();
            writeBuffer = BufferUtils.allocateExpandable(4096);

            cachedOnFinishRootLevel = this::onFinishRootLevel;
            cachedRefreshTask = this::refreshTask;
        }

        @Override
        public ObjectEncoder getObjectEncoder() {
            return objectEncoder;
        }

        @Override
        public void setObjectType(String objectType) {
        }

        @Override
        public long getObjectId() {
            return objectId;
        }

        @Override
        public void setObjectId(long objectId) {
            this.objectId = objectId;
        }

        @Override
        public void start() {
            start(null, null);
        }

        @Override
        public void start(Object associatedObject) {
            start(associatedObject, null);
        }

        @Override
        public void start(Object associatedObject, Consumer<AsyncCommandContext> stopListener) {
            open = true;
            this.associatedObject = associatedObject;
            this.stopListener = stopListener;

            objectEncoder.setFinishRootLevelListener(cachedOnFinishRootLevel);
            objectEncoder.start(writeBuffer, 0);
        }

        @Override
        public void setRefreshTask(long nanoseconds, Consumer<AsyncCommandContext> task, String name) {
            this.refreshTask = task;
            refreshTaskId = scheduler.scheduleEvery(nanoseconds, cachedRefreshTask, name, 0);
        }

        @Override
        public void cancelRefreshTask() {
            refreshTaskId = scheduler.cancel(refreshTaskId);
        }

        @Override
        public Object getAssociatedObject() {
            return associatedObject;
        }

        @Override
        public void stop() {
            open = false;
            clientSocket = null;
            associatedObject = null;
            refreshTaskId = scheduler.cancel(refreshTaskId);

            if (stopListener != null) {
                stopListener.accept(this);
                stopListener = null;
            }

            if (shellContextHandler != null) {
                shellContextHandler.onStopped(this);
                shellContextHandler = null;
            } else {
                log.warn().append("shell context is null -- should not happen").commit();
            }
        }

        private void refreshTask() {
            if (refreshTask != null) {
                refreshTask.accept(this);
            }
        }

        private void onFinishRootLevel() {
            try {
                if (open) {
                    clientSocket.write(writeBuffer, 0, objectEncoder.getEncodedLength());
                    clientSocket.write(NEWLINE);
                    objectEncoder.rewind();
                }
            } catch (IOException e) {
                stop();
            }
        }
    }
}
