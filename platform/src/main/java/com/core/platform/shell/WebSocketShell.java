package com.core.platform.shell;

import com.core.infrastructure.Json;
import com.core.infrastructure.buffer.BufferNumberUtils;
import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.collections.ObjectPool;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.MutableObjectEncoder;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.IoLogger;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.io.ServerSocketChannel;
import com.core.infrastructure.io.SocketChannel;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.metrics.MetricFactory;
import com.core.infrastructure.time.Scheduler;
import com.core.platform.io.JsonWebSocketServerClient;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Provides an interface to the command shell over WebSockets.
 *
 * <p>WebSocket commands are JSON messages.
 * For example, the following will return information on an instrument identified by a symbol by invoking the
 * '/repos/instrument/instrument' command with the specified symbol parameter:<pre>
 *     INPUT  {"cmd":"/repos/instrument/instrument", "params":{"symbol": "BTC/USD"}}
 *     OUTPUT {"status":"ok","cmd":"/repos/instrument/instrument","params":{"symbol":"BTC/USD"},"data":{
 *     "instrumentId":13,"instrumentType":"SPOT","symbol":"BTC/USD","minQty":0.001,"maxQty":100,"maxVenueQty":5,
 *     "minQtyIncrement":0.0001,"minPrice":0,"maxPrice":0,"minPriceIncrement":1,"minNotional":0,"baseCurrency":"BTC",
 *     "quoteCurrency":"USD"}}
 *
 * </pre>
 *
 * <p>Parameters can be typed, although you can always fall back to a string representation:<pre>
 *     INPUT  {"cmd":"/repos/instrument/instrumentById", "params":{"instrumentId": 1}}
 *     OUTPUT {"status":"ok","cmd":"/repos/instrument/instrumentById","params":{"instrumentId":1},"data":{
 *     "instrumentId":1,"instrumentType":"SPOT","symbol":"BUSD/USDT","minQty":0.01,"maxQty":1000000,
 *     "maxVenueQty":20000000,"minQtyIncrement":0.01,"minPrice":0,"maxPrice":0,"minPriceIncrement":1,"minNotional":0,
 *     "baseCurrency":"BUSD","quoteCurrency":"USDT"}}
 *
 * </pre>
 *
 * <p>Streams are also supported. For example, the following will stream data for the BBO of two instruments by invoking
 * the market data monitor's bbo-stream command:<pre>
 *
 *     INPUT  {"cmd":"/mdm01a/bbo-stream", "params":{"symbols": ["BTC/USD", "ETH/USD"]}}
 *     OUTPUT {"data":[{"instrument":"BTC/USD","instrumentType":"SPOT","bestBidQty":0.06762,"bestBidPrice":21261.62,
 *     "bestBidVenue":"BINANCE_SPOT","bestAskQty":0.0339,"bestAskPrice":21260,"bestAskVenue":"FTX",
 *     "lastUpdateTime":"2022-11-06T12:22:22.547-05:00","tradeVolume":3206.96668,"lastTradePrice":21262.12,
 *     "lastTradeQty":0.00438,"lastTradeTime":"2022-11-06T12:22:22.396-05:00","lastTradeVenue":"BINANCE_SPOT"},
 *     {"instrument":"ETH/USD","instrumentType":"SPOT","bestBidQty":11.8812,"bestBidPrice":1619.49,
 *     "bestBidVenue":"BINANCE_SPOT","bestAskQty":29.718,"bestAskPrice":1619.5,"bestAskVenue":"FTX",
 *     "lastUpdateTime":"2022-11-06T12:22:20.986-05:00","tradeVolume":13806.5856,"lastTradePrice":1619.5,
 *     "lastTradeQty":0.0141,"lastTradeTime":"2022-11-06T12:22:21.549-05:00","lastTradeVenue":"BINANCE_SPOT"}],
 *     "cmd":"/mdm01a/bbo-stream","streamId":1,"status":"ok"}
 * </pre>
 *
 * <p>No acknowledgement of the command will occur, but most streams start streaming data immediately.
 * Each stream is identified by a unique, streamId parameter which can be used for canceling or updating the stream.
 *
 * <p>A specified stream can be canceled by adding the {@code streamId} parameter.<pre>
 *
 *      INPUT  {"stream":"cancel", "streamId": 2}
 * </pre>
 *
 * <p>All streams for a specified command can be canceled using the stream parameter.<pre>
 *
 *     INPUT  {"stream":"cancel", "cmd":"/mdm01a/bbo-stream"}
 * </pre>
 *
 * <p>Streams can be updated, replacing the existing stream with updated parameters.<pre>
 *
 *     INPUT  {"cmd":"/mdm01a/bbo-stream", "params":{"symbols": ["SOL/USD"]}, "stream":"update" }
 *     INPUT  {"cmd":"/mdm01a/bbo-stream", "params":{"symbols": ["SOL/USD"]}, "stream":"update", "streamId":1}
 * </pre>
 *
 * <p>No acknowledgement of canceling or updating a stream will occur.
 */
public class WebSocketShell implements Encodable {

    private final Scheduler scheduler;
    private final Selector selector;
    private final Log log;
    private final Shell shell;

    private final ObjectPool<WebSocketAsyncCommandContext> asyncPool;
    private final ObjectPool<WebSocketShellContextHandler> shellHandlerPool;
    private final List<WebSocketShellContextHandler> activeShellHandlers;

    private final Runnable cachedOnClientAccept;
    private final IoLogger ioListener;
    private final MutableDirectBuffer textCommand;

    private ServerSocketChannel serverSocket;

    /**
     * Creates an empty {@code HttpShell}.
     *
     * @param scheduler a scheduler of tasks
     * @param selector the selector used to create TCP server socket channels
     * @param logFactory a factory to create logs
     * @param metricFactory a factory to create metrics
     * @param shell the shell
     */
    public WebSocketShell(
            Scheduler scheduler, Selector selector, LogFactory logFactory, MetricFactory metricFactory, Shell shell) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler is null");
        this.selector = Objects.requireNonNull(selector, "selectService is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(metricFactory, "metricFactory is null");
        this.shell = Objects.requireNonNull(shell, "shell is null");

        log = logFactory.create(getClass());
        cachedOnClientAccept = this::onClientAccept;
        asyncPool = new ObjectPool<>(WebSocketAsyncCommandContext::new);
        shellHandlerPool = new ObjectPool<>(WebSocketShellContextHandler::new);
        activeShellHandlers = new CoreList<>();
        textCommand = BufferUtils.allocateExpandable(256);
        ioListener = new IoLogger(log, "WS_SHELL");
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
     * Closes the TCP server socket channel and any open HTTP sessions.
     *
     * @throws IOException if an I/O error occurs
     */
    @Command
    public void close() throws IOException {
        while (!activeShellHandlers.isEmpty()) {
            var shellHandler = activeShellHandlers.get(activeShellHandlers.size() - 1);
            shellHandler.shellContext.close();
        }

        if (serverSocket != null) {
            log.info().append("closing server socket: ").append(serverSocket.getLocalAddress()).commit();
            serverSocket.close();
            serverSocket = null;
        }
    }

    @Override
    @Command(path = "status")
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("open").bool(serverSocket != null)
                .string("connections").number(activeShellHandlers.size())
                .string("asyncPool").object(asyncPool)
                .string("shellHandlerPool").object(shellHandlerPool)
                .closeMap();
    }

    private void onClientAccept() {
        try {
            var clientSocket = serverSocket.accept();
            log.info().append("accepting client socket: ").append(clientSocket.getRemoteAddress()).commit();

            var shellHandler = shellHandlerPool.borrowObject();
            shellHandler.init(clientSocket);
            activeShellHandlers.add(shellHandler);
        } catch (IOException e) {
            log.warn().append("could not accept connection: ").append(e).commit();
        }
    }

    private class WebSocketShellContextHandler implements ShellContextHandler, Encodable {

        private final Runnable cachedOnConnectionFailed;
        private final Consumer<Json.Value> cachedOnRead;
        private final JsonWebSocketServerClient webSocket;
        private final List<WebSocketAsyncCommandContext> asyncCommandContexts;
        private int streamId;

        private CommandDescriptor commandDescriptor;
        private Json.MapValue params;
        private boolean wroteError;

        ShellContext shellContext;
        private int updateStreamId;

        WebSocketShellContextHandler() {
            webSocket = new JsonWebSocketServerClient(scheduler);
            asyncCommandContexts = new CoreList<>();
            cachedOnRead = this::onRead;
            cachedOnConnectionFailed = this::onConnectionFailed;
        }

        void init(SocketChannel channel) throws IOException {
            shellContext = shell.open(null, null, false, false, this);
            webSocket.init(channel);
            webSocket.setReadListener(cachedOnRead);
            webSocket.setIoListener(ioListener);
            webSocket.setConnectionFailedListener(cachedOnConnectionFailed);
            webSocket.enablePing(true);
        }

        private void onRead(Json.Value value) {
            if (value == null || !value.isMap()) {
                ioListener.onConnectionEvent("did not receive map object");
                onConnectionFailed();
                return;
            }
            var map = value.asMap();

            var command = map.getOptionalStringValue("cmd");
            var stream = map.getOptionalStringValue("stream");
            updateStreamId = -1;
            if (stream != null && (stream.equals("cancel") || stream.equals("update"))) {
                var streamId = map.getOptionalLongValue("streamId", -1);

                for (var i = 0; i < asyncCommandContexts.size(); i++) {
                    var asyncCommandContext = asyncCommandContexts.get(i);
                    if (streamId == asyncCommandContext.streamId
                            || streamId == -1 && command != null && command.equals(asyncCommandContext.command)) {
                        asyncCommandContext.stop();
                        if (stream.equals("update")) {
                            updateStreamId = (int) streamId;
                        }
                    }
                }

                if (stream.equals("cancel")) {
                    return;
                }
            }

            if (command == null) {
                ioListener.onConnectionEvent("did not receive command");
                onConnectionFailed();
                return;
            }
            var params = map.getOptionalMap("params");

            try {
                commandDescriptor = shell.getCommandDescriptor(command);

                if (!commandDescriptor.isExecutable()) {
                    ioListener.onConnectionFailed("command is not executable", null);
                    sendError("command is not executable", command, params, null);
                    return;
                }
            } catch (CommandException e) {
                ioListener.onConnectionFailed("error parsing or executing command", e);
                sendError("unknown command", command, params, null);
                return;
            }

            try {
                var commandLength = textCommand.putStringWithoutLengthAscii(0, commandDescriptor.getPath());
                var paramNames = commandDescriptor.getParameterNames();
                if (paramNames.length > 0 && params == null) {
                    sendError("params not specified", command, null, null);
                    return;
                }

                for (var i = 0; i < paramNames.length; i++) {
                    var paramName = paramNames[i];
                    var paramValue = params.getOptional(paramName);
                    if (paramValue == null) {
                        sendError("missing required parameter", command, params, paramName);
                        return;
                    }

                    textCommand.putByte(commandLength++, (byte) ' ');

                    if (paramValue.isString()) {
                        var str = paramValue.asStringValue();
                        textCommand.putByte(commandLength++, (byte) '\"');
                        textCommand.putBytes(commandLength, str, 0, str.capacity());
                        commandLength += str.capacity();
                        textCommand.putByte(commandLength++, (byte) '\"');
                    } else if (paramValue.isNull()) {
                        commandLength += textCommand.putStringWithoutLengthAscii(commandLength, "null");
                    } else if (paramValue.isBool()) {
                        commandLength += textCommand.putStringWithoutLengthAscii(
                                commandLength, paramValue.asBoolValue() ? "true" : "false");
                    } else if (paramValue.isLong()) {
                        commandLength += textCommand.putLongAscii(commandLength, paramValue.asLongValue());
                    } else if (paramValue.isDouble()) {
                        commandLength += textCommand.putStringWithoutLengthAscii(
                                commandLength, Double.toString(paramValue.asDoubleValue()));
                    } else if (paramValue.isList()) {
                        if (i == paramNames.length - 1) {
                            var vals = paramValue.asListValue();
                            for (var j = 0; j < vals.size(); j++) {
                                if (j != 0) {
                                    textCommand.putByte(commandLength++, (byte) ' ');
                                }

                                var val = vals.get(j);
                                if (val.isString()) {
                                    var str = val.asStringValue();
                                    textCommand.putByte(commandLength++, (byte) '\"');
                                    textCommand.putBytes(commandLength, str, 0, str.capacity());
                                    commandLength += str.capacity();
                                    textCommand.putByte(commandLength++, (byte) '\"');
                                } else if (val.isNull()) {
                                    commandLength += textCommand.putStringWithoutLengthAscii(commandLength, "null");
                                } else if (val.isBool()) {
                                    commandLength += textCommand.putStringWithoutLengthAscii(
                                            commandLength, val.asBoolValue() ? "true" : "false");
                                } else if (paramValue.isLong()) {
                                    commandLength += textCommand.putLongAscii(commandLength, val.asLongValue());
                                } else if (paramValue.isDouble()) {
                                    commandLength += BufferNumberUtils.putAsAsciiDecimal(
                                            textCommand, commandLength, val.asDoubleValue());
                                } else {
                                    sendError("unsupported list parameter type", command, params, paramName);
                                    return;
                                }
                            }
                        } else {
                            sendError("lists only supported for the last parameter", command, params, paramName);
                            return;
                        }
                    } else {
                        sendError("unsupported parameter type", command, params, paramName);
                        return;
                    }
                }

                textCommand.putByte(commandLength++, (byte) '\n');
                var numContexts = asyncCommandContexts.size();
                var object = shellContext.executeInline(textCommand, 0, commandLength);
                if (numContexts == asyncCommandContexts.size()) {
                    webSocket.json().openMap()
                            .string("status").string("ok")
                            .string("cmd").string(command)
                            .string("params").object(params)
                            .string("data").object(object)
                            .closeMap();
                }
            } catch (IOException e) {
                ioListener.onConnectionFailed("I/O write error", e);
                wroteError = true;
                onConnectionFailed();
                wroteError = false;
            } catch (CommandException e) {
                sendError("error executing command", command, params, e.getMessage());
            }
        }

        private void sendError(String msg, DirectBuffer command, Json.MapValue params, String param) {
            webSocket.json().openMap()
                    .string("status").string("error")
                    .string("msg").string(msg)
                    .string("cmd").string(command)
                    .string("params").object(params)
                    .string("data").string(param)
                    .closeMap();
        }

        private void onConnectionFailed() {
            while (!asyncCommandContexts.isEmpty()) {
                asyncCommandContexts.get(asyncCommandContexts.size() - 1).stop();
            }

            if (!wroteError) {
                ioListener.onConnectionFailed(
                        webSocket.getConnectionFailedReason(),
                        webSocket.getConnectionFailedException());
            }

            webSocket.close();

            if (shellContext != null) {
                shellContext.close();
            }
        }

        @Override
        public void onClosed() {
            streamId = 0;
            updateStreamId = -1;
            shellContext = null;
            webSocket.close();
            activeShellHandlers.remove(this);
            shellHandlerPool.returnObject(this);
        }

        @Override
        public MutableObjectEncoder getObjectEncoder() {
            return (MutableObjectEncoder) webSocket.json();
        }

        @Override
        public AsyncCommandContext borrowAsyncCommandContext() {
            var async = asyncPool.borrowObject();
            async.shellContextHandler = this;
            async.command = commandDescriptor.getPath();
            async.streamId = updateStreamId == -1 ? ++streamId : updateStreamId;
            asyncCommandContexts.add(async);
            return async;
        }

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.openMap()
                    .string("asyncCommandContexts").number(asyncCommandContexts.size())
                    .closeMap();
        }
    }

    private class WebSocketAsyncCommandContext extends JsonWebSocketServerClient implements AsyncCommandContext {

        private final Runnable cachedFinishedData;
        private final Runnable cachedRefreshTask;

        String command;
        WebSocketShellContextHandler shellContextHandler;

        private Object associatedObject;
        private boolean open;
        private Consumer<AsyncCommandContext> stopListener;
        private int streamId;

        private long refreshTaskId;
        private Consumer<AsyncCommandContext> refreshTask;

        private MutableObjectEncoder encoder;

        WebSocketAsyncCommandContext() {
            super(WebSocketShell.this.scheduler);

            cachedFinishedData = this::onFinishData;
            cachedRefreshTask = () -> {
                if (refreshTask != null) {
                    refreshTask.accept(this);
                }
            };
        }

        @Override
        public ObjectEncoder getObjectEncoder() {
            encoder = shellContextHandler.getObjectEncoder();
            encoder.openMap().string("data");
            encoder.setFinishLevelListener(1, cachedFinishedData);
            return encoder;
        }

        @Override
        public long getObjectId() {
            return -1;
        }

        @Override
        public Object getAssociatedObject() {
            return associatedObject;
        }

        @Override
        public void setObjectId(long objectId) {
        }

        @Override
        public void setObjectType(String objectType) {
        }

        @Override
        public void setRefreshTask(long nanoseconds, Consumer<AsyncCommandContext> task, String name) {
            this.refreshTask = task;
            refreshTaskId = scheduler.scheduleEvery(nanoseconds, cachedRefreshTask, name, 0);
        }

        @Override
        public void start() {
            start(null);
        }

        @Override
        public void start(Object associatedObject) {
            start(associatedObject, null);
        }

        @Override
        public void start(Object associatedObject, Consumer<AsyncCommandContext> stopListener) {
            this.associatedObject = associatedObject;
            this.stopListener = stopListener;
            open = true;
        }

        @Override
        public void cancelRefreshTask() {
            refreshTaskId = scheduler.cancel(refreshTaskId);
        }

        @SuppressWarnings("PMD.EmptyCatchBlock")
        @Override
        public void stop() {
            command = null;
            refreshTaskId = scheduler.cancel(refreshTaskId);
            refreshTask = null;

            open = false;
            encoder = null;

            if (stopListener != null) {
                stopListener.accept(this);
                stopListener = null;
            }
            associatedObject = null;
            shellContextHandler.asyncCommandContexts.remove(this);
            asyncPool.returnObject(this);
            shellContextHandler = null;
        }

        private void onFinishData() {
            if (open) {
                encoder.setFinishLevelListener(1, null);
                encoder.string("cmd").string(command)
                        .string("streamId").number(streamId)
                        .string("status").string("ok")
                        .closeMap();
            }
        }
    }
}
