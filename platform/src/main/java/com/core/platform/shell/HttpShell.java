package com.core.platform.shell;

import com.core.infrastructure.buffer.BufferNumberUtils;
import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.collections.CoreMap;
import com.core.infrastructure.collections.ObjectPool;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.EncoderUtils;
import com.core.infrastructure.encoding.MutableObjectEncoder;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.DirectBufferChannel;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.io.ServerSocketChannel;
import com.core.infrastructure.io.SocketChannel;
import com.core.infrastructure.io.WritableBufferChannel;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.metrics.MetricFactory;
import com.core.infrastructure.time.Scheduler;
import com.core.infrastructure.time.Time;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Provides an interface to the command shell over HTTP.
 *
 * <p>HTTP requests for commands are REST requests.
 * For example, the following will invoke "/seq01a/status" (i.e., the status of the Sequencer)<pre>
 *
 *     GET /seq01a/status HTTP/1.1
 * </pre>
 *
 * <p>Commands can also specify arguments.
 * For example, the following will invoke "/val01a/addBook ETH/USD "Binance Spot".
 * The body of the POST request specifies the arguments to the addBook method in the valuation application.<pre>
 *
 *     POST http://localhost:8001/val01a/addBook HTTP/1.1
 *     Content-Type: text/html
 *     Content-Length: 28
 *
 *     symbol=ETH/USD&amp;venueName=Coinbase
 * </pre>
 *
 * <p>Finally, if the endpoint references a command that is not executable (i.e., a directory), the a directory list is
 * returned.
 * For example, the following will return all commands in "/seq01a" (i.e., all commands in the Sequencer)<pre>
 *
 *     GET /seq01a HTTP/1.1
 * </pre>
 */
public class HttpShell implements Encodable {

    private static final long DEFAULT_HEARTBEAT = TimeUnit.SECONDS.toNanos(1);
    private static final DirectBuffer ID_START = BufferUtils.fromAsciiString("id: ");
    private static final DirectBuffer EVENT_START = BufferUtils.fromAsciiString("event: ");
    private static final DirectBuffer NEWLINE = BufferUtils.fromAsciiString("\n");
    private static final DirectBuffer DATA_START = BufferUtils.fromAsciiString("data: ");
    private static final DirectBuffer DATA_END = BufferUtils.fromAsciiString("\n\n");
    private static final DirectBuffer HEARTBEAT = BufferUtils.fromAsciiString(": heartbeat\n\n");
    private static final DirectBuffer FAV_ICON = BufferUtils.fromAsciiString("/favicon.ico");
    private static final DirectBuffer LAST_EVENT_ID = BufferUtils.fromAsciiString("Last-Event-ID");
    private static final DirectBuffer CONTENT_LENGTH = BufferUtils.fromAsciiString("Content-Length");

    private final Time time;
    private final Scheduler scheduler;
    private final Selector selector;
    private final Log log;
    private final Shell shell;

    @Property
    private final ObjectPool<DirectBuffer> bufferPool;
    @Property
    private final ObjectPool<SseAsyncCommandContext> asyncPool;
    @Property
    private final ObjectPool<HttpShellContextHandler> shellHandlerPool;
    @Property
    private final List<HttpShellContextHandler> activeShellHandlers;

    private final DirectBuffer temp;
    private final MutableDirectBuffer tempString;
    private final MutableDirectBuffer tempCommand;
    private final MutableDirectBuffer tempReturn;
    private final List<DirectBuffer> buffersToReturn;
    private final Map<DirectBuffer, DirectBuffer> paramsMap;
    private final Map<DirectBuffer, DirectBuffer> headersMap;

    private final Runnable cachedOnClientAccept;

    @Property
    private int totalAsyncContexts;
    private ServerSocketChannel serverSocket;

    /**
     * Creates an empty {@code HttpShell}.
     *
     * @param time the system time source
     * @param scheduler a scheduler of tasks
     * @param selector the selector used to create TCP server socket channels
     * @param logFactory a factory to create logs
     * @param metricFactory a factory to create metrics
     * @param shell the shell
     */
    public HttpShell(
            Time time, Scheduler scheduler,
            Selector selector, LogFactory logFactory, MetricFactory metricFactory, Shell shell) {
        this.time = Objects.requireNonNull(time, "time is null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler is null");
        this.selector = Objects.requireNonNull(selector, "selectService is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(metricFactory, "metricFactory is null");
        this.shell = Objects.requireNonNull(shell, "shell is null");

        log = logFactory.create(getClass());
        cachedOnClientAccept = this::onClientAccept;
        asyncPool = new ObjectPool<>(SseAsyncCommandContext::new);
        shellHandlerPool = new ObjectPool<>(HttpShellContextHandler::new);
        bufferPool = new ObjectPool<>(BufferUtils::emptyBuffer);
        activeShellHandlers = new CoreList<>();

        temp = BufferUtils.emptyBuffer();
        tempString = BufferUtils.allocateExpandable(128);
        tempCommand = BufferUtils.allocateExpandable(128);
        tempReturn = BufferUtils.allocateExpandable(4096);
        buffersToReturn = new CoreList<>();
        paramsMap = new CoreMap<>();
        headersMap = new CoreMap<>();
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
                .string("lifeInstances").number(totalAsyncContexts)
                .closeMap();
    }

    private void onClientAccept() {
        try {
            var clientSocket = serverSocket.accept();
            clientSocket.configureBlocking(false);
            clientSocket.setOption(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE);
            log.info().append("accepting client socket: ").append(clientSocket.getRemoteAddress()).commit();

            var shellHandler = shellHandlerPool.borrowObject();
            clientSocket.setReadListener(shellHandler);
            shellHandler.id = ++totalAsyncContexts;
            shellHandler.clientSocket = clientSocket;
            shellHandler.shellContext = shell.open(null, shellHandler.writeBuffer, false, false, shellHandler);
            activeShellHandlers.add(shellHandler);
        } catch (IOException e) {
            log.warn().append("could not accept connection: ").append(e).commit();
        }
    }

    private class HttpShellContextHandler implements ShellContextHandler, Encodable, Runnable {

        private static final int INITIAL_BUFFER_SIZE = 4096;

        private final MutableDirectBuffer readBuffer;
        private final DirectBufferChannel writeBuffer;
        private final MutableObjectEncoder encoder;
        private final List<SseAsyncCommandContext> asyncCommandContexts;
        public int id;

        SocketChannel clientSocket;
        ShellContext shellContext;

        private int readIndex;

        HttpShellContextHandler() {
            readBuffer = BufferUtils.allocateExpandable(INITIAL_BUFFER_SIZE);
            writeBuffer = new DirectBufferChannel(INITIAL_BUFFER_SIZE);
            encoder = EncoderUtils.createJsonEncoder();
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

            clientSocket.setReadListener(null);
            clientSocket = null;
            shellContext = null;
            readIndex = 0;

            activeShellHandlers.remove(this);
            shellHandlerPool.returnObject(this);
        }

        @Override
        public MutableObjectEncoder getObjectEncoder() {
            return encoder;
        }

        @Override
        public AsyncCommandContext borrowAsyncCommandContext() {
            var lastEventIdBuf = headersMap.get(LAST_EVENT_ID);
            var lastEventId = lastEventIdBuf == null ? -1 : BufferNumberUtils.parseAsLong(lastEventIdBuf);

            var async = asyncPool.borrowObject();
            async.shellContextHandler = this;
            async.clientSocket = clientSocket;
            async.eventId = lastEventId;
            asyncCommandContexts.add(async);
            return async;
        }

        void onStopped(SseAsyncCommandContext asyncCommandContext) {
            asyncCommandContexts.remove(asyncCommandContext);
            asyncPool.returnObject(asyncCommandContext);
        }

        @Override
        public void run() {
            try {
                var bytesRead = clientSocket.read(readBuffer, readIndex, readBuffer.capacity() - readIndex);
                if (bytesRead == -1) {
                    log.info().append("end of stream, closing: ").append(clientSocket.getRemoteAddress()).commit();
                    shellContext.close();
                } else {
                    readIndex += bytesRead;
                    if (log.isDebug()) {
                        log.info().append(readBuffer, 0, bytesRead).commit();
                    }
                    var position = 0;

                    // parse verb
                    var startIndex = 0;
                    var endIndex = -1;
                    while (position < readIndex) {
                        if (readBuffer.getByte(position++) == ' ') {
                            endIndex = position - 1;
                            break;
                        }
                    }
                    if (endIndex == -1) {
                        // wait for more data
                        return;
                    }
                    temp.wrap(readBuffer, startIndex, endIndex - startIndex);
                    var verb = Keys.valueOf(temp);
                    if (verb == null) {
                        error("400 Bad Request", "unknown HTTP verb", temp);
                        return;
                    }

                    // parse path
                    startIndex = position;
                    endIndex = -1;
                    while (position < readIndex) {
                        var theByte = readBuffer.getByte(position++);
                        if (theByte == ' ' || theByte == '?') {
                            endIndex = position - 1;
                            break;
                        }
                    }
                    if (endIndex == -1) {
                        // wait for more data
                        return;
                    }
                    temp.wrap(readBuffer, startIndex, endIndex - startIndex);
                    if (FAV_ICON.equals(temp)) {
                        readIndex = 0;
                        return;
                    }

                    clearBuffers();

                    if (verb == Keys.GET) {
                        // read query params
                        if (readBuffer.getByte(endIndex) == '?') {
                            position += parseParams(position);
                        }
                        position += fastForwardToEndOfLine(position);
                        position += parseHeaders(position);
                    } else {
                        // POST, etc.
                        position += fastForwardToEndOfLine(position);
                        position += parseHeaders(position);

                        var contentLength = headersMap.get(CONTENT_LENGTH);
                        if (contentLength == null) {
                            // body contains the post params
                            parseParams(position);
                        } else {
                            var contentLengthNum = BufferNumberUtils.parseAsLong(contentLength);
                            if (contentLengthNum == -1) {
                                error("400 Bad Request", "invalid Content-Length header", temp);
                                return;
                            }
                            if (position + contentLengthNum > readIndex) {
                                // did not read enough data yet
                                log.debug().append("waiting for more data: position=").append(position)
                                        .append(", contentLength=").append(contentLengthNum)
                                        .append(", readBytes=").append(readIndex)
                                        .commit();
                                return;
                            }
                            parseParams(position);
                        }
                    }

                    writeBuffer.position(0);

                    var commandDescriptor = shell.getCommandDescriptor(temp);
                    if (commandDescriptor.isExecutable()) {
                        var commandLength = tempCommand.putStringWithoutLengthAscii(0, commandDescriptor.getPath());
                        for (var i = 0; i < commandDescriptor.getParameterNames().length; i++) {
                            var paramName = commandDescriptor.getParameterNames()[i];
                            var length = tempString.putStringWithoutLengthAscii(0, paramName);
                            temp.wrap(tempString, 0, length);
                            var paramValue = paramsMap.get(temp);
                            if (paramValue == null) {
                                error("400 Bad Request", "missing required parameter", temp);
                                return;
                            }
                            tempCommand.putByte(commandLength++, (byte) ' ');

                            var inPercent = 0;
                            var encodedChar = 0;
                            for (var j = 0; j < paramValue.capacity(); j++) {
                                var character = paramValue.getByte(j);
                                if (inPercent > 0) {
                                    encodedChar *= 16;
                                    if (character >= '0' && character <= '9') {
                                        encodedChar += character - '0';
                                    } else if (character >= 'A' && character <= 'F') {
                                        encodedChar += character - 'A' + 10;
                                    }
                                    if (++inPercent == 3) {
                                        tempCommand.putByte(commandLength++, (byte) encodedChar);
                                        inPercent = 0;
                                    }
                                } else if (character == '%') {
                                    encodedChar = 0;
                                    inPercent = 1;
                                } else {
                                    tempCommand.putByte(commandLength++, character);
                                }
                            }
                        }
                        tempCommand.putByte(commandLength++, (byte) '\n');

                        shellContext.execute(tempCommand, 0, commandLength);
                    } else {
                        encoder.start(writeBuffer.getBuffer(), 0);
                        commandDescriptor.ls(encoder);
                        writeBuffer.position(encoder.stop());
                    }

                    // header
                    if (!commandDescriptor.isStreaming()) {
                        returnCommandResult();
                    }
                    readIndex = 0;
                }
            } catch (IOException e) {
                log.warn().append("error reading from socket, disconnecting: ").append(clientSocket.getRemoteAddress())
                        .commit();
                clearBuffers();
                shellContext.close();
            } catch (CommandException commandException) {
                log.warn().append("error parsing command: address=").append(clientSocket.getRemoteAddress())
                        .append(", exception=").append(commandException)
                        .commit();
                clearBuffers();
                var length = 0;
                for (var stackTraceElement : commandException.getStackTrace()) {
                    length += tempString.putStringWithoutLengthAscii(length, stackTraceElement.toString());
                    length += tempString.putStringWithoutLengthAscii(length, "<br/>");
                }
                temp.wrap(tempString, 0, length);
                error("500 Internal Server Error", commandException.getMessage(), temp);
            }
        }

        private int fastForwardToEndOfLine(int offset) {
            var position = offset;
            var b0 = (byte) 0;
            var b1 = (byte) 0;
            while (position < readIndex && b0 != '\r' && b1 != '\n') {
                b0 = b1;
                b1 = readBuffer.getByte(position++);
            }
            return position - offset;
        }

        private int parseParams(int offset) {
            var position = offset;
            var inKey = true;
            var inQuotes = false;
            var keyStart = position;
            var valueStart = -1;

            while (position < readIndex) {
                var theByte = readBuffer.getByte(position);
                if (theByte == '"') {
                    inQuotes = !inQuotes;
                } else if (!inQuotes && inKey && theByte == '=') {
                    valueStart = position + 1;
                    inKey = false;
                } else if (!inQuotes
                        && !inKey
                        && (theByte == '&' || Character.isWhitespace(theByte) || position == readIndex - 1)) {
                    var valueLength = position - valueStart;
                    var key = bufferPool.borrowObject();
                    key.wrap(readBuffer, keyStart, position - keyStart - valueLength - 1);
                    buffersToReturn.add(key);
                    var value = bufferPool.borrowObject();
                    var finalByte = position == readIndex - 1 ? 1 : 0;
                    value.wrap(readBuffer, valueStart, valueLength + finalByte);
                    buffersToReturn.add(value);
                    paramsMap.put(key, value);

                    keyStart = position + 1;
                    inKey = true;
                }
                position++;
                if (!inQuotes && Character.isWhitespace(theByte)) {
                    break;
                }
            }

            return position - offset;
        }

        private int parseHeaders(int offset) {
            var position = offset;
            var inKey = true;
            var inValue = false;
            var keyStart = position;
            var keyEnd  = -1;
            var valueStart = -1;
            var b0 = (byte) 0;
            var b1 = (byte) 0;
            var b2 = (byte) 0;
            var b3 = (byte) 0;

            while (position < readIndex) {
                b0 = b1;
                b1 = b2;
                b2 = b3;
                b3 = readBuffer.getByte(position);
                if (inKey) {
                    if (b0 == '\r' && b1 == '\n' && b2 == '\r' && b3 == '\n') {
                        position++;
                        break;
                    } else if (b3 == ':') {
                        // breaks key/value
                        inKey = false;
                        keyEnd = position;
                    }
                } else if (inValue) {
                    if (b2 == '\r' && b3 == '\n') {
                        var key = bufferPool.borrowObject();
                        key.wrap(readBuffer, keyStart, keyEnd - keyStart);
                        buffersToReturn.add(key);
                        var value = bufferPool.borrowObject();
                        value.wrap(readBuffer, valueStart, position - valueStart - 1);
                        buffersToReturn.add(value);
                        headersMap.put(key, value);

                        inKey = true;
                        inValue = false;
                        keyStart = position + 1;
                        keyEnd = -1;
                        valueStart = -1;
                    }
                } else if (!Character.isWhitespace(b3)) {
                    inValue = true;
                    valueStart = position;
                }
                position++;
            }

            return position - offset;
        }

        private void clearBuffers() {
            paramsMap.clear();
            headersMap.clear();
            if (!buffersToReturn.isEmpty()) {
                for (var entry : buffersToReturn) {
                    bufferPool.returnObject(entry);
                }
                buffersToReturn.clear();
            }
        }

        private void returnCommandResult() throws IOException {
            if (log.isDebug()) {
                log.debug().append("command result: ").append(writeBuffer.getBuffer(), 0, (int) writeBuffer.position())
                        .commit();
            }

            int length;

            if (writeBuffer.position() == 0) {
                length = tempReturn.putStringWithoutLengthAscii(0,
                        "HTTP/1.1 204 No Content\r\n"
                                + "Access-Control-Allow-Origin: *\r\n"
                                + "Cache-Control: no-cache\r\n\r\n");
            } else {
                length = tempReturn.putStringWithoutLengthAscii(
                        0, "HTTP/1.1 200 OK\r\n"
                                + "Access-Control-Allow-Origin: *\r\n"
                                + "Cache-Control: no-cache\r\n"
                                + "Content-Type: application/json\r\n"
                                + "Content-Length: ");
                length += tempReturn.putLongAscii(length, writeBuffer.position());
                length += tempReturn.putStringWithoutLengthAscii(length, "\r\n\r\n");
                tempReturn.putBytes(length, writeBuffer.getBuffer(), 0, (int) writeBuffer.position());
                length += writeBuffer.position();
            }

            clientSocket.write(tempReturn, 0, length);
        }

        private void error(String code, String error1, DirectBuffer error2) {
            try {
                log.warn().append("error processing http request: ")
                        .append(code).append(": ").append(error1).append(": ").append(error2)
                        .commit();

                var pre = "<html><body><h1>Error</h1><p>";
                var post = "</p></body></html>";
                var sep = "<br/>";

                // header
                var length = tempReturn.putStringWithoutLengthAscii(0, "HTTP/1.1 ");
                length += tempReturn.putStringWithoutLengthAscii(length, code);
                length += tempReturn.putStringWithoutLengthAscii(length,
                        "\r\nAccess-Control-Allow-Origin: *\r\n"
                                + "Cache-Control: no-cache\r\n"
                                + "Content-Type: text/html; charset=UTF-8\r\n"
                                + "Content-Length: ");
                var contentLength = pre.length() + error1.length() + sep.length() + error2.capacity() + post.length();
                length += tempReturn.putLongAscii(length, contentLength);
                length += tempReturn.putStringWithoutLengthAscii(length, "\r\n\r\n");

                // content
                length += tempReturn.putStringWithoutLengthAscii(length, pre);
                length += tempReturn.putStringWithoutLengthAscii(length, error1);
                length += tempReturn.putStringWithoutLengthAscii(length, sep);
                tempReturn.putBytes(length, error2, 0, error2.capacity());
                length += error2.capacity();
                length += tempReturn.putStringWithoutLengthAscii(length, post);
                clientSocket.write(tempReturn, 0, length);
            } catch (IOException e) {
                log.warn().append("error writing to client socket: ").append(clientSocket.getRemoteAddress()).commit();
            } finally {
                shellContext.close();
            }
        }
    }

    private class SseAsyncCommandContext implements AsyncCommandContext {

        private final MutableDirectBuffer writeBuffer;
        private final MutableObjectEncoder objectEncoder;

        private final Runnable cachedOnFinishRootLevel;
        private final Runnable cachedSendHeartbeat;
        private final Runnable cachedRefreshTask;

        private HttpShellContextHandler shellContextHandler;
        private WritableBufferChannel clientSocket;

        private boolean open;
        private Consumer<AsyncCommandContext> stopListener;

        private String eventType;
        private long eventId;
        private boolean eventIdSet;
        private long heartbeatTaskId;
        private long refreshTaskId;
        private long lastMessageSent;
        private boolean sentHeader;
        private Consumer<AsyncCommandContext> refreshTask;
        private Object associatedObject;
        private boolean closeOnNextHeartbeat;

        SseAsyncCommandContext() {
            objectEncoder = EncoderUtils.createJsonEncoder();
            writeBuffer = BufferUtils.allocateExpandable(4096);

            cachedOnFinishRootLevel = this::onFinishRootLevel;
            cachedSendHeartbeat = this::sendHeartbeat;
            cachedRefreshTask = this::refreshTask;
        }

        private void refreshTask() {
            if (refreshTask != null) {
                refreshTask.accept(this);
            }
        }

        @Override
        public ObjectEncoder getObjectEncoder() {
            return objectEncoder;
        }

        @Override
        public long getObjectId() {
            return eventId;
        }

        @Override
        public void setObjectId(long objectId) {
            eventId = objectId;
            eventIdSet = true;
        }

        @Override
        public void setObjectType(String objectType) {
            this.eventType = objectType;
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

            objectEncoder.setFinishRootLevelListener(cachedOnFinishRootLevel);
            objectEncoder.start(writeBuffer, 0);

            heartbeatTaskId = scheduler.scheduleEvery(
                    heartbeatTaskId, DEFAULT_HEARTBEAT, cachedSendHeartbeat,
                    "HttpShell:heartbeat", shellContextHandler.id);
        }

        @Override
        public void setRefreshTask(long nanoseconds, Consumer<AsyncCommandContext> task, String name) {
            this.refreshTask = task;
            refreshTaskId = scheduler.scheduleEvery(
                    nanoseconds, cachedRefreshTask, name, 0);
        }

        @Override
        public void cancelRefreshTask() {
            refreshTaskId = scheduler.cancel(refreshTaskId);
        }

        @Override
        public Object getAssociatedObject() {
            return associatedObject;
        }

        @SuppressWarnings("PMD.EmptyCatchBlock")
        @Override
        public void stop() {
            heartbeatTaskId = scheduler.cancel(heartbeatTaskId);
            refreshTaskId = scheduler.cancel(refreshTaskId);
            refreshTask = null;
            eventType = null;
            eventId = 0;
            eventIdSet = false;
            lastMessageSent = 0;
            sentHeader = false;
            closeOnNextHeartbeat = false;

            if (clientSocket != null) {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    // do nothing
                }
            }
            open = false;
            clientSocket = null;

            if (stopListener != null) {
                stopListener.accept(this);
                stopListener = null;
            }
            associatedObject = null;
            shellContextHandler.onStopped(this);
            shellContextHandler = null;
        }

        private void onFinishRootLevel() {
            try {
                if (open) {
                    checkSentHeader();
                    lastMessageSent = time.nanos();

                    if (eventIdSet) {
                        clientSocket.write(ID_START);
                        var eventIdLength = tempString.putLongAscii(0, eventId);
                        clientSocket.write(tempString, 0, eventIdLength);
                        clientSocket.write(NEWLINE);
                        eventIdSet = false;
                    }

                    if (eventType != null) {
                        clientSocket.write(EVENT_START);
                        var eventTypeLength = tempString.putStringWithoutLengthAscii(0, eventType);
                        clientSocket.write(tempString, 0, eventTypeLength);
                        clientSocket.write(NEWLINE);
                        eventType = null;
                    }

                    clientSocket.write(DATA_START);
                    clientSocket.write(writeBuffer, 0, objectEncoder.getEncodedLength());
                    clientSocket.write(DATA_END);

                    objectEncoder.rewind();
                }
            } catch (IOException e) {
                closeOnNextHeartbeat = true;
            }
        }

        private void sendHeartbeat() {
            try {
                if (closeOnNextHeartbeat) {
                    stop();
                } else if (open) {
                    checkSentHeader();
                    var currentTime = time.nanos();

                    if (heartbeatTaskId != 0 && open && lastMessageSent + DEFAULT_HEARTBEAT < currentTime) {
                        clientSocket.write(HEARTBEAT);
                        lastMessageSent = currentTime;
                    }
                }
            } catch (IOException e) {
                stop();
            }
        }

        private void checkSentHeader() throws IOException {
            if (!sentHeader) {
                var length = tempReturn.putStringWithoutLengthAscii(0,
                        "HTTP/1.1 200 OK\r\n"
                                + "Access-Control-Allow-Origin: *\r\n"
                                + "Cache-Control: no-cache\r\n"
                                + "Connection: keep-alive\r\n"
                                + "Content-Type: text/event-stream\r\n\r\n");
                clientSocket.write(tempReturn, 0, length);
                sentHeader = true;
            }
        }
    }

    private enum Keys {

        GET,
        POST;

        private static final Map<DirectBuffer, Keys> LOOKUP;

        static {
            LOOKUP = new CoreMap<>();
            for (var value : values()) {
                LOOKUP.put(value.nameBuf, value);
            }
        }

        private final DirectBuffer nameBuf;

        Keys() {
            nameBuf = BufferUtils.fromAsciiString(name());
        }

        static Keys valueOf(DirectBuffer buffer) {
            return LOOKUP.get(buffer);
        }
    }
}
