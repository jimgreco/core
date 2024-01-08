package com.core.platform.io;

import com.core.infrastructure.Json;
import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.EncoderUtils;
import com.core.infrastructure.encoding.MutableObjectEncoder;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.io.SocketChannel;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * This class is in progress.
 */
@SuppressWarnings("checkstyle:MissingJavadocMethod")
public class JsonRestClient implements Encodable {

    private static final DirectBuffer CONTENT_LENGTH = BufferUtils.fromAsciiString("Content-Length");

    private final Selector selector;
    private final Runnable cachedRead;
    private final Runnable cachedWrite;
    private final Runnable cachedOnConnect;

    private final MutableDirectBuffer readBuffer;
    private final MutableDirectBuffer chunkedReadBuffer;
    private final MutableDirectBuffer writeBuffer;
    private final DirectBuffer headerTagWrapper;

    private final Json json;
    private final QueryStringWriter queryStringWriter;
    private final HeaderWriter headerWriter;
    private final BodyWriter bodyWriter;

    private Runnable connectListener;
    private Runnable connectionFailedListener;
    private Consumer<Json.Value> readListener;

    private SocketChannel channel;
    private String host;

    private HttpVerb verb;
    private int writeBufferLength;
    private int readBufferLength;

    private Exception connectionFailedException;
    private String connectionFailedReason;

    /**
     * Creates a {@code JsonRestClient} with the specified {@code selector} used to create a socket.
     *
     * @param selector the selector
     */
    public JsonRestClient(Selector selector) {
        this.selector = selector;
        cachedRead = this::onRead;
        cachedWrite = this::onWrite;
        cachedOnConnect = this::onConnect;

        readBuffer = BufferUtils.allocateDirect(4096);
        chunkedReadBuffer = BufferUtils.allocateDirect(readBuffer.capacity());
        writeBuffer = BufferUtils.allocateDirect(4096);
        headerTagWrapper = BufferUtils.emptyBuffer();

        json = new Json();
        queryStringWriter = new QueryStringWriter();
        headerWriter = new HeaderWriter();
        bodyWriter = new BodyWriter();
    }

    public boolean isConnected() {
        return channel != null && channel.isConnected();
    }

    public Exception getConnectionFailedException() {
        return connectionFailedException;
    }

    public String getConnectionFailedReason() {
        return connectionFailedReason;
    }

    public void setConnectedListener(Runnable listener) {
        connectListener = listener;
    }

    public void setConnectionFailedListener(Runnable listener) {
        connectionFailedListener = listener;
    }

    public void setReadListener(Consumer<Json.Value> listener) {
        readListener = listener;
    }

    public JsonRestClient connect(String address) {
        try {
            if (channel == null) {
                host = address;

                channel = selector.createSocketChannel();
                channel.configureBlocking(false);
                channel.setReadListener(cachedRead);
                channel.setWriteListener(cachedWrite);
                channel.setConnectListener(cachedOnConnect);
                channel.connect(address);
            }

            return this;
        } catch (Exception e) {
            connectionFailedReason = "error creating channel";
            connectionFailedException = e;
            onConnectionFailed();
            return this;
        }
    }

    public void close() {
        try {
            if (channel == null) {
                return;
            }

            connectionFailedReason = null;
            connectionFailedException = null;
            readBufferLength = 0;
            writeBufferLength = 0;
            verb = null;

            var ch = channel;
            channel = null;
            ch.close();
        } catch (IOException e) {
            connectionFailedReason = "I/O error closing socket";
            connectionFailedException = e;
            onConnectionFailed();
        }
    }

    public QueryStringWriter post(DirectBuffer endpoint) {
        return request(HttpVerb.POST, endpoint);
    }

    public QueryStringWriter request(HttpVerb verb, DirectBuffer endpoint) {
        if (channel == null) {
            connect(host);
        }

        this.verb = verb;

        writeBufferLength += writeBuffer.putStringWithoutLengthAscii(writeBufferLength, verb.name());
        writeBuffer.putByte(writeBufferLength++, (byte) ' ');
        writeBuffer.putBytes(writeBufferLength, endpoint, 0, endpoint.capacity());
        writeBufferLength += endpoint.capacity();
        writeBufferLength += writeBuffer.putStringWithoutLengthAscii(writeBufferLength, " HTTP/1.1\r\n");

        return queryStringWriter.start();
    }

    private void onWrite() {
        connectionFailedReason = "write buffer full";
        onConnectionFailed();
    }

    @SuppressWarnings("checkstyle:Indentation")
    private void onRead() {
        try {
            var length = channel.read(readBuffer, readBufferLength, readBuffer.capacity() - readBufferLength);
            if (length == -1) {
                // end of file
                onConnectionFailed();
            }
            readBufferLength += length;
        } catch (IOException e) {
            connectionFailedReason = "I/O read error";
            connectionFailedException = e;
            onConnectionFailed();
        }

        while (readBufferLength > 0) {
            var contentLength = -1;
            var bodyStart = -1;

            {
                // read header
                var startOfHeaderLine = 0;
                byte c1;
                byte c2 = 0;
                byte c3 = 0;
                byte c4 = 0;
                for (var i = 0; i < readBufferLength; i++) {
                    c1 = c2;
                    c2 = c3;
                    c3 = c4;
                    c4 = readBuffer.getByte(i);

                    if (c3 == '\r' && c4 == '\n') {
                        startOfHeaderLine = i + 1;
                        if (c1 == '\r' && c2 == '\n') {
                            bodyStart = i + 1;
                            break;
                        }
                    } else if (c4 == ':') {
                        headerTagWrapper.wrap(readBuffer, startOfHeaderLine, i - startOfHeaderLine);
                        if (CONTENT_LENGTH.equals(headerTagWrapper)) {
                            contentLength = 0;
                            for (var j = i + 1; j < readBufferLength; j++) {
                                var v = readBuffer.getByte(j);
                                if (v >= '0' && v <= '9') {
                                    contentLength *= 10;
                                    contentLength += v - '0';
                                } else if (v != ' ') {
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            if (bodyStart == -1 || bodyStart >= readBufferLength) {
                // wait for more data
                return;
            }

            Json.ParseResult result;

            if (contentLength >= 0) {
                if (bodyStart + contentLength > readBufferLength) {
                    // waiting for more data
                    return;
                }

                result = json.parse(readBuffer, bodyStart, contentLength);
                bodyStart += contentLength;
                bodyStart = slurpWhitespace(bodyStart);
            } else {
                // the body is chunked
                var totalChunkedReadLength = 0;
                int chunkSize;

                do {
                    byte c1;
                    byte c2 = 0;
                    chunkSize = 0;

                    while (bodyStart < readBufferLength) {
                        c1 = c2;
                        c2 = readBuffer.getByte(bodyStart);
                        bodyStart++;

                        if (c1 == '\r' && c2 == '\n') {
                            if (bodyStart + chunkSize > readBufferLength) {
                                // not enough data for the next chunk
                                return;
                            }

                            chunkedReadBuffer.putBytes(totalChunkedReadLength, readBuffer, bodyStart, chunkSize);
                            totalChunkedReadLength += chunkSize;
                            bodyStart += chunkSize;
                            bodyStart = slurpWhitespace(bodyStart);
                            break;
                        } else if (c2 >= '0' && c2 <= '9') {
                            chunkSize *= 16;
                            chunkSize += c2 - '0';
                        } else if (c2 >= 'a' && c2 <= 'f') {
                            chunkSize *= 16;
                            chunkSize += c2 - 'a' + 10;
                        } else if (c2 >= 'A' && c2 <= 'F') {
                            chunkSize *= 16;
                            chunkSize += c2 - 'A' + 10;
                        } else if (c2 != '\r') {
                            connectionFailedReason = "illegal character in chunked output";
                            onConnectionFailed();
                            return;
                        }
                    }
                } while (chunkSize != 0);

                result = json.parse(chunkedReadBuffer, 0, totalChunkedReadLength);
            }

            if (result.isError()) {
                connectionFailedReason = result.getErrorReason();
                onConnectionFailed();
            }

            readListener.accept(result.getRoot());

            var remainingBytes = readBufferLength - bodyStart;
            BufferUtils.compact(readBuffer, bodyStart, remainingBytes);
            readBufferLength = remainingBytes;
        }
    }

    private int slurpWhitespace(int start) {
        for (var i = start; i < readBufferLength; i++) {
            var c1 = readBuffer.getByte(i);
            if (!Character.isWhitespace(c1)) {
                return i;
            }
        }
        return readBufferLength;
    }

    private void writeToSocket() {
        try {
            if (isConnected()) {
                channel.write(writeBuffer, 0, writeBufferLength);
            }
        } catch (IOException e) {
            connectionFailedReason = "I/O error writing to socket";
            connectionFailedException = e;
            onConnectionFailed();
        }
    }

    private void onConnect() {
        writeToSocket();

        if (connectListener != null) {
            connectListener.run();
        }
    }

    private void onConnectionFailed() {
        if (connectionFailedListener != null) {
            connectionFailedListener.run();
        }
    }

    private void crlf() {
        writeBufferLength += writeBuffer.putStringWithoutLengthAscii(writeBufferLength, "\r\n");
    }

    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("host").string(host)
                .string("connected").bool(channel != null && channel.isConnected())
                .string("handshakeComplete").bool(isConnected());

        if (writeBufferLength > 0) {
            encoder.string("bufferedWrite").number(writeBufferLength);
        }
        if (connectionFailedReason != null) {
            encoder.string("connectionFailedReason").string(connectionFailedReason);
        }

        encoder.closeMap();
    }

    @Override
    public String toString() {
        return toEncodedString();
    }

    /**
     * This object is used to write the query string in a URL.
     */
    public class QueryStringWriter {

        boolean first;

        QueryStringWriter() {
        }

        private QueryStringWriter start() {
            writeBufferLength += writeBuffer.putStringWithoutLengthAscii(writeBufferLength, "Host");
            writeBufferLength += writeBuffer.putStringWithoutLengthAscii(writeBufferLength, ": ");
            writeBufferLength += writeBuffer.putStringWithoutLengthAscii(writeBufferLength, host);
            first = true;
            return this;
        }

        public HeaderWriter header(String key, String value) {
            return headerWriter.start().header(key, value);
        }

        public HeaderWriter header(String key, DirectBuffer value) {
            return headerWriter.start().header(key, value);
        }

        public ObjectEncoder json() {
            return headerWriter.start().body().json();
        }

        public ObjectEncoder keyValue() {
            return headerWriter.start().body().keyValue();
        }

        public void send() {
            headerWriter.start().body().end();
        }

        public QueryStringWriter param(String key, String value) {
            writeDelimiter();

            writeBufferLength += writeBuffer.putStringWithoutLengthAscii(writeBufferLength, key);

            writeBuffer.putByte(writeBufferLength++, (byte) '=');

            writeBufferLength += writeBuffer.putStringWithoutLengthAscii(writeBufferLength, value);

            return this;
        }

        public QueryStringWriter param(String key, DirectBuffer value) {
            writeDelimiter();

            writeBufferLength += writeBuffer.putStringWithoutLengthAscii(writeBufferLength, key);

            writeBuffer.putByte(writeBufferLength++, (byte) '=');

            writeBuffer.putBytes(writeBufferLength, value, 0, value.capacity());
            writeBufferLength += value.capacity();

            return this;
        }

        private void writeDelimiter() {
            if (first) {
                writeBuffer.putByte(writeBufferLength++, (byte) '?');
                first = false;
            } else {
                writeBuffer.putByte(writeBufferLength++, (byte) '&');
            }
        }
    }

    /**
     * This object is used to write header fields in an HTTP request.
     */
    public class HeaderWriter {

        HeaderWriter() {
        }

        private HeaderWriter start() {
            header("User-Agent", "java");
            header("Accept", "*/*");
            header("Connection", "Keep-Alive");
            header("Keep-Alive", "timeout=120, max=0");
            return this;
        }

        public BodyWriter body() {
            return bodyWriter.start();
        }

        public void send() {
            body().end();
        }

        public HeaderWriter header(String key, String value) {
            writeBufferLength += writeBuffer.putStringWithoutLengthAscii(writeBufferLength, key);

            writeBufferLength += writeBuffer.putStringWithoutLengthAscii(writeBufferLength, ": ");

            writeBufferLength += writeBuffer.putStringWithoutLengthAscii(writeBufferLength, value);

            crlf();
            return this;
        }

        public HeaderWriter header(String key, DirectBuffer value) {
            writeBufferLength += writeBuffer.putStringWithoutLengthAscii(writeBufferLength, key);

            writeBufferLength += writeBuffer.putStringWithoutLengthAscii(writeBufferLength, ": ");

            writeBuffer.putBytes(writeBufferLength, value, 0, value.capacity());
            writeBufferLength += value.capacity();

            crlf();
            return this;
        }
    }

    /**
     * This object is used to write the body of an HTTP message.
     */
    public class BodyWriter {

        private final MutableObjectEncoder query;
        private final MutableObjectEncoder json;

        private int contentLengthOffset;
        private int bodyOffset;

        BodyWriter() {
            query = EncoderUtils.createQueryEncoder();
            query.setFinishRootLevelListener(this::onKeyValue);

            json = EncoderUtils.createJsonEncoder();
            json.setFinishRootLevelListener(this::onJsonRoot);
        }

        private BodyWriter start() {
            if (verb == HttpVerb.GET) {
                headerWriter.header("Content-Length", "0");
            } else {
                headerWriter.header("Content-Length", "00000");
                contentLengthOffset = writeBufferLength - "00000\r\n".length();
            }

            crlf();

            bodyOffset = writeBufferLength;
            return this;
        }

        public ObjectEncoder json() {
            return json.start(writeBuffer, writeBufferLength);
        }

        public ObjectEncoder keyValue() {
            query.start(writeBuffer, writeBufferLength);
            return query;
        }

        private void onJsonRoot() {
            writeBufferLength += json.getEncodedLength();
            end();
        }

        private void onKeyValue() {
            writeBufferLength += query.getEncodedLength();
            end();
        }

        private void end() {
            if (verb != HttpVerb.GET) {
                writeBuffer.putNaturalPaddedIntAscii(contentLengthOffset, 5, writeBufferLength - bodyOffset);
            }

            writeToSocket();
        }
    }
}
