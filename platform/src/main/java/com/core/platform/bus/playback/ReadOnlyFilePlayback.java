package com.core.platform.bus.playback;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.EventLoop;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Directory;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.messages.Dispatcher;
import com.core.infrastructure.messages.Provider;
import com.core.infrastructure.messages.Schema;
import com.core.infrastructure.time.ManualTime;
import com.core.infrastructure.time.Time;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.bus.BusClient;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

/**
 * File playback provides a way to replay the messages in a corefile through the trading system.
 * A corefile typically contains an entire session's worth of messages.
 *
 * <p>This object instantiates an implementation of {@link BusClient} that applications use listen to for messages.
 *
 * <p>This class also manages the simulated system time.
 * The system time is set to the timestamp recorded in the message file.
 * In this way, components that have time-based requirements (e.g., algos) can behave in a deterministic fashion.
 *
 * <p>This will not work with applications that write to a bus.
 * See {@link FilePlayback} for that implementation.
 */
public class ReadOnlyFilePlayback implements Consumer<String> {

    private static final int READ_BUFFER_SIZE = 32 * 1024;

    private final ManualTime time;
    private final EventLoop eventLoop;
    private final ActivatorFactory activatorFactory;
    private final Schema<Dispatcher, Provider> schema;
    private final Dispatcher dispatcher;

    @Directory(path = "/bus")
    private final FileBusClient busClient;

    private final byte[] readBytes;
    private final DirectBuffer readWrapper;
    private final DirectBuffer messageWrapper;

    private final List<MessageTranslator> translators;

    private MessageTranslator[] filteredTranslators;
    private boolean initialized;
    private String session;

    /**
     * Constructs the {@code ReadOnlyFilePlayback} object with the specified properties.
     *
     * @param time             the time
     * @param eventLoop        the event loop
     * @param logFactory       a factory to create logs
     * @param activatorFactory a factory to create activators
     * @param schema           the message schema
     */
    public ReadOnlyFilePlayback(
            Time time, EventLoop eventLoop, LogFactory logFactory, ActivatorFactory activatorFactory,
            Schema<Dispatcher, Provider> schema) {
        this.time = (ManualTime) Objects.requireNonNull(time, "time is null");
        this.eventLoop = Objects.requireNonNull(eventLoop, "eventService is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        this.activatorFactory = Objects.requireNonNull(activatorFactory, "activatorFactory is null");
        this.schema = Objects.requireNonNull(schema, "schema is null");
        readBytes = new byte[READ_BUFFER_SIZE];
        readWrapper = BufferUtils.emptyBuffer();
        messageWrapper = BufferUtils.emptyBuffer();

        dispatcher = schema.createDispatcher();
        translators = new CoreList<>();
        busClient = new FileBusClient();
    }

    /**
     * Adds the translator to be processed for each message.
     * All translators will be sorted by date and only be applied to messages that come from the session before the
     * specified date.
     *
     * @param translator the translator
     */
    @Command
    public void addTranslator(MessageTranslator translator) {
        translators.add(translator);
    }

    @SuppressWarnings("PMD.EmptyCatchBlock")
    @Override
    public void accept(String fileName) {
        InputStream inputStream;
        try {
            inputStream = fileName.endsWith(".dat.gz")
                    ? new GZIPInputStream(new FileInputStream(fileName))
                    : new FileInputStream(fileName);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        var path = Path.of(fileName);
        session = path.getName(path.getNameCount() - 1).toString().split("\\.")[0];

        for (var listener : busClient.openListeners) {
            listener.run();
        }

        var offset = 0;
        while (!eventLoop.isDone()) {
            var bytesRead = readFromOffset(inputStream, offset);

            if (bytesRead == -1) {
                eventLoop.exit();
                break;
            }

            var pos = 0;
            offset += bytesRead;
            while (pos < offset - 1) {
                // read the message in
                readWrapper.wrap(readBytes, pos, offset - pos);
                var messageLength = readWrapper.getShort(0);
                var lengthPlusMessageLength = Short.BYTES + messageLength;
                if (offset - pos < lengthPlusMessageLength) {
                    // check to see we have enough bytes for the full message
                    break;
                }
                messageWrapper.wrap(readWrapper, Short.BYTES, messageLength);
                // set the "system time"
                var timestamp = messageWrapper.getLong(schema.getTimestampOffset());
                time.setNanos(timestamp);

                // dispatch the message
                processMessage(messageWrapper);
                runEventLoop();
                pos += lengthPlusMessageLength;
            }
            System.arraycopy(
                    readBytes, pos, readBytes,
                    0, offset - pos);
            offset -= pos;
        }

        for (var listener : busClient.closeListeners) {
            listener.run();
        }

        try {
            inputStream.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void runEventLoop() {
        try {
            eventLoop.runOnce();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private int readFromOffset(InputStream inputStream, int offset) {
        var bytesRead = 0;
        try {
            bytesRead = inputStream.read(readBytes, offset, readBytes.length - offset);
        } catch (IOException e) {
            eventLoop.exit();
            throw new UncheckedIOException(e);
        }
        return bytesRead;
    }

    private void processMessage(DirectBuffer message) {
        if (!initialized) {
            initialized = true;
            busClient.activator.ready();
        }

        var decoder = dispatcher.getDecoder(message, 0, message.capacity());
        if (decoder == null) {
            throw new IllegalStateException("unknown message");
        }

        var mutableMessage = (MutableDirectBuffer) message;
        var translatedMessage = translate(mutableMessage);

        if (translatedMessage != null) {
            dispatcher.dispatch(translatedMessage, 0, translatedMessage.capacity());
        }
    }

    private DirectBuffer translate(MutableDirectBuffer message) {
        if (filteredTranslators == null) {
            var timestamp = message.getLong(schema.getTimestampOffset());
            var firstMessageEpochDay = Instant.ofEpochSecond(TimeUnit.NANOSECONDS.toSeconds(timestamp))
                    .atOffset(ZoneOffset.UTC).toLocalDate().toEpochDay();
            filteredTranslators = translators.stream()
                    .sorted(Comparator.comparing(MessageTranslator::getLastDate))
                    .filter(translator -> firstMessageEpochDay < translator.getLastDate().toEpochDay())
                    .toArray(MessageTranslator[]::new);
        }

        var translatedMessage = message;
        for (var filteredTranslator : filteredTranslators) {
            translatedMessage = filteredTranslator.translate(translatedMessage);
        }

        return translatedMessage;
    }

    private class FileBusClient implements BusClient<Dispatcher, Provider> {

        private final Activator activator;
        private final List<Runnable> openListeners;
        private final List<Runnable> closeListeners;

        FileBusClient() {
            activator = activatorFactory.createActivator("FileBusClient", this);
            openListeners = new CoreList<>();
            closeListeners = new CoreList<>();
        }

        @Override
        public Schema<Dispatcher, Provider> getSchema() {
            return schema;
        }

        @Override
        public Dispatcher getDispatcher() {
            return dispatcher;
        }

        @Override
        public Provider getProvider(String applicationName, Object associatedObject) {
            return null;
        }

        @Override
        public String getSession() {
            return session;
        }

        @Override
        public void addOpenSessionListener(Runnable listener) {
            openListeners.add(listener);
        }

        @Override
        public void addCloseSessionListener(Runnable listener) {
            closeListeners.add(listener);
        }
    }
}
