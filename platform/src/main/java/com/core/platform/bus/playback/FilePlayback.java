package com.core.platform.bus.playback;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.EventLoop;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.collections.Deque;
import com.core.infrastructure.collections.LinkedList;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Directory;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.messages.Dispatcher;
import com.core.infrastructure.messages.MessagePublisher;
import com.core.infrastructure.messages.Provider;
import com.core.infrastructure.messages.Schema;
import com.core.infrastructure.time.ManualTime;
import com.core.infrastructure.time.Time;
import com.core.platform.activation.Activatable;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.bus.AbstractBusServer;
import com.core.platform.bus.BusClient;
import com.core.platform.bus.BusServer;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.eclipse.collections.api.map.primitive.MutableShortShortMap;
import org.eclipse.collections.api.set.primitive.MutableShortSet;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.map.mutable.primitive.ShortShortHashMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.set.mutable.primitive.ShortHashSet;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.List;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * File playback provides a way to replay the messages in a corefile through the trading system.
 * A corefile typically contains an entire session's worth of messages.
 *
 * <p>This object instantiates an implementation of {@link BusClient} that applications use listen to for messages and
 * send messages to the sequencer.
 * The Sequencer uses an implementation of {@link BusServer} that dispatches messages sent from the client and
 * dispatches reads messages from the corefile.
 *
 * <p>This class also manages the simulated system time.
 * The system time is set to the timestamp recorded in the message file.
 * In this way, components that have time-based requirements (e.g., algos) can behave in a deterministic fashion.
 *
 * @param <DispatcherT> the message dispatcher type
 * @param <ProviderT> the message provider type
 */
public class FilePlayback<DispatcherT extends Dispatcher, ProviderT extends Provider> implements Consumer<String> {

    private final ManualTime time;
    private final EventLoop eventLoop;
    private final ActivatorFactory activatorFactory;
    private final Schema<DispatcherT, ProviderT> schema;
    private final DispatcherT dispatcher;

    @Directory(path = "/busServer")
    private final FileBusServer busServer;
    @Directory(path = "/bus")
    private final FileBusClient busClient;

    private final String appDefMsgName;
    private final String appDefNameField;

    private final Set<DirectBuffer> appNamesToIgnore;
    private final MutableShortSet appIdsToIgnore;
    private final MutableShortShortMap remapAppIds;

    private final byte[] readBytes;
    private final DirectBuffer readWrapper;
    private final DirectBuffer messageWrapper;
    private final Log log;

    private final List<MessageTranslator> translators;

    private MessageTranslator[] filteredTranslators;
    private boolean initialized;
    private short fileAppId;
    private short seqAppId;
    private String session;

    /**
     * Constructs the {@code FilePlayback} object with the specified properties.
     *
     * @param time the time
     * @param eventLoop the event loop
     * @param logFactory a factory to create logs
     * @param activatorFactory a factory to create activators
     * @param schema the message schema
     */
    public FilePlayback(
            Time time, EventLoop eventLoop, LogFactory logFactory, ActivatorFactory activatorFactory,
            Schema<DispatcherT, ProviderT> schema) {
        this.time = (ManualTime) Objects.requireNonNull(time, "time is null");
        this.eventLoop = Objects.requireNonNull(eventLoop, "eventService is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        this.activatorFactory = Objects.requireNonNull(activatorFactory, "activatorFactory is null");
        this.schema = Objects.requireNonNull(schema, "schema is null");

        log = logFactory.create(FilePlayback.class);

        readBytes = new byte[MessageTranslator.MESSAGE_BUFFER_SIZE];
        readWrapper = BufferUtils.emptyBuffer();
        messageWrapper = BufferUtils.emptyBuffer();

        dispatcher = schema.createDispatcher();
        appDefMsgName = schema.getProperty("applicationDefinitionMessageName");
        appDefNameField = schema.getProperty("applicationDefinitionNameField");

        appNamesToIgnore = new UnifiedSet<>();
        appIdsToIgnore = new ShortHashSet();
        remapAppIds = new ShortShortHashMap();
        translators = new CoreList<>();

        busServer = new FileBusServer();
        busClient = new FileBusClient();

        dispatcher.addListener(appDefMsgName, decoder -> seqAppId = decoder.getApplicationId());
    }

    /**
     * Request the sequencer to suppress commands from an application.
     *
     * <p>Usually, this application will be the sequencer application from the command file
     * which is being played back, but it can be any other.
     *
     * @param applicationName name of application to suppress commands for
     */
    @Command
    public void deny(DirectBuffer applicationName) {
        log.info().append("suppress commands from application: ").append(applicationName).commit();
        appNamesToIgnore.add(BufferUtils.copy(applicationName));
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
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(fileName);
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
            try {
                var read = inputStream.read(readBytes, offset, readBytes.length - offset);
                if (read == -1) {
                    eventLoop.exit();
                } else {
                    offset += read;
                    while (offset > 1) {
                        // read the message in
                        readWrapper.wrap(readBytes, 0, offset);
                        var messageLength = readWrapper.getShort(0);
                        var lengthPlusMessageLength = Short.BYTES + messageLength;
                        if (readWrapper.capacity() < lengthPlusMessageLength) {
                            // check to see we have enough bytes for the full message
                            break;
                        }
                        messageWrapper.wrap(readWrapper, Short.BYTES, messageLength);

                        // set the "system time"
                        var timestamp = messageWrapper.getLong(schema.getTimestampOffset());
                        time.setNanos(timestamp);

                        // dispatch the message
                        processMessage(messageWrapper);

                        System.arraycopy(
                                readBytes, lengthPlusMessageLength, readBytes,
                                0, offset - lengthPlusMessageLength);
                        offset -= lengthPlusMessageLength;
                    }
                    eventLoop.runOnce();
                }
            } catch (IOException e) {
                eventLoop.exit();
            }
        }

        for (var listener : busClient.closeListeners) {
            listener.run();
        }

        try {
            inputStream.close();
        } catch (IOException e) {
            // do nothing
        }
    }

    private void processMessage(DirectBuffer message) {
        if (!initialized) {
            initialized = true;
            busServer.activator.ready();
            busClient.activator.ready();
        }

        var decoder = dispatcher.getDecoder(message, 0, message.capacity());
        if (decoder == null) {
            throw new IllegalStateException("unknown message");
        }

        fileAppId = decoder.getApplicationId();
        seqAppId = 0;
        var remapAppId = remapAppIds.get(fileAppId);

        var isAppDefMsg = decoder.messageName().equals(appDefMsgName);
        if (isAppDefMsg) {
            var appName = (DirectBuffer) decoder.get(appDefNameField);
            if (appNamesToIgnore.contains(appName)) {
                appIdsToIgnore.add(fileAppId);
                return;
            }
        }

        if (appIdsToIgnore.contains(fileAppId)) {
            return;
        }

        var mutableMessage = (MutableDirectBuffer) message;
        mutableMessage.putShort(schema.getApplicationIdOffset(), remapAppId);

        var translatedMessage = translate(mutableMessage);

        if (translatedMessage != null) {
            busServer.commandListener.accept(translatedMessage);
        }

        fileAppId = 0;
        seqAppId = 0;
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

    private class FileBusClient implements BusClient<DispatcherT, ProviderT> {

        private final Activator activator;
        private final Map<String, ProviderT> appNameToProvider;
        private final List<Runnable> openListeners;
        private final List<Runnable> closeListeners;

        FileBusClient() {
            appNameToProvider = new UnifiedMap<>();
            activator = activatorFactory.createActivator("FileBusClient", this);
            openListeners = new CoreList<>();
            closeListeners = new CoreList<>();
        }

        @Override
        public Schema<DispatcherT, ProviderT> getSchema() {
            return schema;
        }

        @Override
        public DispatcherT getDispatcher() {
            return dispatcher;
        }

        @Override
        public ProviderT getProvider(String applicationName, Object associatedObject) {
            var provider = appNameToProvider.get(applicationName);
            if (provider == null) {
                var messagePublisher = new FileMessagePublisher(applicationName);
                provider = schema.createProvider(messagePublisher);
                appNameToProvider.put(applicationName, provider);

                var activator = activatorFactory.createActivator(
                        "FileMessageProvider:" + applicationName, provider, messagePublisher);
                activator.ready();
            }
            return provider;
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

    private class FileMessagePublisher implements MessagePublisher, Activatable {

        private final Activator activator;
        private final DirectBuffer appNameBuf;
        private final Deque<DirectBuffer> queuedMessages;
        private final String appName;

        private short appId;
        private int appSeqNum;
        private MutableDirectBuffer messageBuffer;

        FileMessagePublisher(String applicationName) {
            appName = applicationName;
            appNameBuf = BufferUtils.fromAsciiString(applicationName);
            queuedMessages = new LinkedList<>();

            activator = activatorFactory.createActivator(
                    "FileMessagePublisher:" + applicationName, this, busClient);

            dispatcher.addListener(appDefMsgName, decoder -> {
                var appName = (DirectBuffer) decoder.get(appDefNameField);
                if (appId == 0 && appName.equals(this.appNameBuf)) {
                    appId = decoder.getApplicationId();
                    activator.ready();
                }
            });

            var appDefEncoder = schema.createEncoder(appDefMsgName);
            appDefEncoder.wrap(acquire());
            appDefEncoder.set(appDefNameField, appNameBuf);
            commit(appDefEncoder.length());
        }

        @Override
        public void activate() {
            send();
        }

        @Override
        public void deactivate() {
            activator.notReady();
        }

        @Override
        public MutableDirectBuffer acquire() {
            messageBuffer = BufferUtils.allocate(MessageTranslator.MESSAGE_BUFFER_SIZE);
            return messageBuffer;
        }

        @Override
        public void commit(int commandLength) {
            // command
            messageBuffer.putShort(schema.getApplicationIdOffset(), appId);
            messageBuffer.putInt(schema.getApplicationSequenceNumberOffset(), ++appSeqNum);
            queuedMessages.addLast(BufferUtils.wrap(messageBuffer, 0, commandLength));
            messageBuffer = null;
        }

        @Override
        public void send() {
            while (!queuedMessages.isEmpty() && (activator.isActivating() || activator.isActive())) {
                var message = queuedMessages.removeFirst();
                busServer.commandListener.accept(message);
            }
        }

        @Override
        public String getApplicationName() {
            return appName;
        }

        @Override
        public short getApplicationId() {
            return appId;
        }

        @Override
        public boolean isCurrent() {
            return true;
        }

    }

    private class FileBusServer extends AbstractBusServer<DispatcherT, ProviderT> {

        private final Activator activator;
        private final Deque<DirectBuffer> queuedMessages;

        private Consumer<DirectBuffer> commandListener;
        private MutableDirectBuffer messageBuffer;
        private boolean processing;

        FileBusServer() {
            super(schema);
            queuedMessages = new LinkedList<>();
            activator = activatorFactory.createActivator("FileBusServer", this);
        }

        @Override
        public MutableDirectBuffer acquire() {
            messageBuffer = BufferUtils.allocate(MessageTranslator.MESSAGE_BUFFER_SIZE);
            return messageBuffer;
        }

        @Override
        public void commit(int msgLength) {
            messageBuffer.putLong(schema.getTimestampOffset(), time.nanos());
            queuedMessages.addLast(BufferUtils.wrap(messageBuffer, 0, msgLength));
            messageBuffer = null;
        }

        @Override
        public void commit(int msgLength, long timestamp) {
            messageBuffer.putLong(schema.getTimestampOffset(), timestamp);
            queuedMessages.addLast(BufferUtils.wrap(messageBuffer, 0, msgLength));
            messageBuffer = null;
        }

        @Override
        public boolean isActive() {
            return activator.isActive();
        }

        @Override
        public void send() {
            if (processing) {
                return;
            }

            processing = true;

            while (!queuedMessages.isEmpty()) {
                var message = queuedMessages.removeFirst();

                // dispatch to clients
                dispatcher.dispatch(message, 0, message.capacity());

                if (fileAppId != 0 && seqAppId != 0) {
                    remapAppIds.put(fileAppId, seqAppId);
                    fileAppId = 0;
                    seqAppId = 0;
                }
            }

            processing = false;

            if (!queuedMessages.isEmpty()) {
                send();
            }
        }

        @Override
        public void addEventListener(Consumer<DirectBuffer> eventListener) {
            // do nothing, can only act as a server
        }

        @Override
        public void setCommandListener(Consumer<DirectBuffer> commandListener) {
            this.commandListener = commandListener;
        }
    }
}
