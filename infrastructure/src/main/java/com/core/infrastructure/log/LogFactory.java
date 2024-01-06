package com.core.infrastructure.log;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.collections.CoreMap;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.io.DirectBufferChannel;
import com.core.infrastructure.time.Time;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A factory to create logs and manage the medium (sink) that is logged to.
 *
 * <p>The logging infrastructure is initialized with an in-memory log so that the application can log messages before
 * the log medium has been created.
 * When the log sink is specified with the {@link #logSink(LogSink)}, the contents the in-memory log are transferred to
 * the new sink.
 */
public class LogFactory {

    private final Time time;
    private final List<String> logIdentifiers;
    private final Map<String, Log> idToLogs;
    private DirectBufferChannel tempLog;
    private LogSink logSink;
    private boolean debug;

    /**
     * Creates a {@code LogFactory} with the specified log identifiers.
     *
     * @param time the time source
     */
    public LogFactory(Time time) {
        this.time = Objects.requireNonNull(time, "time is null");

        idToLogs = new CoreMap<>();
        logIdentifiers = new CoreList<>();
        tempLog = new DirectBufferChannel();
        logSink = new ChannelLogSink(tempLog);
    }

    /**
     * Sets the log {@code identifier} at the specified {@code index}.
     * The log identifier will be pre-pended to all log names with joining slashes ('/').
     * For example, if the log identifiers are ["foo", "bar"] and the log name is "soo" then the log identifier will be
     * "/foo/bar/soo".
     * If the value is null, the log identifier is removed at the specified index.
     *
     * @param index the index of the log identifier to set
     * @param identifier the log identifier
     * @throws IndexOutOfBoundsException if an {@code index} is specified is greater than the number of log identifiers
     *     currently specified in the factory
     */
    @Command
    public void setLogIdentifier(int index, String identifier) {
        if (identifier == null) {
            logIdentifiers.remove(index);
        } else if (index == logIdentifiers.size()) {
            logIdentifiers.add(identifier);
        } else {
            logIdentifiers.set(index, identifier);
        }
    }

    /**
     * Sets the sink for logs to write log messages to and dumps the contents of the old log sink into the new log sink.
     * The logging infrastructure is initialized with an in-memory log so that the application can log messages before
     * the log medium has been created.
     * When the log sink is specified with this method, the contents the in-memory log are transferred to the new sink.
     *
     * @param logSink the log sink
     * @throws IOException if an I/O error occurs
     * @throws IllegalStateException if this method has already been invoke
     */
    @Command
    public void logSink(LogSink logSink) throws IOException {
        if (this.tempLog == null) {
            throw new IllegalStateException("log sink is already set");
        }
        this.logSink = Objects.requireNonNull(logSink);
        logSink.dump(tempLog.getBuffer(), 0, (int) tempLog.position());
        tempLog = null;

        for (var log : idToLogs.values()) {
            log.setLogSink(logSink);
        }
    }

    /**
     * Creates a log with the specified log name.
     * The log identifiers previously specified with {@link #setLogIdentifier(int, String) setLogIdentifier} will be
     * pre-pended to the log name with joining slashes ('/').
     * For example, if the log identifiers are ["foo", "bar"] and the log name is "soo" then the log identifier will be
     * "/foo/bar/soo".
     *
     * @param logName the name of the log
     * @return a log with the specified {@code name}
     */
    public Log create(String logName) {
        var logId = String.join("/", logIdentifiers) + "/" + logName;
        var log = idToLogs.get(logId);
        if (log == null) {
            log = new Log(time, logSink, BufferUtils.fromAsciiString(logId));
            log.setDebug(debug);
            idToLogs.put(logId, log);
        }
        return log;
    }

    /**
     * Creates a log with the simple name of the specified class.
     *
     * @param clzName the class
     * @return a log with the simple name of the specified class
     */
    public Log create(Class<?> clzName) {
        return create(clzName.getSimpleName());
    }

    /**
     * Sets the debug flag for all logs in the factory.
     *
     * @param debug true if the logs are to be set to debug mode
     */
    @Command(path = "debugForAll")
    public void setDebugForAll(boolean debug) {
        this.debug = debug;
        for (var log : idToLogs.values()) {
            log.setDebug(debug);
        }
    }

    /**
     * Sets the debug flag for all logs that match the specified log identifier.
     * The {@code logIdentifier} value does not have to be a complete identifier.
     * For example, if there are logs created with "/vm01/app01a/foo" and "/vm01/app02a/foo" and "foo" is specified
     * as the {@code logIdentifier} value then both logs would be set to debug.
     * Similarly, if there are logs created with "/vm01/app01a/foo" and "/vm01/app01a/bar" and "app01a" is specified
     * as the {@code logIdentifier} value then both logs would be set to debug.
     *
     * @param logIdentifier the identifier of the log to set the debug status for
     * @param debug true if the logs are to be set to debug mode
     */
    @Command(path = "debug")
    public void setDebug(String logIdentifier, boolean debug) {
        for (var logEntry : idToLogs.entrySet()) {
            if (logEntry.getKey().contains(logIdentifier)) {
                logEntry.getValue().setDebug(debug);
            }
        }
    }
}
