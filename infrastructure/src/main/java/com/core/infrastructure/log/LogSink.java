package com.core.infrastructure.log;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;

/**
 * A log sink abstracts the medium (e.g., a file, syslog) that the log writes to.
 */
public interface LogSink {

    /**
     * Returns a buffer to write a new log statement to.
     * The specified log level, log names, and timestamp will be added to the log statement per the sink's
     * implementation.
     * The log statement is not finalized until {@link #commit(int)} is invoked with the number of bytes in the log
     * statement.
     *
     * <code>
     *     var logBuffer = logSink.start(LogLevel.INFO, logId, timestamp);
     *     logBuffer.putAsciiWithoutLength(0, "hello!");
     *     logSink.commit(6); // length of "hello!"
     * </code>
     *
     * @param logLevel the level of the log
     * @param logId the identifier for the log that is writing the log statement
     * @param timestamp the time the log statement was created
     * @return the buffer to write the log statement to
     */
    MutableDirectBuffer start(LogLevel logLevel, DirectBuffer logId, long timestamp);

    /**
     * Commits the log statement.
     *
     * @param length the number of bytes in the log message
     * @throws IOException if an I/O error occurs
     */
    void commit(int length) throws IOException;

    /**
     * Copies the buffer, byte-for-byte, into the sink.
     *
     * @param buffer the buffer to copy
     * @param index the first byte of the buffer to copy
     * @param length the number of bytes to copy
     */
    void dump(DirectBuffer buffer, int index, int length);
}
