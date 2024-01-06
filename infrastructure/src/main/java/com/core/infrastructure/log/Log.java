package com.core.infrastructure.log;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.buffer.BufferNumberUtils;
import com.core.infrastructure.time.Time;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;

/**
 * A Logger object is used to log messages for a specific system or application component.
 * Loggers are normally named, using a hierarchical space-separated namespace.
 * Logger names can be arbitrary strings, but they should normally be based on the application hierarchy
 * (e.g., Virtual_Machine_Name Application_Name Component_Name).
 */
public class Log {

    private final Time time;
    private final DirectBuffer logIdentifier;
    private final Statement logStatement;
    private LogSink logSink;
    private boolean debug;
    private MutableDirectBuffer buffer;
    private int offset;

    Log(Time time, LogSink logSink, DirectBuffer logIdentifier) {
        this.logSink = logSink;
        this.time = time;
        this.logIdentifier = logIdentifier;
        logStatement = new Statement();
    }

    /**
     * Sets the log sink.
     *
     * @param logSink the log sink
     */
    void setLogSink(LogSink logSink) {
        this.logSink = logSink;
    }

    /**
     * Returns true if the log is in debug mode.
     *
     * @return true if the log is in debug mode
     */
    public boolean isDebug() {
        return debug;
    }

    /**
     * Sets whether the log is in debug mode.
     *
     * @param debug true if the log is in debug mode
     */
    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    /**
     * Starts a debug-level log message.
     *
     * @return a debug-level log message
     */
    public Statement debug() {
        return log(LogLevel.DEBUG);
    }

    /**
     * Starts an info-level log message.
     *
     * @return an info-level log message
     */
    public Statement info() {
        return log(LogLevel.INFO);
    }

    /**
     * Starts an warn-level log message.
     *
     * @return an warn-level log message
     */
    public Statement warn() {
        return log(LogLevel.WARN);
    }

    /**
     * Starts an error-level log message.
     *
     * @return an error-level log message
     */
    public Statement error() {
        return log(LogLevel.ERROR);
    }

    /**
     * Starts a log message with the specified {@code logLevel}.
     *
     * @param logLevel the log level
     * @return a log message with the specified {@code logLevel}
     */
    public Statement log(LogLevel logLevel) {
        buffer = logSink.start(logLevel, logIdentifier, time.nanos());
        offset = 0;
        return logStatement;
    }

    /**
     * A log statement.
     */
    public class Statement {

        private Statement() {
        }

        /**
         * Appends the buffer to the log statement.
         *
         * @param value the value to append
         * @return this statement
         */
        public Statement append(DirectBuffer value) {
            if (value != null) {
                buffer.putBytes(offset, value, 0, value.capacity());
                offset += value.capacity();
            }
            return this;
        }

        /**
         * Appends the buffer to the log statement.
         *
         * @param value the value to append
         * @param index the first byte of the buffer to append
         * @param length the length of the buffer to append
         * @return this statement
         */
        public Statement append(DirectBuffer value, int index, int length) {
            if (value != null) {
                buffer.putBytes(offset, value, index, length);
                offset += length;
            }
            return this;
        }

        /**
         * Appends the string to the log statement.
         *
         * @param value the value to append
         * @return this statement
         */
        public Statement append(String value) {
            offset += buffer.putStringWithoutLengthAscii(offset, value);
            return this;
        }

        /**
         * Appends the long as an ASCII string to the log statement.
         *
         * @param value the value to append
         * @return this statement
         */
        public Statement append(long value) {
            offset += buffer.putLongAscii(offset, value);
            return this;
        }

        /**
         * Appends the long as an ASCII string to the log statement with printing between {@code 0} and {@code 8}
         * decimal places.
         *
         * @param value the value to append
         * @return this statement
         */
        public Statement append(double value) {
            offset += BufferNumberUtils.putAsAsciiDecimal(buffer, offset, value);
            return this;
        }

        /**
         * Appends the long as an ASCII string to the log statement with printing 0 minimal decimals places and
         * {@code 8} maximum decimal places.
         *
         * @param value the value to append
         * @param minDecimals the minimum number of decimal places to print
         * @return this statement
         */
        public Statement append(double value, int minDecimals) {
            offset += BufferNumberUtils.putAsAsciiDecimal(buffer, offset, value, minDecimals);
            return this;
        }

        /**
         * Appends the long as an ASCII string to the log statement with the specified minimum and maximum number of
         * decimal places.
         *
         * @param value the value to append
         * @param minDecimals the minimum number of decimals to print
         * @param maxDecimals the maximum number of decimals to print
         * @return this statement
         */
        public Statement append(double value, int minDecimals, int maxDecimals) {
            offset += BufferNumberUtils.putAsAsciiDecimal(buffer, offset, value, minDecimals, maxDecimals);
            return this;
        }

        /**
         * Appends the booelean as an ASCII string to the log statement.
         *
         * @param value the value to append
         * @return this statement
         */
        public Statement append(boolean value) {
            offset += BufferUtils.putBoolAscii(buffer, offset, value);
            return this;
        }

        /**
         * Appends the character to the log statement.
         *
         * @param value the value to append
         * @return this statement
         */
        public Statement append(char value) {
            buffer.putByte(offset, (byte) value);
            offset++;
            return this;
        }

        /**
         * Appends the object, converted to a string with the {@code value.toString()} method, to the log statement.
         *
         * @param value the value to append
         * @return this statement
         */
        public Statement append(Object value) {
            if (value != null) {
                append(value.toString());
            }
            return this;
        }

        /**
         * Appends the throwable to the log statement.
         * The log statement will print the throwable's message and then a stack trace element on a separate line.
         * Then it will do so recursively for the throwable's cause (e.g., value.getCause()).
         *
         * @param value the value to append
         * @return this statement
         */
        public Statement append(Throwable value) {
            append(value.getMessage()).append('\n');
            var stackTrace = value.getStackTrace();
            for (var i = 0; i < stackTrace.length; i++) {
                append(stackTrace[i].toString()).append('\n');
            }
            if (value.getCause() != null) {
                append(value.getCause());
            }
            return this;
        }

        /**
         * Commits the log statement to the log sink.
         */
        @SuppressWarnings("PMD.EmptyCatchBlock")
        public void commit() {
            try {
                buffer.putByte(offset, (byte) '\n');
                offset++;
                logSink.commit(offset);
            } catch (IOException e) {
                // do nothing
            }
        }
    }
}
