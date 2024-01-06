package com.core.infrastructure.log;

import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.DirectBuffer;

/**
 * The {@code LogLevel} class defines a set of standard logging levels that can be used to specify the urgency of the
 * logging output.
 *
 * <p>The levels in descending order are:
 * <ul>
 *     <li>ERROR
 *     <li>WARN
 *     <li>INFO
 *     <li>DEBUG
 * </ul>
 */
public enum LogLevel {

    /**
     * Designates fine-grained informational messages that are most useful to debug an application.
     */
    DEBUG,
    /**
     * Designates informational messages that highlight the progress of the application at coarse-grained level.
     */
    INFO,
    /**
     * Designates error events that might still allow the application to continue running.
     */
    WARN,
    /**
     * Designates very severe error events that will presumably lead the application to abort.
     */
    ERROR;

    private final DirectBuffer nameAsDirectBuffer;

    LogLevel() {
        this.nameAsDirectBuffer = BufferUtils.fromAsciiString(name());
    }

    /**
     * Returns the name of the enum as a buffer.
     *
     * @return the name of the enum as a buffer
     */
    public DirectBuffer getNameAsBuffer() {
        return nameAsDirectBuffer;
    }
}
