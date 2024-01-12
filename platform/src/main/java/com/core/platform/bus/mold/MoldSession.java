package com.core.platform.bus.mold;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.time.Time;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import org.agrona.DirectBuffer;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A mold session is a unique sequence of events.
 * The mold session is identified by a unique name, the session name.
 * Events in the stream have an implicit sequence number, starting from 1 and increasing monotonically by one.
 *
 * <p>The name of the session is 10-characters with the following format: {@code yyyyMMddXX}.
 * Where {@code yyyyMMdd} is the date the session was created. and {@code XX} is the two-letter user-defined suffix for
 * the session.
 */
class MoldSession implements Encodable {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final Time time;
    private final Activator activator;
    private final List<Runnable> openSessionListeners;
    private DirectBuffer sessionName;
    private String sessionNameString;
    private long nextSessionSeqNum;

    /**
     * Creates the {@code MoldSession} with the specified {@code time} object that is used to query the current date
     * for the session when the session is created.
     *
     * @param name the name of the session
     * @param time an object that returns the current time
     * @param activatorFactory the activator factory
     */
    MoldSession(String name, Time time, ActivatorFactory activatorFactory) {
        this.time = Objects.requireNonNull(time);
        nextSessionSeqNum = 1;
        activator = activatorFactory.createActivator(name, this);
        openSessionListeners = new CoreList<>();
    }

    /**
     * Creates the session with the specified session suffix.
     * The session is 10-characters with the following format: {@code yyyyMMddXX}.
     * Where {@code yyyyMMdd} is the date the session was created (from the UTC timezone) and
     * {@code XX} is the {@code sessionSuffix} parameter.
     *
     * @param sessionSuffix the suffix of the session
     * @throws IllegalStateException if the session has already been created or set
     * @throws IllegalArgumentException if {@code sessionSuffix} is not 2 bytes
     */
    @Command
    public void create(DirectBuffer sessionSuffix) {
        Objects.requireNonNull(sessionSuffix);
        if (sessionSuffix.capacity() != 2) {
            throw new IllegalArgumentException("sessionSuffix must be length of 2");
        }

        var instant = Instant.ofEpochSecond(time.nanos() / TimeUnit.SECONDS.toNanos(1));
        var localDate = LocalDate.ofInstant(instant, ZoneOffset.UTC);
        setSessionName(BufferUtils.fromAsciiString(
                DATE_FORMATTER.format(localDate) + BufferUtils.toAsciiString(sessionSuffix)));
    }

    void addOpenSessionListener(Runnable listener) {
        if (getSessionName() == null) {
            openSessionListeners.add(listener);
        } else {
            listener.run();
        }
    }

    /**
     * Sets the name of the session.
     *
     * @param sessionName the name of the session
     */
    void setSessionName(DirectBuffer sessionName) {
        checkSession();
        this.sessionName = Objects.requireNonNull(sessionName);
        sessionNameString = BufferUtils.toAsciiString(sessionName);
        activator.ready();

        for (var openSessionListener : openSessionListeners) {
            openSessionListener.run();
        }
    }

    /**
     * Returns the name of the session.
     *
     * @return the name of the session
     */
    @Command(readOnly = true)
    public DirectBuffer getSessionName() {
        return sessionName;
    }

    /**
     * Returns the name of the session.
     *
     * @return the name of the session
     */
    public String getSessionNameAsString() {
        return sessionNameString;
    }

    /**
     * Returns the sequence number of the next event.
     *
     * @return the sequence number of the next event
     */
    @Command(readOnly = true)
    public long getNextSequenceNumber() {
        return nextSessionSeqNum;
    }

    /**
     * Sets the sequence number of the next event.
     *
     * @param nextSessionSeqNum the sequence number of the next event
     */
    void setNextSequenceNumber(long nextSessionSeqNum) {
        this.nextSessionSeqNum = nextSessionSeqNum;
    }

    private void checkSession() {
        if (sessionName != null) {
            throw new IllegalStateException(
                    "session is already defined: " + BufferUtils.toAsciiString(sessionName));
        }
    }

    @Override
    public String toString() {
        return toEncodedString();
    }

    @Command(path = "status", readOnly = true)
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("sessionName").string(sessionName)
                .string("nextSessionSeqNum").number(nextSessionSeqNum)
                .closeMap();
    }
}
