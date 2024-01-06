package com.core.platform.bus.mold;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;

/**
 * A highly optimized store for sequenced messages.
 *
 * <p>Messages are written to a buffer acquired through the {@link #acquire()} method.
 * One or messages can then be committed to the store with the {@link #commit(int[], int, int)} method which specifies
 * the length of each committed message.
 */
public interface MessageStore {

    /**
     * Opens the session with the specified name.
     *
     * @param sessionName the name of the session
     * @throws IOException if an I/O error occurs
     */
    void open(DirectBuffer sessionName) throws IOException;

    /**
     * Closes the session.
     * No more events can be written to the store until the store is reopened.
     *
     * @throws IOException if an I/O error occurs
     */
    void close() throws IOException;

    /**
     * Returns the buffer for writing events.
     * Events are not committed until {@link #commit(int[], int, int)} is invoked.
     *
     * @return the buffer
     */
    MutableDirectBuffer acquire();

    /**
     * Writes a sequence of events with a subsequence of given lengths.
     * Events are stored sequentially in in the event buffer that is acquired with {@code acquire()}.
     * The number of events is defined by the given {@code length} and each event has a length as specified in the
     * {@code lengths} array.
     *
     * @param lengths the lengths of the events in the event buffer
     * @param index the offset with the {@code lengths} array of the first event length
     * @param length the number events int he event buffer
     * @throws IOException if an I/O error occurs
     */
    void commit(int[] lengths, int index, int length) throws IOException;

    /**
     * Reads the event with the sequence number corresponding to the {@code seqNum} value into the {@code buffer}.
     *
     * @param buffer the buffer to write the event to
     * @param index the first byte of the buffer to write the event to
     * @param seqNum the sequence number of the event to write to the buffer
     * @return the number of bytes written
     * @throws IOException if an I/O error occurs
     */
    int read(MutableDirectBuffer buffer, int index, long seqNum) throws IOException;

    /**
     * Returns the number of events in the store.
     *
     * @return the number of events in the store
     */
    long getNumMessages();
}
