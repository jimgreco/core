package com.core.infrastructure.encoding;

import org.agrona.MutableDirectBuffer;

/**
 * An extension of {@code ObjectEncoder} that initializes and finalizes the buffer to write the object to.
 */
public interface MutableObjectEncoder extends ObjectEncoder {

    /**
     * Starts writing object to the specified {@code buffer}.
     *
     * @param buffer the buffer to write to
     * @param offset the first byte of the buffer to write to
     * @return this
     */
    ObjectEncoder start(MutableDirectBuffer buffer, int offset);

    /**
     * Stops writing the object and returns the number of bytes written to the buffer specified in the {@code start}
     * method.
     *
     * @return the number of bytes written
     */
    int stop();

    /**
     * Rewinds the position of the buffer being encoded to the offset specified in {@code start}.
     */
    void rewind();

    /**
     * Returns the number of bytes encoded.
     *
     * @return the number of bytes encoded
     */
    int getEncodedLength();

    /**
     * Sets a listener to be invoked when a root-level element is finished encoded.
     * A root-level element can be a map, list, string, boolean, number, or null.
     *
     * @param listener a listener to be invoked when a top-level element is starting to be encoded
     */
    void setFinishRootLevelListener(Runnable listener);

    /**
     * Sets a listener to be invoked when a element at the specified {@code level} is finished encoded.
     * An element can be a map, list, string, boolean, number, or null.
     *
     * @param level the level
     * @param listener a listener to be invoked when a top-level element is starting to be encoded
     */
    void setFinishLevelListener(int level, Runnable listener);
}
