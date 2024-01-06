package com.core.platform.bus.playback;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.messages.Decoder;
import com.core.infrastructure.messages.Encoder;
import org.agrona.MutableDirectBuffer;

import java.time.LocalDate;

/**
 * A message translator will transform the raw bytes of a message from one schema to another.
 */
public interface MessageTranslator {

    /**
     * The buffer size of the encoded message to allocate.
     * This is larger than a typical max message size (1450 bytes) because the translators can add additional fields to
     * the message.
     */
    int MESSAGE_BUFFER_SIZE = 2000;

    /**
     * Returns the last date (exclusive) to apply the translator to all messages.
     *
     * @return the last date (exclusive) to apply the translator to all messages
     */
    LocalDate getLastDate();

    /**
     * Translates the specified message.
     *
     * @param buffer the message buffer
     * @return the translated message buffer
     */
    MutableDirectBuffer translate(MutableDirectBuffer buffer);

    /**
     * Creates a new buffer and uses the specified {@code encoder} to set the header fields.
     * The {@code ApplicationId}, {@code ApplicationSequenceNumber}, and {@code Timestamp} fields are copied from the
     * specified {@code decoder}.
     *
     * @param decoder the decoder
     * @param encoder the encoder
     * @return the new buffer
     */
    default MutableDirectBuffer copy(Decoder decoder, Encoder encoder) {
        // account for increases in the size of the encoded data beyond a packet
        // for example, adding a field to a full market data message
        var buffer = BufferUtils.allocate(MESSAGE_BUFFER_SIZE);
        encoder.wrap(buffer);
        encoder.setApplicationId(decoder.getApplicationId());
        encoder.setApplicationSequenceNumber(decoder.getApplicationSequenceNumber());
        encoder.setTimestamp(decoder.getTimestamp());
        return buffer;
    }

    /**
     * Returns a new buffer that wraps the encoded buffer from the specified {@code encoder}.
     *
     * @param encoder the encoder
     * @return the wrapped buffer
     */
    default MutableDirectBuffer wrap(Encoder encoder) {
        var wrapper = BufferUtils.mutableEmptyBuffer();
        wrapper.wrap(encoder.buffer(), encoder.offset(), encoder.length());
        return wrapper;
    }
}
