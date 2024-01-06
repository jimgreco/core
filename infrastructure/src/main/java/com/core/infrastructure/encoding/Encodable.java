package com.core.infrastructure.encoding;

import com.core.infrastructure.Allocation;
import com.core.infrastructure.buffer.BufferUtils;

/**
 * An object that is encodable.
 */
public interface Encodable {

    /**
     * Invoked when the object is to be encoded with the specified {@code encoder}.
     * It is up to the object to specify how the object is encoded, but typically objects should be encoded as maps with
     * key/value pairs.<pre>
     *
     * public void encode(ObjectEncoder encoder) {
     *     encoder.mapStart()
     *          .string("foo").string("bar")
     *          .mapEnd();
     * }
     * </pre>
     *
     * @param encoder the encoder
     */
    void encode(ObjectEncoder encoder);

    /**
     * Converts the {@code Encodable} into a string.
     * Most objects will return this method in their {@code toString()} method to prevent redundancy with the
     * {@code encode} method.
     *
     * @return the encoded string
     * @implSpec the default implementation will create a text encoder, encode this object with the encoder, and then
     *     convert the encoded buffer into a string
     */
    @Allocation
    default String toEncodedString() {
        var buffer = BufferUtils.allocateExpandable(1024);
        var encoder = EncoderUtils.createJavaEncoder();
        encoder.start(buffer, 0).object(this);
        var bytesEncoded = encoder.stop();
        var string = BufferUtils.toAsciiString(buffer, 0, bytesEncoded);
        if (bytesEncoded > 0 && buffer.getByte(0) == '{') {
            string = getClass().getSimpleName() + string;
        }
        return string;
    }
}
