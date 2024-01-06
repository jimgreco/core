package com.core.infrastructure.encoding;

import org.agrona.DirectBuffer;

/**
 * An {@code ObjectEncoder} is a generic encoder for writing maps, lists, and values.
 *
 * <p>The interface is loosely based on the <a href="https://www.json.org/json-en.html">JSON specification</a>, but
 * could also be used to encode plain text, binary, YAML, XML, etc.
 *
 * <p>Values can be {@link #string(String) strings}, {@link #number(long) numbers}, {@link #bool(boolean) booleans},
 * {@link #object(Object) objects}, maps, or lists.<pre>
 *
 *     encoder.string("foo")
 *     encoder.number(123);
 * </pre>
 *
 * <p>A map is a sequence of key/value pairs.
 * Invoke {@link #openMap()} to start encoding a map, then encode key/value pairs, and then invoke {@link #closeMap()}
 * to stop encoding a map.
 * Keys must be encoded as a {@link #string(String) string}.<pre>
 *
 *     encoder.mapStart()
 *          .string("foo").string("bar")
 *          .string("soo").number(123)
 *          .mapEnd();
 * </pre>
 *
 * <p>A list is a sequence of values.<pre>
 *
 *      encoder.listStart()
 *           .string("foo")
 *           .string("bar")
 *           .number(123)
 *           .listEnd();
 * </pre>
 *
 * <p>Maps and lists can be nested.<pre>
 *
 *     encoder.startMap()
 *          .string("foo").startList()
 *                  .number(1)
 *                  .number(2)
 *                  .endList()
 *          .endMap();
 * </pre>
 *
 * <p>Objects can also be encoded.
 * If the object is an {@code Encodable}, then the {@link Encodable#encode(ObjectEncoder) encode} method will be invoked
 * with this encoder.
 * If the object is not an {@code Encodable}, then {@code toString()} will be invoked on the object and the returned
 * value will be written to the encoder as a string.
 */
public interface ObjectEncoder {

    /**
     * Returns true if the encodable message is machine readable.
     *
     * @return true if the encodable message is machine readable
     */
    boolean isMachineReadable();

    /**
     * Starts encoding a map.
     *
     * @return this
     * @throws IllegalStateException if the encoder was expecting a string map key or the maximum number of encoded
     *     levels has been reached
     */
    ObjectEncoder openMap();

    /**
     * Stops encoding a map.
     *
     * @return this
     * @throws IllegalStateException if the encoder was not encoding a map
     */
    ObjectEncoder closeMap();

    /**
     * Starts encoding a list.
     *
     * @return this
     * @throws IllegalStateException if the encoder was expecting a string map key or the maximum number of encoded
     *     levels has been reached
     */
    ObjectEncoder openList();

    /**
     * Stops encoding a list.
     *
     * @return this
     * @throws IllegalStateException if the encoder was not encoding a list
     */
    ObjectEncoder closeList();

    /**
     * Encodes the specified {@code value} as a string.
     * If the encoder is currently in a map then a string value can be either a key or a value.
     *
     * @param value the value to encode
     * @return this
     * @throws IllegalStateException if {@code value} is null or blank and the encoder was expecting a string map key
     */
    ObjectEncoder string(String value);

    /**
     * Encodes the specified {@code value} as a string.
     * If the encoder is currently in a map then a string value can be either a key or a value.
     *
     * @param value the value to encode
     * @return this
     * @throws IllegalStateException if {@code value} is null or has a capacity of 0 and the encoder was expecting a
     *     string map key
     */
    ObjectEncoder string(DirectBuffer value);

    /**
     * Encodes the specified {@code value} as a string.
     * If the encoder is currently in a map then a string value can be either a key or a value.
     *
     * @param value the value to encode
     * @param offset the first byte of the value to encode
     * @param length the number of bytes of the value to encode
     * @return this
     * @throws IllegalStateException if {@code value} is null or {@code length} is zero and the encoder was expecting a
     *     string map key
     */
    ObjectEncoder string(DirectBuffer value, int offset, int length);

    /**
     * Encodes the specified {@code value} as a string.
     *
     * @param value the value to encode
     * @return this
     * @throws IllegalStateException if the encoder was expecting a string map key
     */
    ObjectEncoder string(char value);

    /**
     * Encodes the specified {@code value} as a number.
     *
     * @param value the value to encode
     * @return this
     * @throws IllegalStateException if the encoder was expecting a string map key
     */
    ObjectEncoder number(long value);

    /**
     * Encodes the specified {@code value} as a number with the specified value encoder.
     *
     * @param value the value to encode
     * @param valueEncoder the value encoder to use
     * @return this
     * @throws IllegalStateException if the encoder was expecting a string map key
     */
    ObjectEncoder number(long value, NumberValueEncoder valueEncoder);

    /**
     * Encodes the specified {@code value} as a number.
     * The minimum number of decimals encoded is 0 and the maximum number of decimals encoded is {@code 8}.
     *
     * @param value the value to encode
     * @return this
     * @throws IllegalStateException if the encoder was expecting a string map key
     */
    ObjectEncoder number(double value);

    /**
     * Encodes the specified {@code value} as a number.
     * The minimum number of decimals encoded is the specified {@code minDecimals} value and maximum number of decimals
     * encoded is {@code 8}.
     *
     * @param value the value to encode
     * @param minDecimals the minimum number of decimals to write
     * @return this
     * @throws IllegalStateException if the encoder was expecting a string map key
     */
    ObjectEncoder number(double value, int minDecimals);

    /**
     * Encodes the specified {@code value} as a number.
     * The minimum number of decimals encoded is the specified {@code minDecimals} value and maximum number of decimals
     * encoded is the specified {@code maxDecimals}.
     *
     * @param value the value to encode
     * @param minDecimals the minimum number of decimals to write
     * @param maxDecimals the maximum number of decimals to write
     * @return this
     * @throws IllegalStateException if the encoder was expecting a string map key
     */
    ObjectEncoder number(double value, int minDecimals, int maxDecimals);

    /**
     * Encodes the specified {@code value} as a number string.
     *
     * @param value the value to encode
     * @return this
     * @throws IllegalStateException if the encoder was expecting a string map key
     */
    ObjectEncoder numberString(long value);

    /**
     * Encodes the specified {@code value} as a number string.
     * The minimum number of decimals encoded is 0 and the maximum number of decimals encoded is {@code 8}.
     *
     * @param value the value to encode
     * @return this
     * @throws IllegalStateException if the encoder was expecting a string map key
     */
    ObjectEncoder numberString(double value);

    /**
     * Encodes the specified {@code value} as a boolean.
     *
     * @param value the value to encode
     * @return this
     * @throws IllegalStateException if the encoder was expecting a string map key
     */
    ObjectEncoder bool(boolean value);

    /**
     * Encodes the specified {@code value} as an object.
     * If the object is an {@code Encodable}, then the {@link Encodable#encode(ObjectEncoder) encode} method will be
     * invoked with this encoder.
     * If the object is not an {@code Encodable}, then {@code toString()} will be invoked on the object and the
     * returned value will be written to the encoder as a string.
     *
     * @param value the value to encode
     * @return this
     * @throws IllegalStateException if the encoder was expecting a string map key
     */
    ObjectEncoder object(Object value);
}
