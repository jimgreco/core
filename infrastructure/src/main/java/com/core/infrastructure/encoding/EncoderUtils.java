package com.core.infrastructure.encoding;

import com.core.infrastructure.time.TimestampDecimals;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.time.ZoneId;

/**
 * Common encoder utils.
 */
public class EncoderUtils {

    /**
     * A encoder that will not write anything.
     * Additionally, no error will be thrown for malformed objects/lists.
     */
    public static final MutableObjectEncoder NULL_ENCODER = new NullObjectEncoder();

    private static final ZoneId ZONE_ID = ZoneId.systemDefault();

    /**
     * A {@code NumberValueEncoder} that encodes timestamps with the system time zone and seconds precision
     * (i.e., {@code yyyy-MM-ddTHH:mm:ss(Z|[+-]HH:mm:ss)}).
     */
    public static final NumberValueEncoder SECOND_ENCODER
            = new DateTimeNumberValueEncoder(ZONE_ID, TimestampDecimals.SECONDS);

    /**
     * A {@code NumberValueEncoder} that encodes timestamps with the system time zone and millisecond precision
     * (i.e., {@code yyyy-MM-ddTHH:mm:ss.SSS(Z|[+-]HH:mm:ss)}).
     */
    public static final NumberValueEncoder MILLISECOND_ENCODER
            = new DateTimeNumberValueEncoder(ZONE_ID, TimestampDecimals.MILLISECONDS);

    /**
     * A {@code NumberValueEncoder} that encodes timestamps with the system time zone and millisecond precision
     * (i.e., {@code yyyy-MM-ddTHH:mm:ss.SSSSSS(Z|[+-]HH:mm:ss)}).
     */
    public static final NumberValueEncoder MICROSECOND_ENCODER
            = new DateTimeNumberValueEncoder(ZONE_ID, TimestampDecimals.MICROSECONDS);

    /**
     * A {@code NumberValueEncoder} that encodes timestamps with the system time zone and millisecond precision
     * (i.e., {@code yyyy-MM-ddTHH:mm:ss.SSSSSSSSS(Z|[+-]HH:mm:ss)}).
     */
    public static final NumberValueEncoder NANOSECOND_ENCODER
            = new DateTimeNumberValueEncoder(ZONE_ID, TimestampDecimals.NANOSECONDS);

    /**
     * A {@code NumberValueEncoder} that encodes timestamps with the system time zone and millisecond precision
     * (i.e., {@code HH:mm:ss[.SSS]}).
     */
    public static final NumberValueEncoder TIME_MILLIS_ENCODER
            = new TimeNumberValueEncoder(ZONE_ID, TimestampDecimals.MILLISECONDS);

    /**
     * A {@code NumberValueEncoder} that encodes timestamps with the system time zone and seconds precision
     * (i.e., {@code HH:mm:ss}).
     */
    public static final NumberValueEncoder TIME_SECONDS_ENCODER
            = new TimeNumberValueEncoder(ZONE_ID, TimestampDecimals.SECONDS);

    /**
     * A {@code NumberValueEncoder} that encodes datestamps with dashes (i.e., {@code yyyy-MM-dd}).
     */
    public static final NumberValueEncoder DATESTAMP_ENCODER = new DatestampNumberValueEncoder();

    /**
     * Creates a {@code MutableObjectEncoder} implementation that encodes lists as a CSV file.
     *
     * <p>The text format prints a lists.
     *
     * <p>For example, consider encoding a list:<pre>
     *
     *     // encodes as
     *     encoder.listStart().encodeString("foo").encodeNumber(1).encodeString("bar").listEnd();
     *     encoder.listStart().encodeString("soo").encodeNumber(99).encodeString("bar").listEnd();
     *
     *     // prints as
     *     foo,1,bar
     *     soo,99,bar
     * </pre>
     *
     * <p>Map objects throw {@code UnsupportedOperationException}.
     *
     * @return a text object encoder
     */
    public static MutableObjectEncoder createCsvEncoder() {
        return new BufferObjectEncoder(new CsvValueEncoder());
    }

    /**
     * Creates a {@code MutableObjectEncoder} implementation that encodes lists as a query string
     *
     * <p>The text format prints a lists.
     *
     * <p>For example, consider encoding a list:<pre>
     *
     *     // encodes as
     *     encoder.listStart().encodeString("foo").encodeNumber(1).encodeString("bar").listEnd();
     *
     *     // prints as
     *     foo&1&bar
     * </pre>
     *
     * <p>Map objects throw {@code UnsupportedOperationException}.
     *
     * @return a text object encoder
     */
    public static MutableObjectEncoder createQueryEncoder() {
        return new BufferObjectEncoder(new QueryValueEncoder());
    }

    /**
     * Creates a {@code MutableObjectEncoder} implementation that encodes objects similar to how IntelliJ generates Java
     * {@code toString()}.
     * This encoding is useful for debugging only.
     *
     * <p>The text format prints an object key/value pair as {@code key=value} with a command and space between
     * key/value pairs.
     * Strings values (but not keys) are formatted with single quotes around the text (e.g., 'foo').
     * Lists are started/ended by brackets (i.e., {@code [} and {@code ]}).
     * Maps are started/ended by curly brackets (i.e., <code>{</code> and <code>}</code>).
     *
     * <p>For example, consider encoding a map with a nested list:<pre>
     *
     *     // encodes as
     *     encoder.mapStart()
     *          .encodeString("foo").encodeString("bar")
     *          .encodeString("soo").listStart()
     *                  .encodeNumber(1)
     *                  .encodeNumber(2)
     *                  .encodeNumber(3)
     *                  .listEnd()
     *          .mapEnd()
     *
     *     // prints as
     *     {foo='bar', soo=[1, 2, 3]}
     * </pre>
     *
     * @return a text object encoder
     */
    public static MutableObjectEncoder createJavaEncoder() {
        return new BufferObjectEncoder(new JavaValueEncoder());
    }

    /**
     * An {@code ObjectEncoder} implementation that encodes objects in JSON.
     *
     * <p>See the <a href="https://www.json.org/json-en.html">JSON specification</a> for more information the JSON format.
     *
     * <p>For example, consider encoding a map with a nested list:<pre>
     *
     *     // encodes as
     *     encoder.mapStart()
     *          .encodeString("foo").encodeString("bar")
     *          .encodeString("soo").listStart()
     *                  .encodeNumber(1)
     *                  .encodeNumber(2)
     *                  .encodeNumber(3)
     *                  .listEnd()
     *          .mapEnd()
     *
     *     // prints as
     *     {"foo":"bar","soo":[1,2,3]}
     * </pre>
     *
     * @return a JSON object encoder
     */
    public static MutableObjectEncoder createJsonEncoder() {
        return new BufferObjectEncoder(new JsonValueEncoder());
    }

    /**
     * An {@code ObjectEncoder} implementation that encodes objects in text format.
     *
     * <p>The text format prints an object key/value pair as {@code key: value} with a newline between key/value pairs.
     * Lists print each item on a separate line.
     * Each level of the object indents the text by 4 spaces.
     *
     * <p>For example, consider encoding a map with a nested list:<pre>
     *
     *     // encodes as
     *     encoder.mapStart()
     *          .encodeString("foo").encodeString("bar")
     *          .encodeString("soo").listStart()
     *                  .encodeNumber(1)
     *                  .encodeNumber(2)
     *                  .encodeNumber(3)
     *                  .listEnd()
     *          .mapEnd()
     *
     *     // prints as
     *     foo: bar
     *     soo:
     *         1
     *         2
     *         3
     * </pre>
     *
     * @return a text object encoder
     */
    public static MutableObjectEncoder createTextEncoder() {
        return new BufferObjectEncoder(new TextValueEncoder());
    }

    private static class NullObjectEncoder implements MutableObjectEncoder {

        private NullObjectEncoder() {
            super();
        }

        @Override
        public ObjectEncoder start(MutableDirectBuffer buffer, int offset) {
            return this;
        }

        @Override
        public int stop() {
            return 0;
        }

        @Override
        public void rewind() {
        }

        @Override
        public int getEncodedLength() {
            return 0;
        }

        @Override
        public void setFinishRootLevelListener(Runnable listener) {
        }

        @Override
        public void setFinishLevelListener(int level, Runnable listener) {

        }

        @Override
        public boolean isMachineReadable() {
            return false;
        }

        @Override
        public ObjectEncoder openMap() {
            return this;
        }

        @Override
        public ObjectEncoder closeMap() {
            return this;
        }

        @Override
        public ObjectEncoder openList() {
            return this;
        }

        @Override
        public ObjectEncoder closeList() {
            return this;
        }

        @Override
        public ObjectEncoder string(String value) {
            return this;
        }

        @Override
        public ObjectEncoder string(DirectBuffer value) {
            return this;
        }

        @Override
        public ObjectEncoder string(DirectBuffer value, int offset, int length) {
            return this;
        }

        @Override
        public ObjectEncoder string(char value) {
            return this;
        }

        @Override
        public ObjectEncoder number(long value) {
            return this;
        }

        @Override
        public ObjectEncoder number(long value, NumberValueEncoder valueEncoder) {
            return this;
        }

        @Override
        public ObjectEncoder number(double value) {
            return this;
        }

        @Override
        public ObjectEncoder number(double value, int minDecimals) {
            return this;
        }

        @Override
        public ObjectEncoder number(double value, int minDecimals, int maxDecimals) {
            return this;
        }

        @Override
        public ObjectEncoder numberString(long value) {
            return this;
        }

        @Override
        public ObjectEncoder numberString(double value) {
            return this;
        }

        @Override
        public ObjectEncoder bool(boolean value) {
            return this;
        }

        @Override
        public ObjectEncoder object(Object value) {
            return this;
        }
    }
}
