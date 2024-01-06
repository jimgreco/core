package com.core.infrastructure.encoding;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.time.TimestampDecimals;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class BufferObjectEncoderTest {

    private BufferObjectEncoder encoder;
    private MutableDirectBuffer buffer;

    @BeforeEach
    void before_each() {
        encoder = new BufferObjectEncoder(new JsonValueEncoder());
        buffer = BufferUtils.allocate(1000);
        encoder.start(buffer, 0);
    }

    @Test
    void encode_with_offset() {
        encoder.start(buffer, 18);

        encoder.openMap()
                .string("item1").string('f')
                .string("item2").bool(true)
                .string("item3").number(123)
                .closeMap();

        then(BufferUtils.toAsciiString(buffer, 18, encoder.stop())).isEqualTo(
                "{\"item1\":\"f\",\"item2\":true,\"item3\":123}");
    }

    @Test
    void encode_map() {
        encoder.openMap()
                .string("item1").bool(false)
                .string("item2").string(BufferUtils.fromAsciiString("bar"))
                .string("item3").number(123)
                .closeMap();

        then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                "{\"item1\":false,\"item2\":\"bar\",\"item3\":123}");
    }

    @Test
    void encode_list() {
        encoder.openList()
                .string('f')
                .string(BufferUtils.fromAsciiString("bar"))
                .number(123)
                .closeList();

        then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo("[\"f\",\"bar\",123]");
    }

    @Test
    void encode_nested_map() {
        encoder.openMap()
                .string("item1")
                        .openMap()
                        .string("item1a")
                        .string(BufferUtils.fromAsciiString("bar"))
                        .string("item2a")
                        .number(123)
                        .closeMap()
                .string("item2").string("soo")
                .closeMap();

        then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                "{\"item1\":{\"item1a\":\"bar\",\"item2a\":123},\"item2\":\"soo\"}");
    }

    @Test
    void encode_list_in_map() {
        encoder.openMap()
                .string("item1")
                .openList()
                .string("item1a")
                .string(BufferUtils.fromAsciiString("bar"))
                .string("item2a")
                .number(123)
                .closeList()
                .string("item2").string("soo")
                .closeMap();

        then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                "{\"item1\":[\"item1a\",\"bar\",\"item2a\",123],\"item2\":\"soo\"}");
    }

    @Test
    void encode_map_in_list() {
        encoder.openList()
                .string("item1")
                .openMap()
                .string("item1a")
                .string(BufferUtils.fromAsciiString("bar"))
                .string("item2a")
                .number(123)
                .closeMap()
                .string("item2").string("soo")
                .closeList();

        then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                "[\"item1\",{\"item1a\":\"bar\",\"item2a\":123},\"item2\",\"soo\"]");
    }

    @Test
    void encode_nested_lists() {
        encoder.openList()
                .string("item1")
                .openList()
                .string("item1a")
                .string(BufferUtils.fromAsciiString("bar"))
                .string("item2a")
                .number(123)
                .closeList()
                .string("item2").string("soo")
                .closeList();

        then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                "[\"item1\",[\"item1a\",\"bar\",\"item2a\",123],\"item2\",\"soo\"]");
    }

    @Test
    void encode_nested_lists_first_element() {
        encoder.openList()
                .openList()
                .string("item1a")
                .string(BufferUtils.fromAsciiString("bar"))
                .string("item2a")
                .number(123)
                .closeList()
                .string("item2").string("soo")
                .closeList();

        then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                "[[\"item1a\",\"bar\",\"item2a\",123],\"item2\",\"soo\"]");
    }

    @Test
    void encode_nested_maps_first_element() {
        encoder.openList()
                .openMap()
                .string("item1a")
                .string(BufferUtils.fromAsciiString("bar"))
                .string("item2a")
                .number(123)
                .closeMap()
                .string("item2").string("soo")
                .closeList();

        then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                "[{\"item1a\":\"bar\",\"item2a\":123},\"item2\",\"soo\"]");
    }

    @Test
    void encode_objects_in_list() {
        encoder.openList()
                .object(new NonencodableObject())
                .object(new EncodableObject())
                .closeList();

        then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                "[\"NonencodableObject{foo=1, bar='soo'}\",{\"foo\":\"bar\"}]");
    }

    @Test
    void encode_value_objects_in_list() {
        encoder.openList()
                .object(new EncodableValueObject())
                .object(new EncodableObject())
                .closeList();

        then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                "[\"foo\",{\"foo\":\"bar\"}]");
    }

    @Test
    void encode_objects_in_a_map() {
        encoder.openMap()
                .string("key1")
                .object(new MultiObject())
                .string("key2")
                .object(new EncodableObject())
                .closeMap();

        then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                "{\"key1\":{\"key3\":{\"foo\":\"bar\"},\"key4\":{\"foo\":\"bar\"}},\"key2\":{\"foo\":\"bar\"}}");
    }

    @Nested
    class ObjectEncodingTests {

        @Test
        void encode_map() {
            var map = new LinkedHashMap<String, Integer>();
            map.put("foo", 1);
            map.put("bar", 2);

            encoder.openMap()
                    .string("soo")
                    .object(map)
                    .closeMap();

            then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                    "{\"soo\":{\"foo\":1,\"bar\":2}}");
        }

        @Test
        void encode_list() {
            var list = new ArrayList<>();
            list.add("foo");
            list.add((short) 1);
            list.add("bar");
            list.add(false);

            encoder.openMap()
                    .string("soo")
                    .object(list)
                    .closeMap();

            then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                    "{\"soo\":[\"foo\",1,\"bar\",false]}");
        }

        @Test
        void encode_set() {
            var set = new LinkedHashSet<>();
            set.add("foo");
            set.add((byte) 1);
            set.add("bar");
            set.add('F');

            encoder.openMap()
                    .string("soo")
                    .object(set)
                    .closeMap();

            then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                    "{\"soo\":[\"foo\",1,\"bar\",\"F\"]}");
        }

        @Test
        void encode_boolean_array() {
            var array = new boolean[] { true, false, true };

            encoder.openMap()
                    .string("soo")
                    .object(array)
                    .closeMap();

            then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                    "{\"soo\":[true,false,true]}");
        }

        @Test
        void encode_byte_array() {
            var array = new byte[] { 1, 2, 3 };

            encoder.openMap()
                    .string("soo")
                    .object(array)
                    .closeMap();

            then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                    "{\"soo\":[1,2,3]}");
        }

        @Test
        void encode_short_array() {
            var array = new short[] { 1, 2, 3 };

            encoder.openMap()
                    .string("soo")
                    .object(array)
                    .closeMap();

            then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                    "{\"soo\":[1,2,3]}");
        }

        @Test
        void encode_char_array() {
            var array = new char[] { '1', '2', '3' };

            encoder.openMap()
                    .string("soo")
                    .object(array)
                    .closeMap();

            then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                    "{\"soo\":[\"1\",\"2\",\"3\"]}");
        }

        @Test
        void encode_int_array() {
            var array = new int[] { 1, 2, 3 };

            encoder.openMap()
                    .string("soo")
                    .object(array)
                    .closeMap();

            then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                    "{\"soo\":[1,2,3]}");
        }

        @Test
        void encode_float_array() {
            var array = new float[] { 1.23f, 2.34f, 3.45f };

            encoder.openMap()
                    .string("soo")
                    .object(array)
                    .closeMap();

            then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                    "{\"soo\":[1.23,2.34,3.45]}");
        }

        @Test
        void encode_long_array() {
            var array = new long[] { 1, 2, 3 };

            encoder.openMap()
                    .string("soo")
                    .object(array)
                    .closeMap();

            then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                    "{\"soo\":[1,2,3]}");
        }

        @Test
        void encode_double_array() {
            var array = new double[] { 1.23, 2.34, 3.45 };

            encoder.openMap()
                    .string("soo")
                    .object(array)
                    .closeMap();

            then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                    "{\"soo\":[1.23,2.34,3.45]}");
        }

        @Test
        void encode_object_array() {
            var array = new EncodableValueObject[] { new EncodableValueObject(), new EncodableValueObject() };

            encoder.openMap()
                    .string("soo")
                    .object(array)
                    .closeMap();

            then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                    "{\"soo\":[\"foo\",\"foo\"]}");
        }
    }

    @Nested
    class NumberEncodingTests {

        @Test
        void encode_datetime() {
            encoder.openMap()
                    .string("foo").number(1623859607_123456789L,
                            new DateTimeNumberValueEncoder(ZoneOffset.UTC, TimestampDecimals.MILLISECONDS))
                    .closeMap();

            then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                    "{\"foo\":\"2021-06-16T16:06:47.123Z\"}");
        }

        @Test
        void encode_time() {
            encoder.openMap()
                    .string("foo").number(1623859607_123456789L,
                            new TimeNumberValueEncoder(ZoneOffset.UTC, TimestampDecimals.MILLISECONDS))
                    .closeMap();

            then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                    "{\"foo\":\"16:06:47.123\"}");
        }

        @Test
        void encode_datestamp() {
            encoder.openMap()
                    .string("foo").number(18780, EncoderUtils.DATESTAMP_ENCODER)
                    .closeMap();

            then(BufferUtils.toAsciiString(buffer, 0, encoder.stop())).isEqualTo(
                    "{\"foo\":\"2021-06-02\"}");
        }
    }

    @Nested
    class StartStopEncodingTests {

        private Runnable finishEncoding;

        @BeforeEach
        void before_each() {
            finishEncoding = mock(Runnable.class);
            encoder.setFinishRootLevelListener(finishEncoding);
        }

        @Test
        void encoder_fires_does_not_fire_finish_when_map_starts() {
            encoder.openMap()
                    .string("key1")
                    .object(new MultiObject())
                    .string("key2");

            verify(finishEncoding, never()).run();
        }

        @Test
        void encoder_fires_finish_encoder_when_map_ends() {
            encoder.openMap()
                    .string("key1")
                    .object(new MultiObject())
                    .closeMap();

            verify(finishEncoding).run();
        }

        @Test
        void encoder_does_not_fire_finish_when_list_starts() {
            encoder.openList()
                    .string("key1")
                    .object(new MultiObject())
                    .string("key2");

            verify(finishEncoding, never()).run();
        }

        @Test
        void encoder_fires_finish_encoder_when_list_ends() {
            encoder.openList()
                    .string("key1")
                    .object(new MultiObject())
                    .closeList();

            verify(finishEncoding).run();
        }

        @Test
        void encoder_fires_finish_when_string_encoded() {
            encoder.string("hi");

            verify(finishEncoding).run();
        }

        @Test
        void encoder_fires_finish_when_bool_encoded() {
            encoder.bool(true);

            verify(finishEncoding).run();
        }

        @Test
        void encoder_fires_finish_when_number_encoded() {
            encoder.number(1000);

            verify(finishEncoding).run();
        }

        @Test
        void encoder_fires_finish_when_null_encoded() {
            encoder.object(null);

            verify(finishEncoding).run();
        }

        @Test
        void encoder_fires_finish_when_buffer_encoded() {
            encoder.object(BufferUtils.fromAsciiString("hi"));

            verify(finishEncoding).run();
        }
    }

    private static class NonencodableObject {

        private final int foo = 1;
        private final String bar = "soo";

        @Override
        public String toString() {
            return "NonencodableObject{"
                    + "foo=" + foo
                    + ", bar='" + bar + '\''
                    + '}';
        }
    }

    private static class MultiObject implements Encodable {

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.openMap()
                    .string("key3").object(new EncodableObject())
                    .string("key4").object(new EncodableObject())
                    .closeMap();
        }
    }

    private static class EncodableObject implements Encodable {

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.openMap()
                    .string("foo").string("bar")
                    .closeMap();
        }
    }

    private static class EncodableValueObject implements Encodable {

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.string("foo");
        }
    }
}
