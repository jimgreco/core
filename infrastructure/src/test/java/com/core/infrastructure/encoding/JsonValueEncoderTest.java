package com.core.infrastructure.encoding;

import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

public class JsonValueEncoderTest {
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
                .string("item1").string("foo")
                .string("item2").bool(true)
                .string("item3").number(123)
                .closeMap();

        then(BufferUtils.toAsciiString(buffer, 18, encoder.stop())).isEqualTo(
                "{\"item1\":\"foo\",\"item2\":true,\"item3\":123}");
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
                .string("foo")
                .string(BufferUtils.fromAsciiString("bar"))
                .number(123)
                .closeList();

        then(BufferUtils.toAsciiString(buffer, 0, encoder.stop()))
                .isEqualTo("[\"foo\",\"bar\",123]");
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
}
