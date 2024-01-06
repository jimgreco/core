package com.core.infrastructure;

import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.DirectBuffer;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

public class JsonTest {

    private static final DirectBuffer FOO = BufferUtils.fromAsciiString("foo");
    private static final DirectBuffer SOO = BufferUtils.fromAsciiString("soo");
    private static final DirectBuffer BAR = BufferUtils.fromAsciiString("bar");
    private static final DirectBuffer ME = BufferUtils.fromAsciiString("me");
    private static final DirectBuffer A = BufferUtils.fromAsciiString("a");
    private static final DirectBuffer B = BufferUtils.fromAsciiString("b");
    private static final DirectBuffer C = BufferUtils.fromAsciiString("c");
    private static final DirectBuffer D = BufferUtils.fromAsciiString("d");
    private static final DirectBuffer E = BufferUtils.fromAsciiString("e");
    private static final DirectBuffer F = BufferUtils.fromAsciiString("f");
    private static final DirectBuffer ABC = BufferUtils.fromAsciiString("abc");
    private Json parser;

    @BeforeEach
    void before_each() {
        parser = new Json();
    }

    @Nested
    class MapTests {

        @Test
        void parse_empty_map() {
            var actual = parse("{}");

            then(actual.getLengthParsed()).isEqualTo(2);
            then(actual.isComplete()).isTrue();
            then(actual.getRoot().asMapValue()).isEmpty();
        }

        @Test
        void parse_map_with_key_value_pairs() {
            var msg = "{\"foo\":\"bar\",\"soo\":\"me\"}";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var map = actual.getRoot().asMapValue();
            then(map.size()).isEqualTo(2);
            then(map.get(FOO).asStringValue()).isEqualTo(BAR);
            then(map.get(SOO).asStringValue()).isEqualTo(ME);
        }

        @Test
        void parse_map_with_key_value_pairs_and_whitespace() {
            var msg = " {  \"foo\" :   \"bar\" ,  \"soo\" :   \"me\" }    ";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length() - "    ".length());
            then(actual.isComplete()).isTrue();
            var map = actual.getRoot().asMapValue();
            then(map.size()).isEqualTo(2);
            then(map.get(FOO).asStringValue()).isEqualTo(BAR);
            then(map.get(SOO).asStringValue()).isEqualTo(ME);
        }

        @Test
        void parse_one_map_at_a_time() {
            var msg = "{\"foo\":\"bar\",\"soo\":\"me\"}{\"abc\":\"123\",\"456\":\"abc\"}";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo("{\"foo\":\"bar\",\"soo\":\"me\"}".length());
            then(actual.isComplete()).isTrue();
            var map = actual.getRoot().asMapValue();
            then(map.size()).isEqualTo(2);
            then(map.get(FOO).asStringValue()).isEqualTo(BAR);
            then(map.get(SOO).asStringValue()).isEqualTo(ME);
        }

        @Test
        void parse_one_map_at_a_time_with_offset() {
            var msg = "12345{\"foo\":\"bar\",\"soo\":\"me\"}{\"abc\":\"123\",\"456\":\"abc\"}";

            var buffer = BufferUtils.fromAsciiString(msg);
            var actual = parser.parse(buffer, 5, buffer.capacity() - 5);

            then(actual.getLengthParsed()).isEqualTo("{\"foo\":\"bar\",\"soo\":\"me\"}".length());
            then(actual.isComplete()).isTrue();
            var map = actual.getRoot().asMapValue();
            then(map.size()).isEqualTo(2);
            then(map.get(FOO).asStringValue()).isEqualTo(BAR);
            then(map.get(SOO).asStringValue()).isEqualTo(ME);
        }

        @Test
        void parse_mixed_map() {
            var msg = "{\"a\": 123, \"b\": \"abc\", \"c\": true, \"d\": null, \"e\": false, \"f\": 456.789}";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var map = actual.getRoot().asMapValue();
            then(map.get(A).asLongValue()).isEqualTo(123L);
            then(map.get(B).asStringValue()).isEqualTo(ABC);
            then(map.get(C).asBoolValue()).isEqualTo(true);
            then(map.get(D).asNullValue()).isEqualTo(null);
            then(map.get(E).asBoolValue()).isEqualTo(false);
            then(map.get(F).asDoubleValue()).isEqualTo(456.789);
        }

        @Test
        void parse_mixed_map_no_spaces() {
            var msg = "{\"a\":123,\"b\":\"abc\",\"c\":true,\"d\":null,\"e\":false,\"f\":456.789}";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var map = actual.getRoot().asMapValue();
            then(map.get(A).asLongValue()).isEqualTo(123L);
            then(map.get(B).asStringValue()).isEqualTo(ABC);
            then(map.get(C).asBoolValue()).isEqualTo(true);
            then(map.get(D).asNullValue()).isEqualTo(null);
            then(map.get(E).asBoolValue()).isEqualTo(false);
            then(map.get(F).asDoubleValue()).isEqualTo(456.789);
        }

        @Test
        void parse_nested_map() {
            var msg = "{\"1\":{\"a\":-123,\"b\":\"abc\",\"c\":true},\"2\":{\"d\":null,\"e\":false,\"f\":-456.789}}";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var map = actual.getRoot().asMapValue();
            then(map.size()).isEqualTo(2);
            var map1 = map.get(BufferUtils.fromAsciiString("1")).asMapValue();
            then(map1.get(A).asLongValue()).isEqualTo(-123L);
            then(map1.get(B).asStringValue()).isEqualTo(ABC);
            then(map1.get(C).asBoolValue()).isEqualTo(true);
            var map2 = map.get(BufferUtils.fromAsciiString("2")).asMapValue();
            then(map2.get(D).asNullValue()).isEqualTo(null);
            then(map2.get(E).asBoolValue()).isEqualTo(false);
            then(map2.get(F).asDoubleValue()).isEqualTo(-456.789);
        }

        @Test
        void close_map_with_list_returns_error() {
            var msg = "{\"foo\":\"bar\"]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("illegal character in map");
        }

        @Test
        void missing_key_returns_error() {
            var msg = "{\"foo\":\"bar\", ,\"a\":\"b\"}";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("illegal comma in map");
        }

        @Test
        void missing_first_key_returns_error() {
            var msg = "{,\"foo\":\"bar\",\"a\":\"b\"}";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("illegal comma in map");
        }


        @Test
        void double_colon_returns_error() {
            var msg = "{\"foo\"::\"bar\",\"a\":\"b\"}";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("illegal colon in map");
        }
    }

    @Nested
    class ListTests {

        @Test
        void parse_empty_list() {
            var actual = parse("[]");

            then(actual.getLengthParsed()).isEqualTo(2);
            then(actual.isComplete()).isTrue();
            then(actual.getRoot().asListValue()).isEmpty();
        }

        @Test
        void multiple_comma_returns_error() {
            var actual = parse("[123,,false]");

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("illegal comma in list");
        }

        @Test
        void multiple_first_comma_returns_error() {
            var actual = parse("[,123,false]");

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("illegal comma in list");
        }

        @Test
        void parse_mixed_list() {
            var msg = "[123, \"abc\", true, null, false, 456.789]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var list = actual.getRoot().asListValue();
            then(list.size()).isEqualTo(6);
            then(list.get(0).asLongValue()).isEqualTo(123L);
            then(list.get(1).asStringValue()).isEqualTo(ABC);
            then(list.get(2).asBoolValue()).isTrue();
            then(list.get(3).asNullValue()).isNull();
            then(list.get(4).asBoolValue()).isFalse();
            then(list.get(5).asDoubleValue()).isEqualTo(456.789);
        }

        @Test
        void parse_mixed_list_no_spaces() {
            var msg = "[123,\"abc\",true,null,false,-0.789]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var list = actual.getRoot().asListValue();
            then(list.size()).isEqualTo(6);
            then(list.get(0).asLongValue()).isEqualTo(123L);
            then(list.get(1).asStringValue()).isEqualTo(ABC);
            then(list.get(2).asBoolValue()).isTrue();
            then(list.get(3).asNullValue()).isNull();
            then(list.get(4).asBoolValue()).isFalse();
            then(list.get(5).asDoubleValue()).isEqualTo(-0.789);
        }

        @Test
        void parse_nested_list() {
            var msg = "[[123,\"abc\",true],[null,false,0.789]]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var list = actual.getRoot().asListValue();
            then(list.size()).isEqualTo(2);
            var list1 = list.get(0).asListValue();
            then(list1.get(0).asLongValue()).isEqualTo(123L);
            then(list1.get(1).asStringValue()).isEqualTo(ABC);
            then(list1.get(2).asBoolValue()).isTrue();
            var list2 = list.get(1).asListValue();
            then(list2.get(0).asNullValue()).isNull();
            then(list2.get(1).asBoolValue()).isFalse();
            then(list2.get(2).asDoubleValue()).isEqualTo(0.789);
        }

        @Test
        void illegal_character_in_list_returns_error() {
            var msg = "[123,:,\"abc\",true,null,false,-0.789]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("illegal character in list");
        }
    }

    @Nested
    class NumberParsingTests {

        @Test
        void illegal_character_in_number_returns_error() {
            var msg = "[123a456]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("illegal character in number");
        }

        @Test
        void illegal_decimal_in_number_returns_error() {
            var msg = "[123.456.789]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("two decimal points in number");
        }

        @Test
        void parse_too_long_fraction_returns_error() {
            var msg = "[0.1234567890123456789]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("cannot parse number");
        }

        @Test
        void parse_too_long_fraction_with_exponent_returns_error() {
            var msg = "[0.1234567890123456789e123]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("cannot parse number");
        }

        @Test
        void decimal_place_in_number_returns_error() {
            var msg = "[1.2345678e12.3]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("illegal character in number");
        }

        @Test
        void parse_negative_decimal_with_leading_zero() {
            var msg = "[-0.123]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var elements = actual.getRoot().asListValue();
            then(elements.size()).isEqualTo(1);
            then(elements.get(0).asDoubleValue()).isEqualTo(-0.123);
        }

        @Test
        void parse_decimal_with_trailing_zeros() {
            var msg = "[0.123000]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var elements = actual.getRoot().asListValue();
            then(elements.size()).isEqualTo(1);
            then(elements.get(0).asDoubleValue()).isEqualTo(0.123);
        }

        @Test
        void parse_decimal_with_zeros_in_first_decimal_part() {
            var msg = "[0.00123]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var elements = actual.getRoot().asListValue();
            then(elements.size()).isEqualTo(1);
            then(elements.get(0).asDoubleValue()).isEqualTo(0.00123);
        }

        @Test
        void parse_negative_decimal_without_leading_zero() {
            var msg = "[-.123]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var elements = actual.getRoot().asListValue();
            then(elements.size()).isEqualTo(1);
            then(elements.get(0).asDoubleValue()).isEqualTo(-0.123);
        }

        @Test
        void parse_decimal_with_leading_zero() {
            var msg = "[0.123]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var elements = actual.getRoot().asListValue();
            then(elements.size()).isEqualTo(1);
            then(elements.get(0).asDoubleValue()).isEqualTo(0.123);
        }

        @Test
        void parse_decimal_without_leading_zero() {
            var msg = "[.123]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var elements = actual.getRoot().asListValue();
            then(elements.size()).isEqualTo(1);
            then(elements.get(0).asDoubleValue()).isEqualTo(0.123);
        }

        @Test
        void number_for_key_returns_error() {
            var msg = "{123:\"abc\"}";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("non-string key");
        }

        @Test
        void number_not_in_map_or_list_number_value() {
            var msg = "123";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(3);
            then(actual.getRoot().asLongValue()).isEqualTo(123L);
        }

        @Test
        void parse_integer_exponent() {
            var msg = "[12e3]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var elements = actual.getRoot().asListValue();
            then(elements.size()).isEqualTo(1);
            then(elements.get(0).asDoubleValue()).isEqualTo(12000.0);
        }

        @Test
        void parse_integer_with_negative_exponent() {
            var msg = "[123456e-3]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var elements = actual.getRoot().asListValue();
            then(elements.size()).isEqualTo(1);
            then(elements.get(0).asDoubleValue()).isEqualTo(123.456);
        }

        @Test
        void parse_negative_integer_exponent() {
            var msg = "[-12e3]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var elements = actual.getRoot().asListValue();
            then(elements.size()).isEqualTo(1);
            then(elements.get(0).asDoubleValue()).isEqualTo(-12000.0);
        }

        @Test
        void parse_negative_integer_with_negative_exponent() {
            var msg = "[-123456e-3]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var elements = actual.getRoot().asListValue();
            then(elements.size()).isEqualTo(1);
            then(elements.get(0).asDoubleValue()).isEqualTo(-123.456);
        }

        @Test
        void parse_decimal_exponent() {
            var msg = "[1.23456e3]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var elements = actual.getRoot().asListValue();
            then(elements.size()).isEqualTo(1);
            then(elements.get(0).asDoubleValue()).isEqualTo(1234.56, Offset.offset(0.00000001));
        }

        @Test
        void parse_decimal_with_negative_exponent() {
            var msg = "[123.456e-3]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var elements = actual.getRoot().asListValue();
            then(elements.size()).isEqualTo(1);
            then(elements.get(0).asDoubleValue()).isEqualTo(0.123456, Offset.offset(0.00000001));
        }

        @Test
        void parse_negative_decimal_exponent() {
            var msg = "[-1.23456e3]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var elements = actual.getRoot().asListValue();
            then(elements.size()).isEqualTo(1);
            then(elements.get(0).asDoubleValue()).isEqualTo(-1234.56, Offset.offset(0.00000001));
        }

        @Test
        void parse_negative_decimal_with_negative_exponent() {
            var msg = "[-12345.6e-3]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var elements = actual.getRoot().asListValue();
            then(elements.size()).isEqualTo(1);
            then(elements.get(0).asDoubleValue()).isEqualTo(-12.3456, Offset.offset(0.00000001));
        }

        @Test
        void parse_long_fee_regression_20220317() {
            var msg = "{\"feeUsd\":0.014114091344554077}";
            var actual = parse(msg);
            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
        }
    }

    @Nested
    class StringTests {

        @Test
        void parse_backslash_characters() {
            var msg = "[\"\\\"\\/\\b\\n\\r\\t\\u\"]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var elements = actual.getRoot().asListValue();
            then(elements.size()).isEqualTo(1);
            then(BufferUtils.toAsciiString(elements.get(0).asStringValue())).isEqualTo("\\\"\\/\\b\\n\\r\\t\\u");
        }

        @Test
        void illegal_escape_character_returns_error() {
            var msg = "[\"\\y\"]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("invalid escaped character");
        }

        @Test
        void string_can_contain_brackets() {
            var msg = "[\"{][}-1230.\"]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            var elements = actual.getRoot().asListValue();
            then(elements.size()).isEqualTo(1);
            then(BufferUtils.toAsciiString(elements.get(0).asStringValue())).isEqualTo("{][}-1230.");
        }

        @Test
        void parse_string_not_in_map_or_list() {
            var msg = "\"abc\"";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            then(BufferUtils.toAsciiString(actual.getRoot().asStringValue())).isEqualTo("abc");
        }
    }

    @Nested
    class OtherValuesTests {

        @Test
        void parse_null_false_true() {
            var msg = "[null,false,true]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            var elements = actual.getRoot().asListValue();
            then(elements.size()).isEqualTo(3);
            then(elements.get(0).asNullValue()).isNull();
            then(elements.get(1).asBoolValue()).isFalse();
            then(elements.get(2).asBoolValue()).isTrue();
        }

        @Test
        void not_false_returns_error() {
            var msg = "[falxe]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("invalid value");
        }

        @Test
        void not_true_returns_error() {
            var msg = "[trxe]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("invalid value");
        }

        @Test
        void not_null_returns_error() {
            var msg = "[nulx]";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("invalid value");
        }

        @Test
        void false_for_key_returns_error() {
            var msg = "{false:\"abc\"}";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("non-string key");
        }

        @Test
        void true_for_key_returns_error() {
            var msg = "{true:\"abc\"}";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("non-string key");
        }

        @Test
        void null_for_key_returns_error() {
            var msg = "{null:\"abc\"}";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("non-string key");
        }

        @Test
        void parse_false() {
            var msg = "false";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            then(actual.getRoot().asBoolValue()).isFalse();
        }

        @Test
        void parse_true() {
            var msg = "true";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            then(actual.getRoot().asBoolValue()).isTrue();
        }

        @Test
        void parse_null() {
            var msg = "null";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(msg.length());
            then(actual.isComplete()).isTrue();
            then(actual.getRoot().asNullValue()).isNull();
        }

        @Test
        void parse_illegal_character_returns_error() {
            var msg = ",";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(-1);
            then(actual.getErrorReason()).isEqualTo("illegal character");
        }
    }

    @Nested
    class UnfinishedTests {

        @Test
        void unfinished_value_returns_zero() {
            var msg = "[123,\"abc\",tr";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(0);
        }

        @Test
        void unfinished_number_returns_zero() {
            var msg = "[123,\"abc\",123";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(0);
        }

        @Test
        void unfinished_array_element_returns_zero() {
            var msg = "[123,\"abc\",123,";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(0);
        }

        @Test
        void unfinished_map_element_returns_zero() {
            var msg = "{\"123\":\"abc\",\"456\":";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(0);
        }

        @Test
        void unfinished_string_returns_length_up_to_before_string() {
            var msg = "{\"foo\":\"bar\",\"soo";

            var actual = parse(msg);

            then(actual.getLengthParsed()).isEqualTo(0);
        }
    }

    private Json.ParseResult parse(String msg) {
        var buffer = BufferUtils.fromAsciiString(msg);
        return parser.parse(buffer, 0, buffer.capacity());
    }
}
