package com.core.infrastructure;

import com.core.infrastructure.buffer.BufferNumberUtils;
import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

public class BufferNumberUtilsTest {

    private MutableDirectBuffer buffer;

    @BeforeEach
    void before_each() {
        buffer = BufferUtils.allocate(100);
    }

    @Test
    void write_negative_infinity() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, Double.NEGATIVE_INFINITY, 0, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("-Infinity");
    }

    @Test
    void write_positive_infinity() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, Double.POSITIVE_INFINITY, 0, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("Infinity");
    }

    @Test
    void write_NaN() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, Double.NaN, 0, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("NaN");
    }

    @Test
    void write_Long_MIN_VALUE_rounds_to_15_decimal_places() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, -9223372036854775808.0, 0, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("-9.223372036854776E18");
    }

    @Test
    void write_Long_MAX_VALUE_rounds_to_15_decimal_places() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, 9223372036854775807.0, 0, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("9.223372036854776E18");
    }

    @Test
    void write_double() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, 1.23, 0, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("1.23");
    }

    @Test
    void write_decimal_minDecimals_less() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, 1.23, 1, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("1.23");
    }

    @Test
    void write_decimal_minDecimals_equal() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, 1.23, 2, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("1.23");
    }

    @Test
    void write_decimal_minDecimals_greater() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, 1.23, 3, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("1.230");
    }

    @Test
    void write_decimal_with_minDecimals_equal_to_maxDecimals() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, 1.23, 12, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("1.230000000000");
    }

    @Test
    void write_decimal_truncated_with_maxDecimals() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, 1.2345, 0, 2);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("1.23");
    }

    @Test
    void write_integer_with_zero_minDecimals() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, 123, 0, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("123");
    }

    @Test
    void write_integer_with_minDecimals() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, 123, 2, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("123.00");
    }

    @Test
    void write_negative_double() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, -1.23, 0, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("-1.23");
    }

    @Test
    void write_negative_decimal_minDecimals_less() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, -1.23, 1, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("-1.23");
    }

    @Test
    void write_negative_decimal_minDecimals_equal() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, -1.23, 2, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("-1.23");
    }

    @Test
    void write_negative_decimal_minDecimals_greater() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, -1.23, 3, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("-1.230");
    }

    @Test
    void write_negative_decimal_with_minDecimals_equal_to_maxDecimals() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, -1.23, 12, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("-1.230000000000");
    }

    @Test
    void write_negative_decimal_truncated_with_maxDecimals() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, -1.2345, 0, 2);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("-1.23");
    }

    @Test
    void write_negative_integer_with_zero_minDecimals() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, -123, 0, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("-123");
    }

    @Test
    void write_negative_integer_with_minDecimals() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, -123, 2, 12);

        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("-123.00");
    }

    @Test
    void write_double_with_float_representation_having_more_9s_than_max_decimals() {
        var bytes = BufferNumberUtils.putAsAsciiDecimal(buffer, 0, 21173.999999999996);
        then(BufferUtils.toAsciiString(buffer, 0, bytes)).isEqualTo("21173");
    }

    @Nested
    class AsciiIntSizeTests {

        @Test
        void size_of_long_min_value_is_20() {
            then(BufferNumberUtils.asciiSize(Long.MIN_VALUE)).isEqualTo(20);
        }

        @Test
        void size_of_long_max_value_is_19() {
            then(BufferNumberUtils.asciiSize(Long.MAX_VALUE)).isEqualTo(19);
        }

        @Test
        void size_of_0_is_1() {
            then(BufferNumberUtils.asciiSize(0)).isEqualTo(1);
        }

        @Test
        void size_of_neg1_is_2() {
            then(BufferNumberUtils.asciiSize(-1)).isEqualTo(2);
        }

        @Test
        void size_of_1_is_1() {
            then(BufferNumberUtils.asciiSize(1)).isEqualTo(1);
        }

        @Test
        void size_of_10_is_2() {
            then(BufferNumberUtils.asciiSize(10)).isEqualTo(2);
        }

        @Test
        void size_of_neg10_is_3() {
            then(BufferNumberUtils.asciiSize(-10)).isEqualTo(3);
        }
    }

    @Nested
    class ParseAsLongTests {

        @Test
        void parseAsLong_positive_integer() {
            then(BufferNumberUtils.parseAsLong(BufferUtils.fromAsciiString("abc123def"), 3, 3))
                    .isEqualTo(123L);
        }

        @Test
        void parseAsLong_negative_integer() {
            then(BufferNumberUtils.parseAsLong(BufferUtils.fromAsciiString("abc-123def"), 3, 4))
                    .isEqualTo(-123L);
        }

        @Test
        void parseAsLong_positive_number_with_plus_character() {
            then(BufferNumberUtils.parseAsLong(BufferUtils.fromAsciiString("abc+123def"), 3, 4))
                    .isEqualTo(123L);
        }

        @Test
        void fail_to_parseAsLong_with_character() {
            then(BufferNumberUtils.parseAsLong(BufferUtils.fromAsciiString("abc-12a3def"), 3, 5))
                    .isEqualTo(-1L);
        }

        @Test
        void fail_to_parseAsLong_with_decimal_point() {
            then(BufferNumberUtils.parseAsLong(BufferUtils.fromAsciiString("abc-12.3def"), 3, 5))
                    .isEqualTo(-1L);
        }
    }

    @Nested
    class ParseAsDoubleTests {

        @Test
        void fastParseAsDouble_with_offset_and_length() {
            var buffer = BufferUtils.fromAsciiString("abc-123.456fs");

            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer, 3, 8);

            then(actual2).isEqualTo(-123.456);
        }

        @Test
        void fastParseAsDouble_with_defaultValue() {
            var buffer = BufferUtils.fromAsciiString("-123a456");

            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer, -2);

            then(actual2).isEqualTo(-2);
        }

        @Test
        void fastParseAsDouble_floating_point_integer() {
            var buffer = BufferUtils.fromAsciiString("-123");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-123.0);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void fastParseAsDouble_floating_point_integer_with_zero_decimal() {
            var buffer = BufferUtils.fromAsciiString("-123.00");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-123.0);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void fastParseAsDouble_floating_point_integer_with_leading_zeros() {
            var buffer = BufferUtils.fromAsciiString("-00123.00");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-123.0);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void fastParseAsDouble_floating_point_with_decimals() {
            var buffer = BufferUtils.fromAsciiString("-123.123");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-123.123);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void fastParseAsDouble_positive_floating_point_with_decimals() {
            var buffer = BufferUtils.fromAsciiString("123.123");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(123.123);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void fastParseAsDouble_plus_sign_floating_point_with_decimals() {
            var buffer = BufferUtils.fromAsciiString("+123.123");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(123.123);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void fastParseAsDouble_long_decimal() {
            var buffer = BufferUtils.fromAsciiString("-60251.26521462");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-60251.26521462);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void fastParseAsDouble_long_decimal_with_18_digits() {
            var buffer = BufferUtils.fromAsciiString("-9876543210.98765432");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-9876543210.9876543);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void fastParseAsDouble_long_decimal_with_18_digits_2() {
            var buffer = BufferUtils.fromAsciiString("-9.87654321098765432");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-9.87654321098765432);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void fastParseAsDouble_long_decimal_with_18_digits_3() {
            var buffer = BufferUtils.fromAsciiString("-98765432109876543.2");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-98765432109876543.2);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void fastParseAsDouble_long_decimal_with_18_digits_4() {
            var buffer = BufferUtils.fromAsciiString("0.014114091344554077");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(0.014114091344554077);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void fastParseAsDouble_long_decimal_with_19_digits_uses_double_parse() {
            var buffer = BufferUtils.fromAsciiString("-9876543210.987654321");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-9876543210.987654321);
            then(actual2).isEqualTo(-9876543210.987654321);
        }

        @Test
        void fastParseAsDouble_number_with_negative_exponent() {
            var buffer = BufferUtils.fromAsciiString("-1.23e-7");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-0.000000123);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void fastParseAsDouble_number_with_positive_exponent() {
            var buffer = BufferUtils.fromAsciiString("-1.23e7");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-12300000);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void fastParseAsDouble_with_long_number_with_positive_exponent() {
            var buffer = BufferUtils.fromAsciiString("-1.23456789012345678e17");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-123456789012345678.0);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void fastParseAsDouble_with_long_number_with_negative_exponent() {
            var buffer = BufferUtils.fromAsciiString("-12345678901234567.8e-17");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-0.123456789012345678);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void invalid_character_in_integer_returns_defaultValue() {
            var buffer = BufferUtils.fromAsciiString("-1a23e7");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-1);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void invalid_character_as_first_character_returns_defaultValue() {
            var buffer = BufferUtils.fromAsciiString("a1.23e7");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-1);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void invalid_character_in_exponent_returns_defaultValue() {
            var buffer = BufferUtils.fromAsciiString("-1.23ea7");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-1);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void invalid_character_as_last_character_in_integer_returns_defaultValue() {
            var buffer = BufferUtils.fromAsciiString("-1a");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-1);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void invalid_character_as_only_character_in_exponent_returns_defaultValue() {
            var buffer = BufferUtils.fromAsciiString("-1ea");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-1);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void invalid_character_as_last_character_in_exponent_returns_defaultValue() {
            var buffer = BufferUtils.fromAsciiString("-1e7a");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-1);
            then(actual2).isEqualTo(actual1);
        }

        @Test
        void invalid_character_as_middle_character_in_exponent_returns_defaultValue() {
            var buffer = BufferUtils.fromAsciiString("-1e7a5");

            var actual1 = parse(buffer);
            var actual2 = BufferNumberUtils.fastParseAsDouble(buffer);

            then(actual1).isEqualTo(-1);
            then(actual2).isEqualTo(actual1);
        }

        private double parse(DirectBuffer buffer) {
            try {
                return Double.parseDouble(BufferUtils.toAsciiString(buffer));
            } catch (Exception e) {
                return -1;
            }
        }
    }
}
