package com.core.infrastructure;

import com.core.infrastructure.buffer.BufferUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

public class BufferUtilsTest {

    @Test
    void emptyBuffer_has_zero_capacity() {
        var buffer = BufferUtils.emptyBuffer();

        then(buffer.capacity()).isEqualTo(0);
    }

    @Test
    void mutableEmptyBuffer_has_zero_capacity() {
        var buffer = BufferUtils.mutableEmptyBuffer();

        then(buffer.capacity()).isEqualTo(0);
    }

    @Test
    void allocate_sets_capacity() {
        var buffer = BufferUtils.allocate(33);

        then(buffer.capacity()).isEqualTo(33);
    }

    @Test
    void allocateDirect_sets_capacity() {
        var buffer = BufferUtils.allocateDirect(65);

        then(buffer.capacity()).isEqualTo(65);
    }

    @Test
    void allocateExpandable_sets_capacity() {
        var buffer = BufferUtils.allocateExpandable(111);

        then(buffer.capacity()).isEqualTo(111);
    }

    @Test
    void fromAsciiString_converts_toString() {
        var buffer = BufferUtils.fromAsciiString("foo");

        then(buffer.capacity()).isEqualTo(3);
        then(buffer.getStringWithoutLengthAscii(0, 3)).isEqualTo("foo");
    }

    @Test
    void copy_copies_byte_for_byte() {
        var expected = BufferUtils.fromAsciiString("foobar");

        var actual = BufferUtils.copy(expected);

        then(actual).isNotSameAs(expected);
        then(actual).isEqualTo(expected);
    }

    @Test
    void copy_with_index_copies_byte_for_byte() {
        var input = BufferUtils.fromAsciiString("foobar");

        var actual = BufferUtils.copy(input, 1, 4);

        then(actual).isNotSameAs(input);
        then(actual).isEqualTo(BufferUtils.fromAsciiString("ooba"));
    }

    @Test
    void putBoolAscii_puts_true_info_buffer() {
        var buffer = BufferUtils.allocate(10);

        var length = BufferUtils.putBoolAscii(buffer, 2, true);

        then(length).isEqualTo(4);
        then(buffer.getStringWithoutLengthAscii(2, 4)).isEqualTo("true");
    }

    @Test
    void putBoolAscii_puts_false_info_buffer() {
        var buffer = BufferUtils.allocate(10);

        var length = BufferUtils.putBoolAscii(buffer, 2, false);

        then(length).isEqualTo(5);
        then(buffer.getStringWithoutLengthAscii(2, 5)).isEqualTo("false");
    }

    @Test
    void toAsciiString_converts_buffer_to_string() {
        var buffer = BufferUtils.fromAsciiString("foobar");

        var actual = BufferUtils.toAsciiString(buffer);

        then(actual).isEqualTo("foobar");
    }

    @Test
    void compact_shifts_bytes() {
        var buffer = BufferUtils.allocate(10);
        buffer.putStringWithoutLengthAscii(0, "foobarsoom");

        BufferUtils.compact(buffer, 3, 6);

        then(buffer.getStringWithoutLengthAscii(0, 10)).isEqualTo("barsoosoom");
    }

    @Test
    void wrap_wraps_existing_buffer() {
        var buffer = BufferUtils.fromAsciiString("foobar");

        var actual = BufferUtils.wrap(buffer, 1, 4);

        then(actual).isEqualTo(BufferUtils.fromAsciiString("ooba"));
    }

    @Test
    void index_of_pattern_is_first_index() {
        var buffer = BufferUtils.fromAsciiString("foobarsoombarsoosoom");
        var pattern = BufferUtils.fromAsciiString("soom");
        var actual = BufferUtils.indexOf(buffer, pattern, 0, 20, 4);
        then(actual).isEqualTo(6);
    }

    @Test
    void index_of_offset_pattern_is_first_index() {
        var buffer = BufferUtils.fromAsciiString("foobarsoombarsoosoom");
        var pattern = BufferUtils.fromAsciiString("soom");
        var actual = BufferUtils.indexOf(buffer, pattern, 7, 20, 4);
        then(actual).isEqualTo(16);
    }

    @Test
    void index_of_missing_pattern_is_minus_one() {
        var buffer = BufferUtils.fromAsciiString("foobarsoombarsoosoom");
        var pattern = BufferUtils.fromAsciiString("moos");
        var actual = BufferUtils.indexOf(buffer, pattern, 0, 20, 4);
        then(actual).isEqualTo(-1);
    }

    @Test
    void index_of_character_returns_index() {
        var buffer = BufferUtils.fromAsciiString("foobarsoombarsoosoom");
        var actual = BufferUtils.indexOf(buffer, 0, 19, 'a');
        then(actual).isEqualTo(4);
    }

    @Test
    void index_of_character_with_offset_returns_index() {
        var buffer = BufferUtils.fromAsciiString("foobarsoombarsoosoom");
        var actual = BufferUtils.indexOf(buffer, 5, 14, 'a');
        then(actual).isEqualTo(11);
    }

    @Test
    void index_of_character_doesnt_exist_returns_neg1() {
        var buffer = BufferUtils.fromAsciiString("foobarsoombarsoosoom");
        var actual = BufferUtils.indexOf(buffer, 5, 14, 'z');
        then(actual).isEqualTo(-1);
    }

    @Nested
    class EqualsTests {

        @Test
        void asciiEquals_null_buffer_returns_false() {
            then(BufferUtils.asciiEquals("foo", null)).isFalse();
            then(BufferUtils.asciiEquals(null, "foo")).isFalse();
            then(BufferUtils.asciiNotEquals("foo", null)).isTrue();
            then(BufferUtils.asciiNotEquals(null, "foo")).isTrue();
        }

        @Test
        void asciiEquals_null_string_returns_false() {
            var buf = BufferUtils.fromAsciiString("foo");

            then(BufferUtils.asciiEquals(buf, null)).isFalse();
            then(BufferUtils.asciiEquals(null, buf)).isFalse();
            then(BufferUtils.asciiNotEquals(buf, null)).isTrue();
            then(BufferUtils.asciiNotEquals(null, buf)).isTrue();
        }

        @Test
        void asciiEquals_both_null_returns_true() {
            then(BufferUtils.asciiEquals(null, (String) null)).isTrue();
            then(BufferUtils.asciiEquals((String) null, null)).isTrue();
            then(BufferUtils.asciiNotEquals(null, (String) null)).isFalse();
            then(BufferUtils.asciiNotEquals((String) null, null)).isFalse();
        }

        @Test
        void asciiEquals_different_length_returns_false() {
            var buf = BufferUtils.fromAsciiString("foo2");

            then(BufferUtils.asciiEquals("foo", buf)).isFalse();
            then(BufferUtils.asciiEquals(buf, "foo")).isFalse();
            then(BufferUtils.asciiNotEquals("foo", buf)).isTrue();
            then(BufferUtils.asciiNotEquals(buf, "foo")).isTrue();
        }

        @Test
        void asciiEquals_different_contents_returns_false() {
            var buf = BufferUtils.fromAsciiString("fao");

            then(BufferUtils.asciiEquals("foo", buf)).isFalse();
            then(BufferUtils.asciiEquals(buf, "foo")).isFalse();
            then(BufferUtils.asciiNotEquals("foo", buf)).isTrue();
            then(BufferUtils.asciiNotEquals(buf, "foo")).isTrue();
        }

        @Test
        void asciiEquals_same_contents_returns_true() {
            var buf = BufferUtils.fromAsciiString("foo");

            then(BufferUtils.asciiEquals("foo", buf)).isTrue();
            then(BufferUtils.asciiEquals(buf, "foo")).isTrue();
            then(BufferUtils.asciiNotEquals("foo", buf)).isFalse();
            then(BufferUtils.asciiNotEquals(buf, "foo")).isFalse();
        }
    }
}