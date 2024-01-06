package com.core.platform.shell;

import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.BDDAssertions.thenThrownBy;

public class BufferCasterTest {

    private BufferCaster caster;

    @BeforeEach
    void before_each() {
        caster = new BufferCaster();
    }

    @Test
    void cast_DirectBuffer_is_same_reference() {
        var expected = BufferUtils.fromAsciiString("hi");

        var actual = caster.cast(expected, DirectBuffer.class);

        then(actual).isSameAs(expected);
    }

    @Test
    void cast_String() {
        var actual = caster.cast(BufferUtils.fromAsciiString("hi"), String.class);

        then(actual).isEqualTo("hi");
    }

    @Test
    void cast_byte() {
        var actual = caster.cast(BufferUtils.fromAsciiString("-123"), byte.class);

        then(actual).isEqualTo((byte) -123);
    }

    @Test
    void cast_byte_object() {
        var actual = caster.cast(BufferUtils.fromAsciiString("-123"), Byte.class);

        then(actual).isEqualTo((byte) -123);
    }

    @Test
    void cast_short() {
        var actual = caster.cast(BufferUtils.fromAsciiString("-12345"), short.class);

        then(actual).isEqualTo((short) -12345);
    }

    @Test
    void cast_short_object() {
        var actual = caster.cast(BufferUtils.fromAsciiString("-12345"), Short.class);

        then(actual).isEqualTo((short) -12345);
    }

    @Test
    void cast_int() {
        var actual = caster.cast(BufferUtils.fromAsciiString("-123456789"), int.class);

        then(actual).isEqualTo(-123456789);
    }

    @Test
    void cast_int_object() {
        var actual = caster.cast(BufferUtils.fromAsciiString("-123456789"), Integer.class);

        then(actual).isEqualTo(-123456789);
    }

    @Test
    void cast_long() {
        var actual = caster.cast(BufferUtils.fromAsciiString("-1234567890123456"), long.class);

        then(actual).isEqualTo(-1234567890123456L);
    }

    @Test
    void cast_long_object() {
        var actual = caster.cast(BufferUtils.fromAsciiString("-1234567890123456"), Long.class);

        then(actual).isEqualTo(-1234567890123456L);
    }

    @Test
    void cast_float() {
        var actual = caster.cast(BufferUtils.fromAsciiString("-123456.789"), float.class);

        then(actual).isEqualTo(-123456.789f);
    }

    @Test
    void cast_float_object() {
        var actual = caster.cast(BufferUtils.fromAsciiString("-123456.789"), Float.class);

        then(actual).isEqualTo(-123456.789f);
    }

    @Test
    void cast_double() {
        var actual = caster.cast(BufferUtils.fromAsciiString("-123456.789"), double.class);

        then(actual).isEqualTo(-123456.789);
    }

    @Test
    void cast_double_object() {
        var actual = caster.cast(BufferUtils.fromAsciiString("-123456.789"), Double.class);

        then(actual).isEqualTo(-123456.789);
    }

    @Test
    void cast_boolean_true() {
        var actual = caster.cast(BufferUtils.fromAsciiString("true"), boolean.class);

        then(actual).isTrue();
    }

    @Test
    void cast_boolean_false() {
        var actual = caster.cast(BufferUtils.fromAsciiString("false"), boolean.class);

        then(actual).isFalse();
    }

    @Test
    void cast_boolean_invalid() {
        thenThrownBy(() -> caster.cast(BufferUtils.fromAsciiString("txue"), boolean.class))
                .isInstanceOf(ClassCastException.class);
    }

    @Test
    void cast_constructor() {
        var actual = caster.cast(BufferUtils.fromAsciiString("hello"), ObjectWithDirectBufferConstructor.class);

        then(actual.value).isEqualTo("hello");
    }

    @Test
    void cast_cached_constructor() {
        caster.cast(BufferUtils.fromAsciiString("hi"), ObjectWithDirectBufferConstructor.class);

        var actual = caster.cast(BufferUtils.fromAsciiString("hello"), ObjectWithDirectBufferConstructor.class);

        then(actual.value).isEqualTo("hello");
    }

    @Test
    void cast_valueOf() {
        var actual = caster.cast(BufferUtils.fromAsciiString("hello"), ObjectWithValueOfMethod.class);

        then(actual.value).isEqualTo("hello");
    }

    @Test
    void cast_cached_valueOf() {
        caster.cast(BufferUtils.fromAsciiString("hi"), ObjectWithValueOfMethod.class);

        var actual = caster.cast(BufferUtils.fromAsciiString("hello"), ObjectWithValueOfMethod.class);

        then(actual.value).isEqualTo("hello");
    }

    @Test
    void unknown_cast_throws_ClassCastException() {
        thenThrownBy(() -> caster.cast(BufferUtils.fromAsciiString("hello"), File.class))
                .isInstanceOf(ClassCastException.class);
    }

    @Test
    void cast_class() {
        var actual = caster.cast(BufferUtils.fromAsciiString("java.io.File"), Class.class);

        then(actual).isEqualTo(File.class);
    }

    @Test
    void cast_unknown_class_throws_ClassCastException() {
        thenThrownBy(() -> caster.cast(BufferUtils.fromAsciiString("java.File"), File.class))
                .isInstanceOf(ClassCastException.class);
    }

    private static class ObjectWithDirectBufferConstructor {

        final String value;

        public ObjectWithDirectBufferConstructor(DirectBuffer buffer) {
            value = BufferUtils.toAsciiString(buffer);
        }
    }

    private static class ObjectWithValueOfMethod {

        final String value;

        private ObjectWithValueOfMethod(String value) {
            this.value = value;
        }

        public static ObjectWithValueOfMethod valueOf(String value) {
            return new ObjectWithValueOfMethod(value);
        }
    }
}
