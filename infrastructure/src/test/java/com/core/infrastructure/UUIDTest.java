package com.core.infrastructure;

import com.core.infrastructure.buffer.BufferUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

public class UUIDTest {

    @Test
    void new_uuid_value_is_zero() {
        var uuid = new UUID();

        then(uuid.getLeastSignificantBits()).isEqualTo(0);
        then(uuid.getMostSignificantBits()).isEqualTo(0);
        then(uuid.isSet()).isFalse();
    }

    @Test
    void get_set_uuid_value() {
        var uuid = new UUID();

        uuid.setLeastSignificantBits(1234567890123456789L);
        uuid.setMostSignificantBits(987654321098765432L);

        then(uuid.getLeastSignificantBits()).isEqualTo(1234567890123456789L);
        then(uuid.getMostSignificantBits()).isEqualTo(987654321098765432L);
        then(uuid.isSet()).isTrue();
    }

    @Test
    void reset_sets_uuid_to_zero() {
        var uuid = new UUID();
        uuid.setLeastSignificantBits(1234567890123456789L);
        uuid.setMostSignificantBits(987654321098765432L);

        uuid.reset();

        then(uuid.getLeastSignificantBits()).isEqualTo(0);
        then(uuid.getMostSignificantBits()).isEqualTo(0);
        then(uuid.isSet()).isFalse();
    }

    @Test
    void copy_uuid() {
        var uuid = new UUID();
        uuid.setLeastSignificantBits(1234567890123456789L);
        uuid.setMostSignificantBits(987654321098765432L);
        var actual = new UUID();

        actual.fromUUID(uuid);

        then(actual.getLeastSignificantBits()).isEqualTo(1234567890123456789L);
        then(actual.getMostSignificantBits()).isEqualTo(987654321098765432L);
    }

    @Test
    void convert_to_string() {
        var uuid = new UUID();

        uuid.setLeastSignificantBits(-5091206193122058597L);
        uuid.setMostSignificantBits(-3094314323176569334L);

        then(uuid.toString()).isEqualTo("d50ec984-77a8-460a-b958-66f114b0de9b");
    }

    @Test
    void toBuffer_with_dashes() {
        var uuid = new UUID();
        uuid.setLeastSignificantBits(-5091206193122058597L);
        uuid.setMostSignificantBits(-3094314323176569334L);
        var buffer = BufferUtils.allocate(5 + UUID.UUID_LENGTH_WITH_DASHES);

        var length = uuid.toBuffer(buffer, 5, true);

        then(length).isEqualTo(36);
        then(BufferUtils.toAsciiString(buffer, 5, length)).isEqualTo("d50ec984-77a8-460a-b958-66f114b0de9b");
    }

    @Test
    void toBuffer_without_dashes() {
        var uuid = new UUID();
        uuid.setLeastSignificantBits(-5091206193122058597L);
        uuid.setMostSignificantBits(-3094314323176569334L);
        var buffer = BufferUtils.allocate(5 + UUID.UUID_LENGTH_WITHOUT_DASHES);

        var length = uuid.toBuffer(buffer, 5, false);

        then(length).isEqualTo(32);
        then(BufferUtils.toAsciiString(buffer, 5, length)).isEqualTo("d50ec98477a8460ab95866f114b0de9b");
    }

    @Test
    void fromBuffer_to_uuid_with_dashes() {
        var uuid = new UUID();

        uuid.fromBuffer(BufferUtils.fromAsciiString("XXXd50ec984-77a8-460a-b958-66f114b0de9bYYYY"), 3, 36);

        then(uuid.getMostSignificantBits()).isEqualTo(-3094314323176569334L);
        then(uuid.getLeastSignificantBits()).isEqualTo(-5091206193122058597L);
    }

    @Test
    void fromBuffer_to_uuid_without_dashes() {
        var uuid = new UUID();

        uuid.fromBuffer(BufferUtils.fromAsciiString("XXXd50ec98477a8460ab95866f114b0de9bYYYY"), 3, 32);

        then(uuid.getMostSignificantBits()).isEqualTo(-3094314323176569334L);
        then(uuid.getLeastSignificantBits()).isEqualTo(-5091206193122058597L);
    }

    @Test
    void fromBuffer_with_invalid_character_is_reset() {
        var uuid = new UUID();

        uuid.fromBuffer(BufferUtils.fromAsciiString("XXXd50ec984-77g8-460a-b958-66f114b0de9bYYYY"), 3, 36);

        then(uuid.isSet()).isFalse();
    }

    @Test
    void fromBuffer_with_invalid_character2_is_reset() {
        var uuid = new UUID();

        uuid.fromBuffer(BufferUtils.fromAsciiString("XXXd50ec98477g8460ab95866f114b0de9bYYYY"), 3, 32);

        then(uuid.isSet()).isFalse();
    }

    @Test
    void fromBuffer_with_invalid_length_is_reset() {
        var uuid = new UUID();

        uuid.fromBuffer(BufferUtils.fromAsciiString("XXXd50ec984-77f8-460a-b958-66f114b0de9bYYYY"), 3, 35);

        then(uuid.isSet()).isFalse();
    }

    @Test
    void fromBuffer_with_invalid_length2_is_reset() {
        var uuid = new UUID();

        uuid.fromBuffer(BufferUtils.fromAsciiString("XXXd50ec98477a8460ab95866f114b0de9bYYYY"), 3, 31);

        then(uuid.isSet()).isFalse();
    }

    @Test
    void equals_with_same_UUID_values_is_true() {
        var uuid1 = new UUID();
        uuid1.setMostSignificantBits(123);
        uuid1.setLeastSignificantBits(456);
        var uuid2 = new UUID();
        uuid2.setMostSignificantBits(123);
        uuid2.setLeastSignificantBits(456);

        then(uuid1.equals(uuid2)).isTrue();
    }

    @Test
    void equals_with_different_UUID_values_is_false() {
        var uuid1 = new UUID();
        uuid1.setMostSignificantBits(123);
        uuid1.setLeastSignificantBits(456);
        var uuid2 = new UUID();
        uuid2.setMostSignificantBits(123);
        uuid2.setLeastSignificantBits(457);

        then(uuid1.equals(uuid2)).isFalse();
    }

    @Test
    void equals_with_different_UUID_values2_is_false() {
        var uuid1 = new UUID();
        uuid1.setMostSignificantBits(123);
        uuid1.setLeastSignificantBits(456);
        var uuid2 = new UUID();
        uuid2.setMostSignificantBits(124);
        uuid2.setLeastSignificantBits(456);

        then(uuid1.equals(uuid2)).isFalse();
    }

    @Test
    void null_equals_is_false() {
        then(new UUID().equals(null)).isFalse();
    }

    @Test
    void non_uuid_class_equals_is_false() {
        then(new UUID().equals(new Object())).isFalse();
    }
}
