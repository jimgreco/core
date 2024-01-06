package com.core.infrastructure.encoding;

import com.core.infrastructure.buffer.BufferNumberUtils;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

class JavaValueEncoder implements ValueEncoder {

    JavaValueEncoder() {
    }

    @Override
    public int writeOpenMap(MutableDirectBuffer buffer, int index, int level) {
        buffer.putByte(index, (byte) '{');
        return 1;
    }

    @Override
    public int writeCloseMap(MutableDirectBuffer buffer, int index, int level) {
        buffer.putByte(index, (byte) '}');
        return 1;
    }

    @Override
    public int writeOpenList(MutableDirectBuffer buffer, int index, int level) {
        buffer.putByte(index, (byte) '[');
        return 1;
    }

    @Override
    public int writeCloseList(MutableDirectBuffer buffer, int index, int level) {
        buffer.putByte(index, (byte) ']');
        return 1;
    }

    @Override
    public int writePreFirstElement(MutableDirectBuffer buffer, int index) {
        return 0;
    }

    @Override
    public int writeNextElementSeparator(
            MutableDirectBuffer buffer, int index, int elementIndex) {
        return buffer.putStringWithoutLengthAscii(index, ", ");
    }

    @Override
    public int writeKeyValueSeparator(MutableDirectBuffer buffer, int index) {
        buffer.putByte(index, (byte) '=');
        return 1;
    }

    @Override
    public int writePreString(MutableDirectBuffer buffer, int index, boolean key) {
        if (!key) {
            buffer.putByte(index, (byte) '\'');
            return 1;
        }
        return 0;
    }

    @Override
    public int writePostString(MutableDirectBuffer buffer, int index, boolean key) {
        if (!key) {
            buffer.putByte(index, (byte) '\'');
            return 1;
        }
        return 0;
    }

    @Override
    public int writeString(
            MutableDirectBuffer buffer, int index, String value, boolean key) {
        return buffer.putStringWithoutLengthAscii(index, value);
    }

    @Override
    public int writeString(
            MutableDirectBuffer buffer, int index,
            DirectBuffer value, int offset, int length, boolean key) {
        buffer.putBytes(index, value, offset, length);
        return length;
    }

    @Override
    public int writeString(MutableDirectBuffer buffer, int index, char character, boolean key) {
        buffer.putByte(index, (byte) character);
        return 1;
    }

    @Override
    public int writeNumber(MutableDirectBuffer buffer, int index, long number) {
        return buffer.putLongAscii(index, number);
    }

    @Override
    public int writeNumber(MutableDirectBuffer buffer, int index, double number) {
        return BufferNumberUtils.putAsAsciiDecimal(buffer, index, number);
    }

    @Override
    public int writeNumber(MutableDirectBuffer buffer, int index, double number, int minDecimals) {
        return BufferNumberUtils.putAsAsciiDecimal(buffer, index, number, minDecimals);
    }

    @Override
    public int writeNumber(MutableDirectBuffer buffer, int index, double number, int minDecimals, int maxDecimals) {
        return BufferNumberUtils.putAsAsciiDecimal(buffer, index, number, minDecimals, maxDecimals);
    }

    @Override
    public int writeBoolean(MutableDirectBuffer buffer, int index, boolean bool) {
        return buffer.putStringWithoutLengthAscii(index, bool ? "true" : "false");
    }

    @Override
    public int writeNull(MutableDirectBuffer buffer, int index) {
        return buffer.putStringWithoutLengthAscii(index, "null");
    }

    @Override
    public boolean isMachineReadable() {
        return false;
    }
}
