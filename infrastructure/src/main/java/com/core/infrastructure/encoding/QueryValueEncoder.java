package com.core.infrastructure.encoding;

import com.core.infrastructure.buffer.BufferNumberUtils;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

class QueryValueEncoder implements ValueEncoder {

    private boolean inKey;
    private boolean inList;

    QueryValueEncoder() {
        inKey = false;
        inList = false;
    }

    @Override
    public int writeOpenMap(MutableDirectBuffer buffer, int index, int level) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int writeCloseMap(MutableDirectBuffer buffer, int index, int level) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int writeOpenList(MutableDirectBuffer buffer, int index, int level) {
        if (level == 1) {
            inKey = true;
            inList = true;
            return 0;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public int writeCloseList(MutableDirectBuffer buffer, int index, int level) {
        if (level == 0) {
            inKey = false;
            inList = false;
            return 0;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public int writePreFirstElement(MutableDirectBuffer buffer, int index) {
        return 0;
    }

    @Override
    public int writeNextElementSeparator(
            MutableDirectBuffer buffer, int index, int elementIndex) {
        if (!inList) {
            throw new UnsupportedOperationException();
        }
        if (inKey) {
            buffer.putByte(index, (byte) '=');
            inKey = false;
        } else {
            buffer.putByte(index, (byte) '&');
            inKey = true;
        }
        return 1;
    }

    @Override
    public int writeKeyValueSeparator(MutableDirectBuffer buffer, int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int writePreString(MutableDirectBuffer buffer, int index, boolean key) {
        return 0;
    }

    @Override
    public int writePostString(MutableDirectBuffer buffer, int index, boolean key) {
        return 0;
    }

    @Override
    public int writeString(
            MutableDirectBuffer buffer, int index, String value, boolean key) {
        if (!inList) {
            throw new UnsupportedOperationException();
        }
        return buffer.putStringWithoutLengthAscii(index, value);
    }

    @Override
    public int writeString(
            MutableDirectBuffer buffer, int index,
            DirectBuffer value, int offset, int length, boolean key) {
        if (!inList) {
            throw new UnsupportedOperationException();
        }
        buffer.putBytes(index, value, offset, length);
        return length;
    }

    @Override
    public int writeString(MutableDirectBuffer buffer, int index, char character, boolean key) {
        if (!inList) {
            throw new UnsupportedOperationException();
        }
        buffer.putByte(index, (byte) character);
        return 1;
    }

    @Override
    public int writeNumber(MutableDirectBuffer buffer, int index, long number) {
        if (inKey || !inList) {
            throw new UnsupportedOperationException();
        }
        return buffer.putLongAscii(index, number);
    }

    @Override
    public int writeNumber(MutableDirectBuffer buffer, int index, double number) {
        if (inKey || !inList) {
            throw new UnsupportedOperationException();
        }
        return BufferNumberUtils.putAsAsciiDecimal(buffer, index, number);
    }

    @Override
    public int writeNumber(MutableDirectBuffer buffer, int index, double number, int minDecimals) {
        if (inKey || !inList) {
            throw new UnsupportedOperationException();
        }
        return BufferNumberUtils.putAsAsciiDecimal(buffer, index, number, minDecimals);
    }

    @Override
    public int writeNumber(MutableDirectBuffer buffer, int index, double number, int minDecimals, int maxDecimals) {
        if (inKey || !inList) {
            throw new UnsupportedOperationException();
        }
        return BufferNumberUtils.putAsAsciiDecimal(buffer, index, number, minDecimals, maxDecimals);
    }

    @Override
    public int writeBoolean(MutableDirectBuffer buffer, int index, boolean bool) {
        if (inKey || !inList) {
            throw new UnsupportedOperationException();
        }
        return buffer.putStringWithoutLengthAscii(index, bool ? "true" : "false");
    }

    @Override
    public int writeNull(MutableDirectBuffer buffer, int index) {
        if (inKey || !inList) {
            throw new UnsupportedOperationException();
        }
        return 0;
    }

    @Override
    public boolean isMachineReadable() {
        return true;
    }
}
