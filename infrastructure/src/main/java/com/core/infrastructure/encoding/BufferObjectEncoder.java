package com.core.infrastructure.encoding;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

class BufferObjectEncoder implements MutableObjectEncoder {

    private static final int MAX_LEVELS = 10;
    private static final int MAX_FLOAT_DECIMALS = 6;

    private final ValueEncoder encoder;
    private final boolean[] inKey;
    private final char[] type;
    private final int[] elementIndex;
    private Runnable[] finishLevelListener;

    private MutableDirectBuffer buffer;
    private int offset;
    private int index;
    private int level;


    BufferObjectEncoder(ValueEncoder encoder) {
        this.encoder = encoder;
        type = new char[MAX_LEVELS];
        inKey = new boolean[MAX_LEVELS];
        elementIndex = new int[MAX_LEVELS];
        finishLevelListener = new Runnable[MAX_LEVELS];
    }

    @Override
    public boolean isMachineReadable() {
        return encoder.isMachineReadable();
    }

    @Override
    public ObjectEncoder start(MutableDirectBuffer buffer, int offset) {
        this.buffer = buffer;
        this.offset = offset;
        level = 0;
        rewind();
        return this;
    }

    @Override
    public void rewind() {
        index = offset;
        inKey[level] = false;
        type[level] = 0;
        elementIndex[level] = 0;
    }

    @Override
    public int stop() {
        if (level != 0) {
            throw new IllegalStateException("level != 0: level=" + level);
        }
        return index - offset;
    }

    @Override
    public void setFinishRootLevelListener(Runnable listener) {
        finishLevelListener[0] = listener;
    }

    @Override
    public void setFinishLevelListener(int level, Runnable listener) {
        finishLevelListener[level] = listener;
    }

    @Override
    public int getEncodedLength() {
        return index - offset;
    }

    @Override
    public ObjectEncoder openMap() {
        if (level + 1 == MAX_LEVELS) {
            throw new IllegalStateException("level >= 10");
        }

        startValue(false);
        level++;
        type[level] = '{';
        inKey[level] = true;
        elementIndex[level] = 0;
        index += encoder.writeOpenMap(buffer, index, level);
        return this;
    }

    @Override
    public ObjectEncoder closeMap() {
        if (!inMap()) {
            throw new IllegalStateException("must end a map");
        }

        level--;
        index += encoder.writeCloseMap(buffer, index, level);
        endValue();
        return this;
    }

    @Override
    public ObjectEncoder openList() {
        if (level + 1 == MAX_LEVELS) {
            throw new IllegalStateException("level >= 10");
        }

        startValue(false);
        level++;
        type[level] = '[';
        inKey[level] = false;
        elementIndex[level] = 0;
        index += encoder.writeOpenList(buffer, index, level);
        return this;
    }

    @Override
    public ObjectEncoder closeList() {
        if (!inArray()) {
            throw new IllegalStateException("must end a list");
        }

        level--;
        index += encoder.writeCloseList(buffer, index, level);
        endValue();
        return this;
    }

    @Override
    public ObjectEncoder string(String value) {
        var key = isKey();
        startValue(true);
        if (value == null) {
            index += encoder.writeNull(buffer, index);
        } else {
            index += encoder.writePreString(buffer, index, key);
            index += encoder.writeString(buffer, index, value, key);
            index += encoder.writePostString(buffer, index, key);
        }
        if (!key) {
            endValue();
        }
        return this;
    }

    @Override
    public ObjectEncoder string(DirectBuffer value) {
        return string(value, 0, value == null ? 0 : value.capacity());
    }

    @Override
    public ObjectEncoder string(DirectBuffer value, int offset, int length) {
        var key = isKey();
        startValue(true);
        if (value == null) {
            index += encoder.writeNull(buffer, index);
        } else {
            index += encoder.writePreString(buffer, index, key);
            index += encoder.writeString(buffer, index, value, offset, length, key);
            index += encoder.writePostString(buffer, index, key);
        }
        if (!key) {
            endValue();
        }
        return this;
    }

    @Override
    public ObjectEncoder string(char value) {
        var key = isKey();
        startValue(true);
        index += encoder.writePreString(buffer, index, key);
        index += encoder.writeString(buffer, index, value, key);
        index += encoder.writePostString(buffer, index, key);
        if (!key) {
            endValue();
        }
        return this;
    }

    @Override
    public ObjectEncoder number(long value) {
        startValue(false);
        index += encoder.writeNumber(buffer, index, value);
        endValue();
        return this;
    }

    @Override
    public ObjectEncoder number(long value, NumberValueEncoder valueEncoder) {
        startValue(false);
        index += valueEncoder.encode(encoder, buffer, index, value);
        endValue();
        return this;
    }

    @Override
    public ObjectEncoder number(double value) {
        startValue(false);
        index += encoder.writeNumber(buffer, index, value);
        endValue();
        return this;
    }

    @Override
    public ObjectEncoder number(double value, int minDecimals) {
        startValue(false);
        index += encoder.writeNumber(buffer, index, value, minDecimals);
        endValue();
        return this;
    }

    @Override
    public ObjectEncoder number(double value, int minDecimals, int maxDecimals) {
        startValue(false);
        index += encoder.writeNumber(buffer, index, value, minDecimals, maxDecimals);
        endValue();
        return this;
    }

    @Override
    public ObjectEncoder numberString(long value) {
        if (isKey()) {
            throw new IllegalStateException("illegal numeric key");
        }
        startValue(true);
        index += encoder.writePreString(buffer, index, false);
        index += encoder.writeNumber(buffer, index, value);
        index += encoder.writePostString(buffer, index, false);
        endValue();
        return this;
    }

    @Override
    public ObjectEncoder numberString(double value) {
        if (isKey()) {
            throw new IllegalStateException("illegal numeric key");
        }
        startValue(true);
        index += encoder.writePreString(buffer, index, false);
        index += encoder.writeNumber(buffer, index, value);
        index += encoder.writePostString(buffer, index, false);
        endValue();
        return this;
    }

    @Override
    public ObjectEncoder bool(boolean value) {
        startValue(false);
        index += encoder.writeBoolean(buffer, index, value);
        endValue();
        return this;
    }

    @Override
    public ObjectEncoder object(Object value) {
        if (value == null) {
            startValue(false);
            index += encoder.writeNull(buffer, index);
            endValue();
        } else {
            if (value instanceof Encodable) {
                ((Encodable) value).encode(this);
            } else if (value instanceof DirectBuffer) {
                string((DirectBuffer) value);
            } else if (value instanceof Map<?, ?>) {
                encodeMap((Map<?, ?>) value);
            } else if (value instanceof List<?>) {
                encodeList((List<?>) value);
            } else if (value instanceof Iterable<?>) {
                encodeIterator(((Iterable<?>) value).iterator());
            } else if (value instanceof Iterator<?>) {
                encodeIterator((Iterator<?>) value);
            } else if (value.getClass().isArray()) {
                encodeArray(value);
            } else if (value instanceof Boolean) {
                bool((Boolean) value);
            } else if (value instanceof Byte) {
                number((Byte) value);
            } else if (value instanceof Short) {
                number((Short) value);
            } else if (value instanceof Character) {
                string((Character) value);
            } else if (value instanceof Integer) {
                number((Integer) value);
            } else if (value instanceof Float) {
                number((Float) value);
            } else if (value instanceof Long) {
                number((Long) value);
            } else if (value instanceof Double) {
                number((Double) value);
            } else {
                string(value.toString());
            }
        }
        return this;
    }

    private void encodeMap(Map<?, ?> value) {
        openMap();
        for (var entry : value.entrySet()) {
            if (entry.getKey() instanceof DirectBuffer) {
                string((DirectBuffer) entry.getKey());
            } else {
                string(entry.getKey() == null ? "null" : entry.getKey().toString());
            }
            object(entry.getValue());
        }
        closeMap();
    }

    private void encodeList(List<?> value) {
        openList();
        for (var i = 0; i < value.size(); i++) {
            object(value.get(i));
        }
        closeList();
    }

    private void encodeIterator(Iterator<?> value) {
        openList();
        while (value.hasNext()) {
            object(value.next());
        }
        closeList();
    }

    private void encodeArray(Object value) {
        var length = Array.getLength(value);
        var componentType = value.getClass().getComponentType();

        openList();
        if (componentType == boolean.class) {
            for (var i = 0; i < length; i++) {
                bool(Array.getBoolean(value, i));
            }
        } else if (componentType == byte.class) {
            for (var i = 0; i < length; i++) {
                number(Array.getByte(value, i));
            }
        } else if (componentType == short.class) {
            for (var i = 0; i < length; i++) {
                number(Array.getShort(value, i));
            }
        } else if (componentType == char.class) {
            for (var i = 0; i < length; i++) {
                string(Array.getChar(value, i));
            }
        } else if (componentType == int.class) {
            for (var i = 0; i < length; i++) {
                number(Array.getInt(value, i));
            }
        } else if (componentType == float.class) {
            for (var i = 0; i < length; i++) {
                number(Array.getFloat(value, i), 0, MAX_FLOAT_DECIMALS);
            }
        } else if (componentType == long.class) {
            for (var i = 0; i < length; i++) {
                number(Array.getLong(value, i));
            }
        } else if (componentType == double.class) {
            for (var i = 0; i < length; i++) {
                number(Array.getDouble(value, i));
            }
        } else {
            for (var i = 0; i < length; i++) {
                object(Array.get(value, i));
            }
        }
        closeList();
    }

    private boolean isKey() {
        return inKey[level];
    }

    private boolean inMap() {
        return type[level] == '{';
    }

    private boolean inArray() {
        return type[level] == '[';
    }

    private void startValue(boolean isString) {
        if (inMap()) {
            if (isKey()) {
                if (!isString) {
                    throw new IllegalStateException("cannot have non-string as map key");
                }
                var elIndex = elementIndex[level]++;
                if (elIndex == 0) {
                    index += encoder.writePreFirstElement(buffer, index);
                } else {
                    index += encoder.writeNextElementSeparator(buffer, index, elIndex);
                }
                inKey[level] = false;
            } else {
                index += encoder.writeKeyValueSeparator(buffer, index);
                inKey[level] = true;
            }
        } else {
            var elIndex = elementIndex[level]++;
            if (elIndex == 0) {
                index += encoder.writePreFirstElement(buffer, index);
            } else {
                index += encoder.writeNextElementSeparator(buffer, index, elIndex);
            }
        }
    }

    private void endValue() {
        if (finishLevelListener[level] != null) {
            //inKey[level] = false;
            //type[level] = 0;
            //elementIndex[level] = 0;
            finishLevelListener[level].run();
        }
    }
}
