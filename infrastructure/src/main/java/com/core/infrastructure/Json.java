package com.core.infrastructure;

import com.core.infrastructure.buffer.AsciiStringBuffer;
import com.core.infrastructure.buffer.BufferNumberUtils;
import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.collections.CoreMap;
import com.core.infrastructure.collections.ObjectPool;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import org.agrona.DirectBuffer;

import java.util.List;
import java.util.Map;

/**
 * A JSON parser.
 *
 * <p>See <a href="https://www.json.org/json-en.html">json.org</a> for more details about the format of JSON.
 */
public class Json {

    private static final DirectBuffer TRUE = BufferUtils.fromAsciiString("true");
    private static final long FALSE_VALUE = (((long) 'f') << 32) + (((long) 'a') << 24) + (((long) 'l') << 16)
            + (((long) 's') << 8) + ((long) 'e');
    private static final long TRUE_VALUE = (((long) 't') << 24) + (((long) 'r') << 16) + (((long) 'u') << 8)
            + ((long) 'e');
    private static final long NULL_VALUE = (((long) 'n') << 24) + (((long) 'u') << 16) + (((long) 'l') << 8)
            + ((long) 'l');

    // pools
    private final ObjectPool<DirectBuffer> keyPool;
    private final ObjectPool<MapValue> mapPool;
    private final ObjectPool<ListValue> listPool;
    private final ObjectPool<NullValue> nullPool;
    private final ObjectPool<BoolValue> boolPool;
    private final ObjectPool<LongValue> longPool;
    private final ObjectPool<DoubleValue> doublePool;
    private final ObjectPool<StringValue> stringPool;
    private final AsciiStringBuffer string;

    private ParseResult internalResult;

    // strings
    private boolean inBackslash;
    private int stringStart;
    private boolean inValue;
    private long value;

    // numbers
    private boolean inNumber;
    private boolean inExponent;
    private boolean negativeFraction;
    private long fractionPart;
    private int numDigits;
    private int decimalPoint;
    private boolean negativeExponent;
    private int exponentPart;

    /**
     * Creates an {@code Json} empty JSON parser.
     */
    public Json() {
        keyPool = new ObjectPool<>(BufferUtils::emptyBuffer);
        mapPool = new ObjectPool<>(MapValue::new);
        nullPool = new ObjectPool<>(NullValue::new);
        boolPool = new ObjectPool<>(BoolValue::new);
        listPool = new ObjectPool<>(ListValue::new);
        longPool = new ObjectPool<>(LongValue::new);
        doublePool = new ObjectPool<>(DoubleValue::new);
        stringPool = new ObjectPool<>(StringValue::new);
        string = new AsciiStringBuffer();
        clearInternalState();
    }

    private void clearInternalState() {
        inBackslash = false;
        stringStart = -1;
        inValue = false;
        value = 0;

        inNumber = false;
        inExponent = false;
        negativeFraction = false;
        fractionPart = 0L;
        numDigits = 0;
        decimalPoint = -1;
        negativeExponent = false;
        exponentPart = 0;
    }

    /**
     * Parses the specified {@code buffer} for a JSON value.
     * The returned result will contain the root element (i.e., the first map, list, boolean, number, or null value)
     * and the number of bytes parsed from the buffer.
     * This method should be invoked again to parse additional bytes from the buffer.
     *
     * @param buffer the buffer to parse
     * @param offset the first byte of the buffer to parse
     * @param length the maximum number of bytes to parse
     * @return the JSON result
     */
    public ParseResult parse(DirectBuffer buffer, int offset, int length) {
        if (internalResult == null) {
            internalResult = new ParseResult();
        }
        parse(internalResult, buffer, offset, length);
        return internalResult;
    }

    /**
     * Parses the specified {@code buffer} for a JSON value.
     * The specified {@code result} will be populated with the root element (i.e., the first map, list, boolean, number,
     * or null value) and the number of bytes parsed from the buffer.
     * This method should be invoked again to parse additional bytes from the buffer.
     *
     * @param result the JSON result
     * @param buffer the buffer to parse
     * @param offset the first byte of the buffer to parse
     * @param length the maximum number of bytes to parse
     */
    public void parse(ParseResult result, DirectBuffer buffer, int offset, int length) {
        clearInternalState();
        result.clear();

        var i = 0;
        for (i = offset; i < offset + length; i++) {
            var character = buffer.getByte(i);
            var lastChar = i == offset + length - 1;
            var rewind = character == ',' || character == ']' || character == '}';
            var evaluate = rewind || Character.isWhitespace(character) || lastChar || character == ':';

            if (stringStart != -1) {
                // in string
                if (inBackslash) {
                    // process backslash
                    if (character == '"' || character == '\\' || character == '/' || character == 'b'
                            || character == 'n' || character == 'r' || character == 't' || character == 'u') {
                        inBackslash = false;
                    } else {
                        result.error("invalid escaped character", i);
                        return;
                    }
                } else if (character == '\\') {
                    // enter backslash
                    inBackslash = true;
                } else if (character == '"') {
                    // end string
                    var element = stringPool.borrowObject();
                    element.setValue(buffer, stringStart, i - stringStart);
                    if (result.push(element, i)) {
                        return;
                    }
                    stringStart = -1;
                }
                // otherwise, continue processing string
            } else {
                if (inNumber) {
                    // in value: number
                    if (inExponent) {
                        if (character >= '0' && character <= '9') {
                            exponentPart *= 10;
                            exponentPart += character - '0';
                        } else if (character == '-') {
                            negativeExponent = true;
                        } else if (!evaluate) {
                            result.error("illegal character in number", i);
                            return;
                        }

                        if (evaluate) {
                            // decimal floating point number with an exponent
                            var dblValue = BufferNumberUtils.floatingPointComponentsToDouble(
                                    negativeFraction, fractionPart, decimalPoint, numDigits,
                                    negativeExponent, exponentPart, Double.NEGATIVE_INFINITY);
                            if (dblValue == Double.NEGATIVE_INFINITY) {
                                result.error("cannot parse number", i);
                                return;
                            } else {
                                var element = doublePool.borrowObject();
                                element.value = dblValue;
                                if (result.push(element, i)) {
                                    return;
                                }
                            }

                            inNumber = false;
                            if (rewind) {
                                // rewind one character so we can process end of list/map in next loop
                                i--;
                            }
                        }
                    } else {
                        if (character >= '0' && character <= '9') {
                            fractionPart *= 10;
                            fractionPart += character - '0';
                            numDigits++;
                        } else if (character == '.') {
                            if (decimalPoint != -1) {
                                result.error("two decimal points in number", i);
                                return;
                            }
                            decimalPoint = numDigits;
                        } else if (character == 'e' || character == 'E') {
                            inExponent = true;
                        } else if (!evaluate) {
                            result.error("illegal character in number", i);
                            return;
                        }

                        if (evaluate) {
                            // end number
                            if (decimalPoint == -1) {
                                var element = longPool.borrowObject();
                                element.value = (negativeFraction ? -1 : 1) * fractionPart;
                                if (result.push(element, i)) {
                                    return;
                                }
                            } else {
                                var dblValue = BufferNumberUtils.floatingPointComponentsToDouble(
                                        negativeFraction, fractionPart, decimalPoint, numDigits,
                                        negativeExponent, exponentPart, Double.NEGATIVE_INFINITY);
                                if (dblValue == Double.NEGATIVE_INFINITY) {
                                    result.error("cannot parse number", i);
                                    return;
                                } else {
                                    var element = doublePool.borrowObject();
                                    element.value = dblValue;
                                    if (result.push(element, i)) {
                                        return;
                                    }
                                }
                            }

                            inNumber = false;
                            if (rewind) {
                                // rewind one character so we can process end of list/map in next loop
                                i--;
                            }
                        }
                    }
                } else if (inValue) {
                    // in value: true, false, or null
                    if (character >= 'a' && character <= 'z') {
                        value <<= 8;
                        value += character;
                    }

                    if (evaluate) {
                        inValue = false;
                        if (value == TRUE_VALUE) {
                            var element = boolPool.borrowObject();
                            element.value = true;
                            if (result.push(element, i)) {
                                return;
                            }
                        } else if (value == FALSE_VALUE) {
                            var element = boolPool.borrowObject();
                            element.value = false;
                            if (result.push(element, i)) {
                                return;
                            }
                        } else if (value == NULL_VALUE) {
                            if (result.push(nullPool.borrowObject(), i)) {
                                return;
                            }
                        } else  if (lastChar && rewind) {
                            result.error("invalid value", i);
                            return;
                        }

                        if (rewind) {
                            // rewind one character so we can process end of list/map in next loop
                            i--;
                        }
                    }
                }  else if (character == '"') {
                    // start of a string
                    stringStart = i + 1;
                } else if (character == '-' || character == '.' || character >= '0' && character <= '9') {
                    // start of number
                    inNumber = true;
                    inExponent = false;
                    negativeFraction = character == '-';
                    fractionPart = character == '-' || character == '.' ? 0 : character - '0';
                    decimalPoint = character == '.' ? 0 : -1;
                    numDigits = character == '-' || character == '.' || character == '0' ? 0 : 1;
                    negativeExponent = false;
                    exponentPart = 0;
                } else if (character == 'f' || character == 't' || character == 'n') {
                    // start of value: true, false, or null
                    value = character;
                    inValue = true;
                } else if (character == '{') {
                    // new map
                    if (result.push(mapPool.borrowObject(), i)) {
                        return;
                    }
                } else if (character == '[') {
                    // new list
                    if (result.push(listPool.borrowObject(), i)) {
                        return;
                    }
                } else if (!Character.isWhitespace(character)) {
                    if (result.isInMap()) {
                        // in map
                        if (character == '}') {
                            // end of map
                            if (result.pop(i)) {
                                return;
                            }
                            if (result.current == null) {
                                i++;
                                break;
                            }
                        } else if (character == ':') {
                            if (result.colon(i)) {
                                return;
                            }
                        } else if (character == ',') {
                            if (result.comma(i)) {
                                return;
                            }
                        } else {
                            result.error("illegal character in map", i);
                            return;
                        }
                    } else if (result.isInList()) {
                        // in list
                        if (character == ']') {
                            // end of list
                            if (result.pop(i)) {
                                return;
                            }
                            if (result.current == null) {
                                i++;
                                break;
                            }
                        } else if (character == ',') {
                            if (result.comma(i)) {
                                return;
                            }
                        } else {
                            result.error("illegal character in list", i);
                            return;
                        }
                    } else {
                        // skip whitespace
                        result.error("illegal character", i);
                        return;
                    }
                }
            }
        }

        if (result.isComplete()) {
            result.lengthParsed = i - offset;
        } else {
            result.lengthParsed = 0;
        }
    }

    /**
     * A {@code JsonParseResult} is the returned value of the {@code Json.parse()} method.
     */
    public static class ParseResult implements Encodable {

        String errorReason;
        int errorIndex;
        int lengthParsed;
        Value root;
        Value current;
        StringValue key;
        boolean colon;
        boolean comma;

        ParseResult() {
        }

        /**
         * Returns true if this there was an error parsing the JSON.
         *
         * @return true if this there was an error parsing the JSON
         */
        public boolean isError() {
            return errorReason != null;
        }

        /**
         * Returns the reason for the JSON parse error.
         *
         * @return the reason for the JSON parse error
         */
        public String getErrorReason() {
            return errorReason;
        }

        /**
         * Returns the index of the character that caused the parse error from the JSON buffer.
         *
         * @return the index of the character that caused the parse error from the JSON buffer
         */
        public int getErrorIndex() {
            return errorIndex;
        }

        /**
         * Returns the number of bytes successfully parsed.
         * A value of -1 is returned if there was an error parsing the JSON buffer.
         * A value of 0 is returned if the JSON buffer was not a complete value.
         *
         * @return the number of bytes successfully parsed
         */
        public int getLengthParsed() {
            return lengthParsed;
        }

        /**
         * Returns the root JSON value parsed.
         *
         * @return the root JSON value parsed
         */
        public Value getRoot() {
            return root;
        }

        boolean isInList() {
            return current != null && current.isList();
        }

        boolean isInMap() {
            return current != null && current.isMap();
        }

        boolean isComplete() {
            return root != null && (current == null || !current.isList() && !current.isMap());
        }

        boolean push(Value element, int index) {
            if (root == null) {
                // first element
                root = element;
                current = element;
                return false;
            }

            if (current.isList()) {
                element.parent = current;
                ((ListValue) current).addValue(element);
                if (element.isMap() || element.isList()) {
                    current = element;
                }
                comma = false;
                return false;
            } else if (current.isMap()) {
                if (key == null) {
                    if (element.isString()) {
                        key = (StringValue) element;
                        colon = false;
                        return false;
                    } else {
                        error("non-string key", index);
                        return true;
                    }
                } else {
                    element.parent = current;
                    if (((MapValue) current).putValue(key, element)) {
                        error("duplicate key", index);
                        return true;
                    }
                    key.clear();
                    key = null;
                    comma = false;
                    if (element.isMap() || element.isList()) {
                        current = element;
                    }
                    return false;
                }
            } else {
                error("can only add child element to a map or list", index);
                return true;
            }
        }

        boolean pop(int index) {
            if (current == null) {
                error("illegal closing of list/map", index);
                return true;
            } else {
                current = current.parent;
                return false;
            }
        }

        boolean colon(int index) {
            if (current == null || !current.isMap()) {
                error("colon character not in map", index);
                return true;
            }

            if (colon) {
                error("illegal colon in map", index);
                return true;
            } else {
                colon = true;
                return false;
            }
        }

        boolean comma(int index) {
            if (current == null) {
                error("comma not in map/list", index);
                return true;
            } else if (current.isMap()) {
                if (current.asMap().isEmpty()) {
                    error("illegal comma in map", index);
                    return true;
                } else if (comma) {
                    error("illegal comma in map", index);
                    return true;
                } else {
                    comma = true;
                    return false;
                }
            } else if (current.isList()) {
                if (current.asList().isEmpty()) {
                    error("illegal comma in list", index);
                    return true;
                } else if (comma) {
                    error("illegal comma in list", index);
                    return true;
                } else {
                    comma = true;
                    return false;
                }
            } else {
                error("comma not in map/list", index);
                return true;
            }
        }

        void error(String reason, int index) {
            this.errorReason = reason;
            this.errorIndex = index;
            lengthParsed = -1;
        }

        void clear() {
            errorReason = null;
            errorIndex = -1;
            lengthParsed = 0;
            if (root != null) {
                root.clear();
                root = null;
            }
            current = null;
            if (key != null) {
                key.clear();
                key = null;
            }
            colon = false;
            comma = false;
        }

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.openMap();

            if (errorReason != null) {
                encoder.string("error").string(errorReason)
                        .string("errorIndex").number(errorIndex);
            } else {
                encoder.string("lengthParsed").number(lengthParsed)
                        .string("value").object(root);
            }

            encoder.closeMap();
        }

        @Override
        public String toString() {
            return toEncodedString();
        }
    }

    /**
     * The type of JSON value.
     */
    public enum ValueType {

        /**
         * A null value.
         */
        NULL,
        /**
         * A string value.
         */
        STRING,
        /**
         * A double value.
         */
        DOUBLE,
        /**
         * A long value.
         */
        LONG,
        /**
         * A boolean value.
         */
        BOOLEAN,
        /**
         * A map value.
         */
        MAP,
        /**
         * A list value.
         */
        LIST
    }

    /**
     * The JSON value.
     */
    public abstract static class Value {

        Value parent;

        Value() {
        }

        abstract void clear();

        /**
         * Returns the type of this value.
         *
         * @return the type of this value
         */
        public abstract ValueType getType();

        /**
         * Returns true if the value is a null type.
         *
         * @return true if the value is a null type
         */
        public boolean isNull() {
            return getType() == ValueType.NULL;
        }

        /**
         * Returns true if the value is a boolean type.
         *
         * @return true if the value is a boolean type
         */
        public boolean isBool() {
            return getType() == ValueType.BOOLEAN;
        }

        /**
         * Returns true if the value is a long type.
         *
         * @return true if the value is a long type
         */
        public boolean isLong() {
            return getType() == ValueType.LONG;
        }

        /**
         * Returns true if the value is a double type.
         *
         * @return true if the value is a double type
         */
        public boolean isDouble() {
            return getType() == ValueType.DOUBLE;
        }

        /**
         * Returns true if the value is a string type.
         *
         * @return true if the value is a string type
         */
        public boolean isString() {
            return getType() == ValueType.STRING;
        }

        /**
         * Returns true if the value is a list type.
         *
         * @return true if the value is a list type
         */
        public boolean isList() {
            return getType() == ValueType.LIST;
        }

        /**
         * Returns true if the value is a map type.
         *
         * @return true if the value is a map type
         */
        public boolean isMap() {
            return getType() == ValueType.MAP;
        }

        /**
         * Returns the null value.
         *
         * @return the null value
         * @throws IllegalStateException if the value is not a null value
         */
        public Object asNullValue() {
            if (isNull()) {
                return ((NullValue) this).getValue();
            } else {
                throw new IllegalStateException("not a null value");
            }
        }

        /**
         * Returns the value as a string.
         *
         * @return the value as a string
         * @throws IllegalStateException if the value is not a null value
         */
        public DirectBuffer asStringValue() {
            if (isString()) {
                return ((StringValue) this).value;
            } else {
                throw new IllegalStateException("not a string value");
            }
        }

        DirectBuffer internalAsStringValue() {
            return ((StringValue) this).value;
        }

        /**
         * Returns the value as a string.
         *
         * @return the value as a string
         */
        public DirectBuffer asOptionalStringValue() {
            return isString() ? ((StringValue) this).value : null;
        }

        /**
         * Returns the value as a long.
         *
         * @return the value as a long
         * @throws IllegalStateException if the value is not a null value
         */
        public long asLongValue() {
            if (isLong()) {
                return ((LongValue) this).value;
            } else {
                throw new IllegalStateException("not a long value");
            }
        }

        long internalAsLongValue() {
            return ((LongValue) this).value;
        }

        /**
         * Returns the value as a long, or -1 if the value is not a long.
         *
         * @return the value as a long
         */
        public long asOptionalLongValue() {
            return asOptionalLongValue(-1);
        }

        /**
         * Returns the value as a long, or the specified default value if the value is not a long.
         *
         * @param defaultValue the default value
         * @return the value as a long
         */
        public long asOptionalLongValue(long defaultValue) {
            return isLong() ? ((LongValue) this).value : defaultValue;
        }

        /**
         * Returns the value as a double, or NaN if the value not a double.
         *
         * @return the value as a double
         */
        public double asOptionalDoubleValue() {
            return asOptionalDoubleValue(Double.NaN);
        }

        /**
         * Returns the value as a double, or the specified default value if the value is not a double.
         *
         * @param defaultValue the default value
         * @return the value as a double
         */
        public double asOptionalDoubleValue(double defaultValue) {
            return isDouble() ? ((DoubleValue) this).value : defaultValue;
        }

        /**
         * Returns the value as a double.
         *
         * @return the value as a double
         * @throws IllegalStateException if the value is not a null value
         */
        public double asDoubleValue() {
            if (isDouble()) {
                return ((DoubleValue) this).value;
            } else {
                throw new IllegalStateException("not a double value");
            }
        }

        double internalAsDoubleValue() {
            return ((DoubleValue) this).value;
        }

        /**
         * Returns the value as a boolean, or false if the value is not a boolean.
         *
         * @return the value as a boolean
         */
        public boolean asOptionalBoolValue() {
            return asOptionalBoolValue(false);
        }

        /**
         * Returns the value as a boolean, of the specified default value if the value is not a boolean.
         *
         * @param defaultValue the default value
         * @return the value as a boolean
         */
        public boolean asOptionalBoolValue(boolean defaultValue) {
            return isBool() ? ((BoolValue) this).value : defaultValue;
        }

        /**
         * Returns the value as a boolean.
         *
         * @return the value as a boolean
         * @throws IllegalStateException if the value is not a null value
         */
        public boolean asBoolValue() {
            if (isBool()) {
                return ((BoolValue) this).value;
            } else {
                throw new IllegalStateException("not a bool value");
            }
        }

        boolean internalAsBoolValue() {
            return ((BoolValue) this).value;
        }

        /**
         * Returns the value as a map.
         *
         * @return the value as a map
         * @throws IllegalStateException if the value is not a null value
         */
        public Map<DirectBuffer, Value> asMapValue() {
            if (isMap()) {
                return ((MapValue) this).value;
            } else {
                throw new IllegalStateException("not a map value");
            }
        }

        Map<DirectBuffer, Value> internalAsMapValue() {
            return ((MapValue) this).value;
        }

        /**
         * Returns the value as a list, or null if the value is not a map.
         *
         * @return the value as a list
         */
        public Map<DirectBuffer, Value> asOptionalMapValue() {
            return isMap() ? ((MapValue) this).value : null;
        }

        /**
         * Returns the value as a map.
         *
         * @return the value as a map
         * @throws IllegalStateException if the value is not a null value
         */
        public MapValue asMap() {
            if (isMap()) {
                return (MapValue) this;
            } else {
                throw new IllegalStateException("not a map value");
            }
        }

        MapValue internalAsMap() {
            return (MapValue) this;
        }

        /**
         * Returns the value as a list, or null if the value is not a map.
         *
         * @return the value as a list
         */
        public MapValue asOptionalMap() {
            return isMap() ? (MapValue) this : null;
        }

        /**
         * Returns the value as a list.
         *
         * @return the value as a list
         * @throws IllegalStateException if the value is not a null value
         */
        public List<Value> asListValue() {
            if (isList()) {
                return ((ListValue) this).value;
            } else {
                throw new IllegalStateException("not a list value");
            }
        }

        List<Value> internalAsListValue() {
            return ((ListValue) this).value;
        }

        /**
         * Returns the value as a list, or null if the value is not a list.
         *
         * @return the value as a list
         */
        public List<Value> asOptionalListValue() {
            return isList() ? ((ListValue) this).value : null;
        }

        /**
         * Returns the value as a list.
         *
         * @return the value as a list
         * @throws IllegalStateException if the value is not a null value
         */
        public ListValue asList() {
            if (isList()) {
                return (ListValue) this;
            } else {
                throw new IllegalStateException("not a list value");
            }
        }

        ListValue internalAsList() {
            return (ListValue) this;
        }

        /**
         * Returns the value as a list, or null if the value is not a list.
         *
         * @return the value as a list
         */
        public ListValue asOptionalList() {
            return isList() ? (ListValue) this : null;
        }
    }

    /**
     * A map value.
     */
    public class MapValue extends Value implements Encodable {

        final Map<DirectBuffer, Value> value;

        MapValue() {
            super();
            value = new CoreMap<>();
        }

        boolean putValue(StringValue key, Value value) {
            var keyBuf = keyPool.borrowObject();
            keyBuf.wrap(key.value, 0, key.value.capacity());
            return this.value.put(keyBuf, value) != null;
        }

        /**
         * Returns the number of entries in the map.
         *
         * @return the number of entries in the map
         */
        public int size() {
            return value.size();
        }

        /**
         * Returns true if the map has no entries.
         *
         * @return true if the map has no entries
         */
        public boolean isEmpty() {
            return value.isEmpty();
        }

        @Override
        public ValueType getType() {
            return ValueType.MAP;
        }

        @Override
        void clear() {
            parent = null;
            for (var entry : value.entrySet()) {
                keyPool.returnObject(entry.getKey());
                entry.getValue().clear();
            }
            value.clear();
            mapPool.returnObject(this);
        }

        /**
         * Returns true if the key is present in the map.
         *
         * @param key the key
         * @return true if the key is present in the map
         */
        public boolean containsKey(DirectBuffer key) {
            return this.value.containsKey(key);
        }

        /**
         * Returns true if the key is present in the map.
         *
         * @param key the key
         * @return true if the key is present in the map
         */
        public boolean containsKey(String key) {
            return containsKey(string.wrap(key));
        }

        /**
         * Returns the value for the specified {@code key}, or null if the key is not present.
         *
         * @param key the key
         * @return the value
         */
        public Value getOptional(DirectBuffer key) {
            return this.value.get(key);
        }

        /**
         * Returns the value for the specified {@code key}, or null if the key is not present.
         *
         * @param key the key
         * @return the value
         */
        public Value getOptional(String key) {
            return getOptional(string.wrap(key));
        }

        /**
         * Returns the value for the specified {@code key}, or null if the key is not present.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present
         */
        public Value get(DirectBuffer key) {
            var v = this.value.get(key);
            if (v == null) {
                throw new IllegalArgumentException("invalid key: " + BufferUtils.toAsciiString(key));
            }
            return v;
        }

        /**
         * Returns the value for the specified {@code key}, or null if the key is not present.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present
         */
        public Value get(String key) {
            return get(string.wrap(key));
        }

        /**
         * Returns the null value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present or is not a null value
         */
        public Object getNullValue(DirectBuffer key) {
            var value = this.value.get(key);
            if (value == null) {
                throw new IllegalArgumentException("invalid null value key: " + BufferUtils.toAsciiString(key));
            } else if (value.isNull()) {
                return null;
            } else {
                throw new IllegalArgumentException("invalid null value key: " + BufferUtils.toAsciiString(key));
            }
        }

        /**
         * Returns the null value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present or is not a null value
         */
        public Object getNullValue(String key) {
            return getNullValue(string.wrap(key));
        }

        /**
         * Returns the string value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present or not a string value
         */
        public DirectBuffer getStringValue(DirectBuffer key) {
            var value = this.value.get(key);
            if (value == null) {
                throw new IllegalArgumentException("invalid string value key: " + BufferUtils.toAsciiString(key));
            } else if (value.isString()) {
                return value.internalAsStringValue();
            } else {
                throw new IllegalArgumentException("invalid string value key: " + BufferUtils.toAsciiString(key));
            }
        }

        /**
         * Returns the string value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present or not a string value
         */
        public DirectBuffer getStringValue(String key) {
            return getStringValue(string.wrap(key));
        }

        /**
         * Returns the string value for the specified {@code key}, or null if the key is not present or the value is not
         * a string value.
         *
         * @param key the key
         * @return the value
         */
        public DirectBuffer getOptionalStringValue(DirectBuffer key) {
            var value = this.value.get(key);
            if (value == null) {
                return null;
            } else if (value.isString()) {
                return value.internalAsStringValue();
            } else {
                return null;
            }
        }

        /**
         * Returns the string value for the specified {@code key}, or null if the key is not present or the value is not
         * a string value.
         *
         * @param key the key
         * @return the value
         */
        public DirectBuffer getOptionalStringValue(String key) {
            return getOptionalStringValue(string.wrap(key));
        }

        /**
         * Returns the long value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is present or not a long value
         */
        public long getLongValue(DirectBuffer key) {
            var value = this.value.get(key);
            if (value == null) {
                throw new IllegalArgumentException("invalid long value key: " + BufferUtils.toAsciiString(key));
            } else if (value.isLong()) {
                return value.internalAsLongValue();
            } else if (value.isString()) {
                var buf = value.internalAsStringValue();
                return buf.parseLongAscii(0, buf.capacity());
            } else {
                throw new IllegalArgumentException("invalid long value key: " + BufferUtils.toAsciiString(key));
            }
        }

        /**
         * Returns the long value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present or not a long value
         */
        public long getLongValue(String key) {
            return getLongValue(string.wrap(key));
        }

        /**
         * Returns the long value for the specified {@code key}, or -1 if the key is not present or the value is not
         * a long value.
         *
         * @param key the key
         * @return the value
         */
        public long getOptionalLongValue(DirectBuffer key) {
            return getOptionalLongValue(key, -1);
        }

        /**
         * Returns the long value for the specified {@code key}, or -1 if the key is not present or the value is not
         * a long value.
         *
         * @param key the key
         * @return the value
         */
        public long getOptionalLongValue(String key) {
            return getOptionalLongValue(string.wrap(key), -1);
        }

        /**
         * Returns the long value for the specified {@code key}, or the specified default value if the key is not
         * present or the value is not a long value.
         *
         * @param key the key
         * @param defaultValue the default value
         * @return the value
         */
        public long getOptionalLongValue(DirectBuffer key, long defaultValue) {
            var value = this.value.get(key);
            if (value == null) {
                return defaultValue;
            } else if (value.isLong()) {
                return value.internalAsLongValue();
            } else if (value.isString()) {
                var buf = value.internalAsStringValue();
                return buf.parseLongAscii(0, buf.capacity());
            } else {
                return defaultValue;
            }
        }

        /**
         * Returns the long value for the specified {@code key}, or the specified default value if the key is not
         * present or the value is not a long value.
         *
         * @param key the key
         * @param defaultValue the default value
         * @return the value
         */
        public long getOptionalLongValue(String key, long defaultValue) {
            return getOptionalLongValue(string.wrap(key), defaultValue);
        }

        /**
         * Returns the double value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present or not a double value
         */
        public double getDoubleValue(DirectBuffer key) {
            var value = this.value.get(key);
            if (value == null) {
                throw new IllegalArgumentException("invalid double value key: " + BufferUtils.toAsciiString(key));
            } else if (value.isDouble()) {
                return value.internalAsDoubleValue();
            } else if (value.isLong()) {
                return value.internalAsLongValue();
            } else if (value.isString())  {
                return BufferNumberUtils.fastParseAsDouble(value.internalAsStringValue(), Double.NaN);
            } else {
                throw new IllegalArgumentException("invalid double value key: " + BufferUtils.toAsciiString(key));
            }
        }

        /**
         * Returns the double value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present or not a double value
         */
        public double getDoubleValue(String key) {
            return getDoubleValue(string.wrap(key));
        }

        /**
         * Returns the double value for the specified {@code key}, or NaN if the key is not present or the value is not
         * a double value.
         *
         * @param key the key
         * @return the value
         */
        public double getOptionalDoubleValue(DirectBuffer key) {
            return getOptionalDoubleValue(key, Double.NaN);
        }

        /**
         * Returns the double value for the specified {@code key}, or NaN if the key is not present or the value is not
         * a double value.
         *
         * @param key the key
         * @return the value
         */
        public double getOptionalDoubleValue(String key) {
            return getOptionalDoubleValue(string.wrap(key), Double.NaN);
        }

        /**
         * Returns the double value for the specified {@code key}, or the specified default value if the key is not
         * present or the value is not a double value.
         *
         * @param key the key
         * @param defaultValue the default value
         * @return the value
         */
        public double getOptionalDoubleValue(DirectBuffer key, double defaultValue) {
            var value = this.value.get(key);
            if (value == null) {
                return defaultValue;
            } else if (value.isDouble()) {
                return value.internalAsDoubleValue();
            } else if (value.isLong()) {
                return value.internalAsLongValue();
            } else if (value.isString())  {
                return BufferNumberUtils.fastParseAsDouble(value.internalAsStringValue(), Double.NaN);
            } else {
                return defaultValue;
            }
        }

        /**
         * Returns the double value for the specified {@code key}, or the specified default value if the key is not
         * present or the value is not a double value.
         *
         * @param key the key
         * @param defaultValue the default value
         * @return the value
         */
        public double getOptionalDoubleValue(String key, double defaultValue) {
            return getOptionalDoubleValue(string.wrap(key), defaultValue);
        }

        /**
         * Returns the boolean value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present or not a boolean value
         */
        public boolean getBoolValue(DirectBuffer key) {
            var value = this.value.get(key);
            if (value == null) {
                throw new IllegalArgumentException("invalid bool value key: " + BufferUtils.toAsciiString(key));
            } else if (value.isBool())  {
                return value.internalAsBoolValue();
            } else if (value.isString()) {
                return value.internalAsStringValue().equals(TRUE);
            } else {
                throw new IllegalArgumentException("invalid bool value key: " + BufferUtils.toAsciiString(key));
            }
        }

        /**
         * Returns the boolean value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present or not a boolean value
         */
        public boolean getBoolValue(String key) {
            return getBoolValue(string.wrap(key));
        }

        /**
         * Returns the boolean value for the specified {@code key}, or false if the key is not present or the value is
         * not a boolean value.
         *
         * @param key the key
         * @return the value
         */
        public boolean getOptionalBoolValue(DirectBuffer key) {
            return getOptionalBoolValue(key, false);
        }

        /**
         * Returns the boolean value for the specified {@code key}, or false if the key is not present or the value is
         * not a boolean value.
         *
         * @param key the key
         * @return the value
         */
        public boolean getOptionalBoolValue(String key) {
            return getOptionalBoolValue(string.wrap(key), false);
        }

        /**
         * Returns the boolean value for the specified {@code key}, or the specified default value if the key is not
         * present or if the value is not a boolean value.
         *
         * @param key the key
         * @param defaultValue the default value
         * @return the value
         */
        public boolean getOptionalBoolValue(DirectBuffer key, boolean defaultValue) {
            var value = this.value.get(key);
            if (value == null) {
                return defaultValue;
            } else if (value.isBool())  {
                return value.internalAsBoolValue();
            } else if (value.isString()) {
                return value.internalAsStringValue().equals(TRUE);
            } else {
                return defaultValue;
            }
        }

        /**
         * Returns the boolean value for the specified {@code key}, or the specified default value if the key is not
         * present or if the value is not a boolean value.
         *
         * @param key the key
         * @param defaultValue the default value
         * @return the value
         */
        public boolean getOptionalBoolValue(String key, boolean defaultValue) {
            return getOptionalBoolValue(string.wrap(key), defaultValue);
        }

        /**
         * Returns the list value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present or not a list value
         */
        public List<Value> getListValue(DirectBuffer key) {
            var value = this.value.get(key);
            if (value == null) {
                throw new IllegalArgumentException("invalid list value key: " + BufferUtils.toAsciiString(key));
            } else if (value.isList()) {
                return value.internalAsListValue();
            } else {
                throw new IllegalArgumentException("invalid list value key: " + BufferUtils.toAsciiString(key));
            }
        }

        /**
         * Returns the list value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present or not a list value
         */
        public List<Value> getListValue(String key) {
            return getListValue(string.wrap(key));
        }

        /**
         * Returns the list value for the specified {@code key}, or null if the key is not present or if the value is
         * not a list value.
         *
         * @param key the key
         * @return the value
         */
        public List<Value> getOptionalListValue(DirectBuffer key) {
            var value = this.value.get(key);
            if (value == null) {
                return null;
            } else if (value.isList()) {
                return value.internalAsListValue();
            } else {
                return null;
            }
        }

        /**
         * Returns the list value for the specified {@code key}, or null if the key is not present or if the value is
         * not a list value.
         *
         * @param key the key
         * @return the value
         */
        public List<Value> getOptionalListValue(String key) {
            return getOptionalListValue(string.wrap(key));
        }

        /**
         * Returns the list value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present or not a list value
         */
        public ListValue getList(DirectBuffer key) {
            var value = this.value.get(key);
            if (value == null) {
                throw new IllegalArgumentException("invalid list key: " + BufferUtils.toAsciiString(key));
            } else if (value.isList()) {
                return value.internalAsList();
            } else {
                throw new IllegalArgumentException("invalid list key: " + BufferUtils.toAsciiString(key));
            }
        }

        /**
         * Returns the list value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present or not a list value
         */
        public ListValue getList(String key) {
            return getList(string.wrap(key));
        }

        /**
         * Returns the list value for the specified {@code key}, or null if the key is not present or if the value is
         * not a list value.
         *
         * @param key the key
         * @return the value
         */
        public ListValue getOptionalList(DirectBuffer key) {
            var value = this.value.get(key);
            if (value == null) {
                return null;
            } else if (value.isList()) {
                return value.internalAsList();
            } else {
                return null;
            }
        }

        /**
         * Returns the list value for the specified {@code key}, or null if the key is not present or if the value is
         * not a list value.
         *
         * @param key the key
         * @return the value
         */
        public ListValue getOptionalList(String key) {
            return getOptionalList(string.wrap(key));
        }

        /**
         * Returns the map value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present or not a map value
         */
        public Map<DirectBuffer, Value> getMapValue(DirectBuffer key) {
            var value = this.value.get(key);
            if (value == null) {
                throw new IllegalArgumentException("invalid map value key: " + BufferUtils.toAsciiString(key));
            } else if (value.isMap()) {
                return value.internalAsMapValue();
            } else {
                throw new IllegalArgumentException("invalid map value key: " + BufferUtils.toAsciiString(key));
            }
        }

        /**
         * Returns the map value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present or not a map value
         */
        public Map<DirectBuffer, Value> getMapValue(String key) {
            return getMapValue(string.wrap(key));
        }

        /**
         * Returns the map value for the specified {@code key}, or null if the key is not present or if the value is
         * not a map value.
         *
         * @param key the key
         * @return the value
         */
        public Map<DirectBuffer, Value> getOptionalMapValue(DirectBuffer key) {
            var value = this.value.get(key);
            if (value == null) {
                return null;
            } else if (value.isMap()) {
                return value.internalAsMapValue();
            } else {
                return null;
            }
        }

        /**
         * Returns the map value for the specified {@code key}, or null if the key is not present or if the value is
         * not a map value.
         *
         * @param key the key
         * @return the value
         */
        public Map<DirectBuffer, Value> getOptionalMapValue(String key) {
            return getOptionalMapValue(string.wrap(key));
        }

        /**
         * Returns the map value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present or not a map value
         */
        public MapValue getMap(DirectBuffer key) {
            var value = this.value.get(key);
            if (value == null) {
                throw new IllegalArgumentException("invalid map key: " + BufferUtils.toAsciiString(key));
            } else if (value.isMap()) {
                return value.internalAsMap();
            } else {
                throw new IllegalArgumentException("invalid map key: " + BufferUtils.toAsciiString(key));
            }
        }

        /**
         * Returns the map value for the specified {@code key}.
         *
         * @param key the key
         * @return the value
         * @throws IllegalArgumentException if the value is not present or not a map value
         */
        public MapValue getMap(String key) {
            return getMap(string.wrap(key));
        }

        /**
         * Returns the map value for the specified {@code key}, or null if the key is not present or if the value is
         * not a map value.
         *
         * @param key the key
         * @return the value
         */
        public MapValue getOptionalMap(DirectBuffer key) {
            var value = this.value.get(key);
            if (value == null) {
                return null;
            } else if (value.isMap()) {
                return value.internalAsMap();
            } else {
                return null;
            }
        }

        /**
         * Returns the map value for the specified {@code key}, or null if the key is not present or if the value is
         * not a map value.
         *
         * @param key the key
         * @return the value
         */
        public MapValue getOptionalMap(String key) {
            return getOptionalMap(string.wrap(key));
        }

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.openMap();
            for (var item : value.entrySet()) {
                encoder.string(item.getKey()).object(item.getValue());
            }
            encoder.closeMap();
        }

        @Override
        public String toString() {
            return toEncodedString();
        }
    }

    /**
     * A list value.
     */
    public class ListValue extends Value implements Encodable {

        final List<Value> value;

        ListValue() {
            super();
            value = new CoreList<>();
        }

        void addValue(Value element) {
            value.add(element);
        }

        /**
         * Returns the number of entries in the map.
         *
         * @return the number of entries in the map
         */
        public int size() {
            return value.size();
        }

        /**
         * Returns true if the map has no entries.
         *
         * @return true if the map has no entries
         */
        public boolean isEmpty() {
            return value.isEmpty();
        }

        @Override
        public ValueType getType() {
            return ValueType.LIST;
        }

        @Override
        void clear() {
            parent = null;
            for (var element : value) {
                element.clear();
            }
            value.clear();
            listPool.returnObject(this);
        }

        /**
         * Returns the null value for the specified {@code index}.
         *
         * @param index the index
         * @return the value
         * @throws IllegalArgumentException if the value is not a null value
         */
        public Object getNullValue(int index) {
            var value = this.value.get(index);
            if (value == null) {
                throw new IllegalArgumentException("invalid null value index: " + index);
            } else if (value.isNull()) {
                return null;
            } else {
                throw new IllegalArgumentException("invalid null value index: " + index);
            }
        }

        /**
         * Returns the string value for the specified {@code index}.
         *
         * @param index the index
         * @return the value
         * @throws IllegalArgumentException if the value is not a string value
         */
        public DirectBuffer getStringValue(int index) {
            var value = this.value.get(index);
            if (value == null) {
                throw new IllegalArgumentException("invalid string value index: " + index);
            } else if (value.isString()) {
                return value.internalAsStringValue();
            } else {
                throw new IllegalArgumentException("invalid string value index: " + index);
            }
        }

        /**
         * Returns the string value for the specified {@code index}, or null if the index is not present or the value is
         * not a string value.
         *
         * @param index the index
         * @return the value
         */
        public DirectBuffer getOptionalStringValue(int index) {
            var value = this.value.get(index);
            if (value == null) {
                return null;
            } else if (value.isString()) {
                return value.internalAsStringValue();
            } else {
                return null;
            }
        }

        /**
         * Returns the long value for the specified {@code index}.
         *
         * @param index the index
         * @return the value
         * @throws IllegalArgumentException if the value is not a long value
         */
        public long getLongValue(int index) {
            var value = this.value.get(index);
            if (value == null) {
                throw new IllegalArgumentException("invalid long value index: " + index);
            } else if (value.isLong()) {
                return value.internalAsLongValue();
            } else if (value.isString()) {
                var buf = value.internalAsStringValue();
                return buf.parseLongAscii(0, buf.capacity());
            } else {
                throw new IllegalArgumentException("invalid long value index: " + index);
            }
        }

        /**
         * Returns the long value for the specified {@code index}, or -1 if the index is not present or the value is
         * not a long value.
         *
         * @param index the index
         * @return the value
         */
        public long getOptionalLongValue(int index) {
            return getOptionalLongValue(index, -1);
        }

        /**
         * Returns the long value for the specified {@code index}, or the specified default value if the index is not
         * present or the value is not a long value.
         *
         * @param index the index
         * @param defaultValue the default value
         * @return the value
         */
        public long getOptionalLongValue(int index, long defaultValue) {
            var value = this.value.get(index);
            if (value == null) {
                return defaultValue;
            } else if (value.isLong()) {
                return value.internalAsLongValue();
            } else if (value.isString()) {
                var buf = value.internalAsStringValue();
                return buf.parseLongAscii(0, buf.capacity());
            } else {
                return defaultValue;
            }
        }

        /**
         * Returns the double value for the specified {@code index}.
         *
         * @param index the index
         * @return the value
         * @throws IllegalArgumentException if the value is not a double value
         */
        public double getDoubleValue(int index) {
            var value = this.value.get(index);
            if (value == null) {
                throw new IllegalArgumentException("invalid double value index: " + index);
            } else if (value.isDouble()) {
                return value.internalAsDoubleValue();
            } else if (value.isLong()) {
                return value.internalAsLongValue();
            } else if (value.isString())  {
                return BufferNumberUtils.fastParseAsDouble(value.internalAsStringValue(), Double.NaN);
            } else {
                throw new IllegalArgumentException("invalid double value index: " + index);
            }
        }

        /**
         * Returns the null value for the specified {@code index}, or NaN if the index is not present or the value is
         * not a double value.
         *
         * @param index the index
         * @return the value
         */
        public double getOptionalDoubleValue(int index) {
            return getOptionalDoubleValue(index, Double.NaN);
        }

        /**
         * Returns the null value for the specified {@code index}, or the specified default value if the index is not
         * present or the value is not a double value.
         *
         * @param index the index
         * @param defaultValue the default value
         * @return the value
         */
        public double getOptionalDoubleValue(int index, double defaultValue) {
            var value = this.value.get(index);
            if (value == null) {
                return defaultValue;
            } else if (value.isDouble()) {
                return value.internalAsDoubleValue();
            } else if (value.isLong()) {
                return value.internalAsLongValue();
            } else if (value.isString())  {
                return BufferNumberUtils.fastParseAsDouble(value.internalAsStringValue(), Double.NaN);
            } else {
                return defaultValue;
            }
        }

        /**
         * Returns the boolean value for the specified {@code index}.
         *
         * @param index the index
         * @return the value
         * @throws IllegalArgumentException if the value is not a boolean value
         */
        public boolean getBoolValue(int index) {
            var value = this.value.get(index);
            if (value == null) {
                throw new IllegalArgumentException("invalid bool value index: " + index);
            } else if (value.isBool())  {
                return value.internalAsBoolValue();
            } else if (value.isString()) {
                return value.internalAsStringValue().equals(TRUE);
            } else {
                throw new IllegalArgumentException("invalid bool value index: " + index);
            }
        }

        /**
         * Returns the boolean value for the specified {@code index}, or false if the index is not present or the value
         * is not a boolean value.
         *
         * @param index the index
         * @return the value
         */
        public boolean getOptionalBoolValue(int index) {
            return getOptionalBoolValue(index, false);
        }

        /**
         * Returns the boolean value for the specified {@code index}, or the specified default value if the index is not
         * present or the value is not a boolean value.
         *
         * @param index the index
         * @param defaultValue the default value
         * @return the value
         */
        public boolean getOptionalBoolValue(int index, boolean defaultValue) {
            var value = this.value.get(index);
            if (value == null) {
                return defaultValue;
            } else if (value.isBool())  {
                return value.internalAsBoolValue();
            } else if (value.isString()) {
                return value.internalAsStringValue().equals(TRUE);
            } else {
                return defaultValue;
            }
        }

        /**
         * Returns the list value for the specified {@code index}.
         *
         * @param index the index
         * @return the value
         * @throws IllegalArgumentException if the value is not a list value
         */
        public List<Value> getListValue(int index) {
            var value = this.value.get(index);
            if (value == null) {
                throw new IllegalArgumentException("invalid list value index: " + index);
            } else if (value.isList()) {
                return value.internalAsListValue();
            } else {
                throw new IllegalArgumentException("invalid list value index: " + index);
            }
        }

        /**
         * Returns the list value for the specified {@code index}, or null if the index is not present or the value is
         * not a list value.
         *
         * @param index the index
         * @return the value
         */
        public List<Value> getOptionalListValue(int index) {
            var value = this.value.get(index);
            if (value == null) {
                return null;
            } else if (value.isList()) {
                return value.internalAsListValue();
            } else {
                return null;
            }
        }

        /**
         * Returns the list value for the specified {@code index}.
         *
         * @param index the index
         * @return the value
         * @throws IllegalArgumentException if the value is not a list value
         */
        public ListValue getList(int index) {
            var value = this.value.get(index);
            if (value == null) {
                throw new IllegalArgumentException("invalid list index: " + index);
            } else if (value.isList()) {
                return value.internalAsList();
            } else {
                throw new IllegalArgumentException("invalid list index: " + index);
            }
        }

        /**
         * Returns the list value for the specified {@code index}, or null if the index is not present or the value is
         * not a list value.
         *
         * @param index the index
         * @return the value
         */
        public ListValue getOptionalList(int index) {
            var value = this.value.get(index);
            if (value == null) {
                return null;
            } else if (value.isList()) {
                return value.internalAsList();
            } else {
                return null;
            }
        }

        /**
         * Returns the map value for the specified {@code index}.
         *
         * @param index the index
         * @return the value
         * @throws IllegalArgumentException if the value is not a map value
         */
        public Map<DirectBuffer, Value> getMapValue(int index) {
            var value = this.value.get(index);
            if (value == null) {
                throw new IllegalArgumentException("invalid map value index: " + index);
            } else if (value.isMap()) {
                return value.internalAsMapValue();
            } else {
                throw new IllegalArgumentException("invalid map value index: " + index);
            }
        }

        /**
         * Returns the map value for the specified {@code index}, or null if the index is not present or the value is
         * not a map value.
         *
         * @param index the index
         * @return the value
         */
        public Map<DirectBuffer, Value> getOptionalMapValue(int index) {
            var value = this.value.get(index);
            if (value == null) {
                return null;
            } else if (value.isMap()) {
                return value.internalAsMapValue();
            } else {
                return null;
            }
        }

        /**
         * Returns the map value for the specified {@code index}.
         *
         * @param index the index
         * @return the value
         * @throws IllegalArgumentException if the value is not a map value
         */
        public MapValue getMap(int index) {
            var value = this.value.get(index);
            if (value == null) {
                throw new IllegalArgumentException("invalid map index: " + index);
            } else if (value.isMap()) {
                return value.internalAsMap();
            } else {
                throw new IllegalArgumentException("invalid map index: " + index);
            }
        }

        /**
         * Returns the map value for the specified {@code index}, or null if the index is not present or the value is
         * not a map value.
         *
         * @param index the index
         * @return the value
         */
        public MapValue getOptionalMap(int index) {
            var value = this.value.get(index);
            if (value == null) {
                return null;
            } else if (value.isMap()) {
                return value.internalAsMap();
            } else {
                return null;
            }
        }

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.openList();
            for (var item : value) {
                encoder.object(item);
            }
            encoder.closeList();
        }

        @Override
        public String toString() {
            return toEncodedString();
        }
    }

    private class BoolValue extends Value implements Encodable {

        boolean value;

        BoolValue() {
            super();
        }

        @Override
        public ValueType getType() {
            return ValueType.BOOLEAN;
        }

        @Override
        void clear() {
            parent = null;
            value = false;
            boolPool.returnObject(this);
        }

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.bool(value);
        }

        @Override
        public String toString() {
            return toEncodedString();
        }
    }

    private class NullValue extends Value implements Encodable {

        NullValue() {
            super();
        }

        Object getValue() {
            return null;
        }

        @Override
        public ValueType getType() {
            return ValueType.NULL;
        }

        @Override
        void clear() {
            parent = null;
            nullPool.returnObject(this);
        }

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.object(getValue());
        }

        @Override
        public String toString() {
            return toEncodedString();
        }
    }

    private class LongValue extends Value implements Encodable {

        long value;

        LongValue() {
            super();
        }

        @Override
        public ValueType getType() {
            return ValueType.LONG;
        }

        @Override
        void clear() {
            parent = null;
            value = 0;
            longPool.returnObject(this);
        }

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.number(value);
        }

        @Override
        public String toString() {
            return toEncodedString();
        }
    }

    private class DoubleValue extends Value implements Encodable {

        double value;

        DoubleValue() {
            super();
        }

        @Override
        public ValueType getType() {
            return ValueType.DOUBLE;
        }

        @Override
        void clear() {
            parent = null;
            value = 0;
            doublePool.returnObject(this);
        }

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.number(value);
        }

        @Override
        public String toString() {
            return toEncodedString();
        }
    }

    private class StringValue extends Value implements Encodable {

        final DirectBuffer value;

        StringValue() {
            super();
            value = BufferUtils.emptyBuffer();
        }

        void setValue(DirectBuffer buffer, int offset, int length) {
            value.wrap(buffer, offset, length);
        }

        @Override
        public ValueType getType() {
            return ValueType.STRING;
        }

        @Override
        void clear() {
            parent = null;
            value.wrap(0, 0);
            stringPool.returnObject(this);
        }

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.string(value);
        }

        @Override
        public String toString() {
            return toEncodedString();
        }
    }
}
