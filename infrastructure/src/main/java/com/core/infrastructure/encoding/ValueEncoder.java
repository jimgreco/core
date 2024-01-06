package com.core.infrastructure.encoding;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

interface ValueEncoder {

    boolean isMachineReadable();

    int writeOpenMap(MutableDirectBuffer buffer, int index, int level);

    int writeCloseMap(MutableDirectBuffer buffer, int index, int level);

    int writeOpenList(MutableDirectBuffer buffer, int index, int level);

    int writeCloseList(MutableDirectBuffer buffer, int index, int level);

    int writePreFirstElement(MutableDirectBuffer buffer, int index);

    int writeNextElementSeparator(MutableDirectBuffer buffer, int index, int elementIndex);

    int writeKeyValueSeparator(MutableDirectBuffer buffer, int index);

    int writePreString(MutableDirectBuffer buffer, int index, boolean key);

    int writePostString(MutableDirectBuffer buffer, int index, boolean key);

    int writeString(MutableDirectBuffer buffer, int index, String value, boolean key);

    int writeString(MutableDirectBuffer buffer, int index, DirectBuffer value, int offset, int length, boolean key);

    int writeString(MutableDirectBuffer buffer, int index, char character, boolean key);

    int writeNumber(MutableDirectBuffer buffer, int index, long number);

    int writeNumber(MutableDirectBuffer buffer, int index, double number);

    int writeNumber(MutableDirectBuffer buffer, int index, double number, int minDecimals);

    int writeNumber(MutableDirectBuffer buffer, int index, double number, int minDecimals, int maxDecimals);

    int writeNull(MutableDirectBuffer buffer, int index);

    int writeBoolean(MutableDirectBuffer buffer, int index, boolean bool);
}
