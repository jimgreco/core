package com.core.infrastructure.buffer;

import org.agrona.DirectBuffer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

@SuppressWarnings({ "checkstyle:MissingJavadocType", "PMD.OverrideBothEqualsAndHashcode" })
public class ExpandableArrayBuffer extends org.agrona.ExpandableArrayBuffer {

    public ExpandableArrayBuffer() {
        super();
    }

    public ExpandableArrayBuffer(int initialCapacity) {
        super(initialCapacity);
    }

    @Override
    public int compareTo(DirectBuffer that) {
        final int thisCapacity = capacity();
        final int thatCapacity = that.capacity();

        for (int i = 0, length = Math.min(thisCapacity, thatCapacity); i < length; i++) {
            final int cmp = Byte.compare(getByte(i), that.getByte(i));
            if (0 != cmp) {
                return cmp;
            }
        }

        if (thisCapacity != thatCapacity) {
            return thisCapacity - thatCapacity;
        }

        return 0;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        if (obj instanceof final ExpandableArrayBuffer that) {
            return Arrays.equals(byteArray(), that.byteArray());
        } else if (obj instanceof final DirectBuffer that) {
            if (capacity() != that.capacity()) {
                return false;
            }

            for (var i = 0; i < capacity(); i++) {
                if (getByte(i) != that.getByte(i)) {
                    return false;
                }
            }

            return true;
        } else if (obj instanceof final String that) {
            if (capacity() != that.length()) {
                return false;
            }

            for (var i = 0; i < capacity(); i++) {
                if (getByte(i) != that.charAt(i)) {
                    return false;
                }
            }

            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return new String(byteArray(), StandardCharsets.US_ASCII);
    }
}
