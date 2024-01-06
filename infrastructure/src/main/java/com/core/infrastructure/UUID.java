package com.core.infrastructure;

import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.Arrays;

/**
 * A class that represents a mutable universally unique identifier (UUID).
 * A UUID represents a 128-bit value.
 *
 * <p>There exist different variants of these global identifiers.
 * The methods of this class are for manipulating the Leach-Salz variant, although the constructors allow the creation
 * of any variant of UUID (described below).
 *
 * <p>The layout of a variant 2 (Leach-Salz) UUID is as follows:
 *
 * <p>The most significant long consists of the following unsigned fields:
 * <pre>
 * 0xFFFFFFFF00000000 time_low
 * 0x00000000FFFF0000 time_mid
 * 0x000000000000F000 version
 * 0x0000000000000FFF time_hi
 * </pre>
 * The least significant long consists of the following unsigned fields:
 * <pre>
 * 0xC000000000000000 variant
 * 0x3FFF000000000000 clock_seq
 * 0x0000FFFFFFFFFFFF node
 * </pre>
 *
 * <p>The variant field contains a value which identifies the layout of the {@code UUID}.
 * The bit layout described above is valid only for a {@code UUID} with a variant value of 2, which indicates the
 * Leach-Salz variant.
 *
 * <p>The version field holds a value that describes the type of this {@code UUID}.
 * There are four different basic types of UUIDs: time-based, DCE security, name-based, and randomly generated UUIDs.
 * These types have a version value of 1, 2, 3 and 4, respectively.
 *
 * <p>For more information including algorithms used to create {@code UUID}s, see
 * <a href="http://www.ietf.org/rfc/rfc4122.txt"> <i>RFC&nbsp;4122: A Universally Unique IDentifier (UUID) URN
 * Namespace</i></a>, section 4.2 &quot;Algorithms for Creating a Time-Based UUID&quot;.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class UUID {

    /**
     * The length of a UUID with dashes.
     * For example, ""d50ec984-77a8-460a-b958-66f114b0de9b".
     */
    public static final int UUID_LENGTH_WITH_DASHES = "d50ec984-77a8-460a-b958-66f114b0de9b".length();
    /**
     * The lenght of a UUID without dashes.
     * For example, "d50ec98477a8460ab95866f114b0de9b".
     */
    public static final int UUID_LENGTH_WITHOUT_DASHES = "d50ec98477a8460ab95866f114b0de9b".length();

    private static final byte[] NIBBLES;
    private static final byte[] DIGITS = new byte[] {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };

    static {
        var ns = new byte[256];
        Arrays.fill(ns, (byte) -1);
        ns['0'] = 0;
        ns['1'] = 1;
        ns['2'] = 2;
        ns['3'] = 3;
        ns['4'] = 4;
        ns['5'] = 5;
        ns['6'] = 6;
        ns['7'] = 7;
        ns['8'] = 8;
        ns['9'] = 9;
        ns['A'] = 10;
        ns['B'] = 11;
        ns['C'] = 12;
        ns['D'] = 13;
        ns['E'] = 14;
        ns['F'] = 15;
        ns['a'] = 10;
        ns['b'] = 11;
        ns['c'] = 12;
        ns['d'] = 13;
        ns['e'] = 14;
        ns['f'] = 15;
        NIBBLES = ns;
    }

    private long mostSigBits;
    private long leastSigBits;

    /**
     * Creates a {@code UUID} with the default value of 0.
     */
    public UUID() {
    }

    /**
     * Returns the least significant 64 bits of this UUID's 128 bit value.
     *
     * @return the least significant 64 bits of this UUID's 128 bit value
     */
    public long getLeastSignificantBits() {
        return leastSigBits;
    }

    /**
     * Returns the most significant 64 bits of this UUID's 128 bit value.
     *
     * @return the most significant 64 bits of this UUID's 128 bit value
     */
    public long getMostSignificantBits() {
        return mostSigBits;
    }

    /**
     * Sets the least significant 64 bits of this UUID's 128 bit value.
     *
     * @param leastSigBits the least significant 64 bits of this UUID's 128 bit value
     */
    public void setLeastSignificantBits(long leastSigBits) {
        this.leastSigBits = leastSigBits;
    }

    /**
     * Sets the most significant 64 bits of this UUID's 128 bit value.
     *
     * @param mostSigBits the most significant 64 bits of this UUID's 128 bit value
     */
    public void setMostSignificantBits(long mostSigBits) {
        this.mostSigBits = mostSigBits;
    }

    /**
     * Sets the UUID to 0.
     */
    public void reset() {
        this.leastSigBits = 0;
        this.mostSigBits = 0;
    }

    /**
     * Returns true if the UUID is not 0.
     *
     * @return true if the UUID is not 0
     */
    public boolean isSet() {
        return leastSigBits != 0 && mostSigBits != 0;
    }

    /**
     * Sets the UUID value to the value of the specified {@code uuid}.
     *
     * @param uuid the UUID
     */
    public void fromUUID(UUID uuid) {
        mostSigBits = uuid.mostSigBits;
        leastSigBits = uuid.leastSigBits;
    }

    /**
     * Writes the UUID into the specified {@code buffer} at the specified {@code offset} with the string representation
     * as described in the {@link #toString()} method.
     * If {@code withDashes} is set to true, the UUID will be written with dashes in the 8th, 13th, 18th, and 23rd byte
     * (e.g., "d50ec984-77a8-460a-b958-66f114b0de9b"), otherwise the UUID will be written without dashes (e.g.,
     * "d50ec98477a8460ab95866f114b0de9b").
     *
     * @param buffer the buffer to write to
     * @param offset the first byte of the buffer to write the UUID to
     * @param withDashes true if the UUID is written with dashes
     * @return the number of bytes written
     */
    public int toBuffer(MutableDirectBuffer buffer, int offset, boolean withDashes) {
        if (withDashes) {
            formatUnsignedLong(leastSigBits, buffer, offset + 24, 12);
            buffer.putByte(offset + 23, (byte) '-');
            formatUnsignedLong(leastSigBits >>> 48, buffer, offset + 19, 4);
            buffer.putByte(offset + 18, (byte) '-');
            formatUnsignedLong(mostSigBits, buffer, offset + 14, 4);
            buffer.putByte(offset + 13, (byte) '-');
            formatUnsignedLong(mostSigBits >>> 16, buffer, offset + 9, 4);
            buffer.putByte(offset + 8, (byte) '-');
            formatUnsignedLong(mostSigBits >>> 32, buffer, offset + 0, 8);
            return UUID_LENGTH_WITH_DASHES;
        } else {
            formatUnsignedLong(leastSigBits, buffer, offset + 20, 12);
            formatUnsignedLong(leastSigBits >>> 48, buffer, offset + 16, 4);
            formatUnsignedLong(mostSigBits, buffer, offset + 12, 4);
            formatUnsignedLong(mostSigBits >>> 16, buffer, offset + 8, 4);
            formatUnsignedLong(mostSigBits >>> 32, buffer, offset + 0, 8);
            return UUID_LENGTH_WITHOUT_DASHES;
        }
    }

    private static void formatUnsignedLong(long value, MutableDirectBuffer buffer, int offset, int length) {
        var charPos = offset + length;
        do {
            buffer.putByte(--charPos, DIGITS[((int) value) & 0xF]);
            value >>>= 4;
        } while (charPos > offset);
    }

    /**
     * Sets the {@code UUID} from the string standard representation as described in the {@link #toString} method.
     *
     * @param buffer a buffer that specifies a {@code UUID}
     * @param index the first byte of the buffer containing the {@code UUID}
     * @param length the length of the UUID
     */
    public void fromBuffer(DirectBuffer buffer, int index, int length) {
        if (length == UUID_LENGTH_WITH_DASHES) {
            var ch1 = buffer.getByte(index + 8);
            var ch2 = buffer.getByte(index + 13);
            var ch3 = buffer.getByte(index + 18);
            var ch4 = buffer.getByte(index + 23);
            if (ch1 == '-' && ch2 == '-' && ch3 == '-' && ch4 == '-') {
                var msb1 = parse4Nibbles(buffer, index + 0);
                var msb2 = parse4Nibbles(buffer, index + 4);
                var msb3 = parse4Nibbles(buffer, index + 9);
                var msb4 = parse4Nibbles(buffer, index + 14);
                var lsb1 = parse4Nibbles(buffer, index + 19);
                var lsb2 = parse4Nibbles(buffer, index + 24);
                var lsb3 = parse4Nibbles(buffer, index + 28);
                var lsb4 = parse4Nibbles(buffer, index + 32);
                if ((msb1 | msb2 | msb3 | msb4 | lsb1 | lsb2 | lsb3 | lsb4) >= 0) {
                    mostSigBits = msb1 << 48 | msb2 << 32 | msb3 << 16 | msb4;
                    leastSigBits = lsb1 << 48 | lsb2 << 32 | lsb3 << 16 | lsb4;
                    return;
                }
            }
        } else if (length == UUID_LENGTH_WITHOUT_DASHES) {
            var msb1 = parse4Nibbles(buffer, index + 0);
            var msb2 = parse4Nibbles(buffer, index + 4);
            var msb3 = parse4Nibbles(buffer, index + 8);
            var msb4 = parse4Nibbles(buffer, index + 12);
            var lsb1 = parse4Nibbles(buffer, index + 16);
            var lsb2 = parse4Nibbles(buffer, index + 20);
            var lsb3 = parse4Nibbles(buffer, index + 24);
            var lsb4 = parse4Nibbles(buffer, index + 28);
            if ((msb1 | msb2 | msb3 | msb4 | lsb1 | lsb2 | lsb3 | lsb4) >= 0) {
                mostSigBits = msb1 << 48 | msb2 << 32 | msb3 << 16 | msb4;
                leastSigBits = lsb1 << 48 | lsb2 << 32 | lsb3 << 16 | lsb4;
                return;
            }
        }
        reset();
    }

    private static long parse4Nibbles(DirectBuffer name, int index) {
        var ns = NIBBLES;
        var ch1 = name.getByte(index);
        var ch2 = name.getByte(index + 1);
        var ch3 = name.getByte(index + 2);
        var ch4 = name.getByte(index + 3);
        return (ch1 | ch2 | ch3 | ch4) > 0xff ? -1 : ns[ch1] << 12 | ns[ch2] << 8 | ns[ch3] << 4 | ns[ch4];
    }

    /**
     * Returns a {@code String} object representing this {@code UUID}.
     *
     * <p>The UUID string representation is as described by this BNF:
     * <blockquote><pre>
     * {@code
     * UUID                   = <time_low> "-" <time_mid> "-"
     *                          <time_high_and_version> "-"
     *                          <variant_and_sequence> "-"
     *                          <node>
     * time_low               = 4*<hexOctet>
     * time_mid               = 2*<hexOctet>
     * time_high_and_version  = 2*<hexOctet>
     * variant_and_sequence   = 2*<hexOctet>
     * node                   = 6*<hexOctet>
     * hexOctet               = <hexDigit><hexDigit>
     * hexDigit               =
     *       "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
     *       | "a" | "b" | "c" | "d" | "e" | "f"
     *       | "A" | "B" | "C" | "D" | "E" | "F"
     * }</pre></blockquote>
     *
     * @return a string representation of this {@code UUID}
     */
    @Override
    public String toString() {
        var buffer = BufferUtils.allocate(UUID_LENGTH_WITH_DASHES);
        toBuffer(buffer, 0, true);
        return BufferUtils.toAsciiString(buffer);
    }

    /**
     * Returns a hash code for this {@code UUID}.
     *
     * @return a hash code value for this {@code UUID}
     */
    public int hashCode() {
        var hilo = mostSigBits ^ leastSigBits;
        return ((int) (hilo >> 32)) ^ (int) hilo;
    }

    /**
     * Compares this object to the specified object.
     * The result is {@code true} if and only if the argument is not {@code null}, is a {@code UUID} object, has the
     * same variant, and contains the same value, bit for bit, as this {@code UUID}.
     *
     * @param obj the object to be compared
     *
     * @return {@code true} if the objects are the same; {@code false} otherwise
     */
    public boolean equals(Object obj) {
        if (null == obj || obj.getClass() != UUID.class) {
            return false;
        }
        var id = (UUID) obj;
        return mostSigBits == id.mostSigBits && leastSigBits == id.leastSigBits;
    }
}
