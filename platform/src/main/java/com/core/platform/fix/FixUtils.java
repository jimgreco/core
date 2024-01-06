package com.core.platform.fix;

import com.core.infrastructure.log.Log;
import org.agrona.DirectBuffer;

/**
 * Utilities for working with FIX messages.
 */
public final class FixUtils {

    /**
     * The FIX field separator (0x01).
     */
    public static final byte SOH = 1;
    /**
     * The default value to specify on the Logon[A] HeartBtInt[108] field.
     */
    public static final int DEFAULT_HEART_BT_INT = 30;
    /**
     * A printable substitute for the FIX field separator ('|').
     */
    static final char SOH_PRINT = '|';

    private FixUtils() {
    }

    /**
     * Logs the specified FIX message.
     *
     * @param logStatement the log statement
     * @param fixMsg the FIX message buffer to log
     * @return the log statement
     */
    public static Log.Statement logFix(Log.Statement logStatement, FixMsg fixMsg) {
        return logFix(logStatement, fixMsg.getBuffer());
    }

    static Log.Statement logFix(Log.Statement logStatement, DirectBuffer fixMsg) {
        for (var i = 0; i < fixMsg.capacity(); i++) {
            var theByte = fixMsg.getByte(i);
            logStatement.append(theByte == SOH ? SOH_PRINT : (char) theByte);
        }
        return logStatement;
    }

    /**
     * Converts the specified FIX message buffer into a string.
     * The SHO (0x01) character is replaced by the pipe ('1') character.
     *
     * @param fixMsg the FIX message buffer
     * @return the FIX string
     */
    public static String toFixString(DirectBuffer fixMsg) {
        return toFixString(fixMsg, 0, fixMsg.capacity());
    }

    static String toFixString(DirectBuffer fixMsg, int offset, int length) {
        var string = new StringBuilder();
        for (var i = offset; i < offset + length; i++) {
            var theByte = fixMsg.getByte(i);
            string.append(theByte == SOH ? SOH_PRINT : (char) theByte);
        }
        return string.toString();
    }
}
