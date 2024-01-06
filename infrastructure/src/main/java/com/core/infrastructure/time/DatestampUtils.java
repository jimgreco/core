package com.core.infrastructure.time;

import org.agrona.MutableDirectBuffer;

import java.time.LocalDate;
import java.time.temporal.ChronoField;

/**
 * Utilities for working with datestamps.
 */
@SuppressWarnings("PMD.UselessParentheses")
public class DatestampUtils {

    private static final int DAYS_PER_CYCLE = 146097;
    /**
     * The number of days from year zero to year 1970.
     * There are five 400 year cycles from year zero to 2000.
     * There are 7 leap years from 1970 to 2000.
     */
    private static final long DAYS_0000_TO_1970 = (DAYS_PER_CYCLE * 5L) - (30L * 365L + 7L);

    /**
     * Converts this date to the Epoch Day.
     *
     * <p>The {@link ChronoField#EPOCH_DAY Epoch Day count} is a simple incrementing count of days where day 0 is
     * 1970-01-01 (ISO).
     * This definition is the same for all chronologies, enabling conversion.
     *
     * @param date the date in yyyyMMdd format
     * @return the number of days since epoch, January 1, 1970
     */
    public static int toEpochDay(int date) {
        var year = date / 10000;
        var month = (date / 100) % 100;
        var day = date % 100;
        return (int) toEpochDay(year, month, day);
    }

    /**
     * Converts this date to the Epoch Day.
     *
     * <p>The {@link ChronoField#EPOCH_DAY Epoch Day count} is a simple incrementing count of days where day 0 is
     * 1970-01-01 (ISO).
     * This definition is the same for all chronologies, enabling conversion.
     *
     * @param year the year
     * @param month the month
     * @param day the day
     * @return the number of days since epoch, January 1, 1970
     * @implNote this method is a copy of {@link LocalDate#toEpochDay()}.
     */
    public static long toEpochDay(int year, int month, int day) {
        long y = year;
        long m = month;
        long total = 0;
        total += 365 * y;
        if (y >= 0) {
            total += (y + 3) / 4 - (y + 99) / 100 + (y + 399) / 400;
        } else {
            total -= y / -4 - y / -100 + y / -400;
        }
        total += ((367 * m - 362) / 12);
        total += day - 1;
        if (m > 2) {
            total--;
            if (isLeapYear(year) == false) {
                total--;
            }
        }
        return total - DAYS_0000_TO_1970;
    }

    private static boolean isLeapYear(long prolepticYear) {
        return ((prolepticYear & 3) == 0) && ((prolepticYear % 100) != 0 || (prolepticYear % 400) == 0);
    }

    /**
     * Puts the specified {@code datestamp} as a human-readable ASCII date into the specified {@code buffer}.
     * The date format is {@code yyyy-MM-dd} or {@code yyyyMMdd} depending on whether {@code dashes} is true or false.
     *
     * @param buffer the buffer
     * @param index the first byte of the buffer to write
     * @param epochDay the datestamp
     * @param dashes true if dashes are added to the date output.
     * @return the number of bytes written
     * @implNote this method is a copy of {@link LocalDate#ofEpochDay(long)}.
     */
    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    public static int putAsDate(MutableDirectBuffer buffer, int index, long epochDay, boolean dashes) {
        ChronoField.EPOCH_DAY.checkValidValue(epochDay);
        long zeroDay = epochDay + DAYS_0000_TO_1970;
        // find the march-based year
        zeroDay -= 60;  // adjust to 0000-03-01 so leap day is at end of four year cycle
        long adjust = 0;
        if (zeroDay < 0) {
            // adjust negative years to positive for calculation
            long adjustCycles = (zeroDay + 1) / DAYS_PER_CYCLE - 1;
            adjust = adjustCycles * 400;
            zeroDay += -adjustCycles * DAYS_PER_CYCLE;
        }
        long yearEst = (400 * zeroDay + 591) / DAYS_PER_CYCLE;
        long doyEst = zeroDay - (365 * yearEst + yearEst / 4 - yearEst / 100 + yearEst / 400);
        if (doyEst < 0) {
            // fix estimate
            yearEst--;
            doyEst = zeroDay - (365 * yearEst + yearEst / 4 - yearEst / 100 + yearEst / 400);
        }
        yearEst += adjust;  // reset any negative year
        int marchDoy0 = (int) doyEst;

        // convert march-based values back to january-based
        int marchMonth0 = (marchDoy0 * 5 + 2) / 153;
        int month = (marchMonth0 + 2) % 12 + 1;
        int dom = marchDoy0 - (marchMonth0 * 306 + 5) / 10 + 1;
        yearEst += marchMonth0 / 10;

        // check year now we are certain it is correct
        int year = ChronoField.YEAR.checkValidIntValue(yearEst);

        var position = index;
        position += buffer.putIntAscii(position, year);
        if (dashes) {
            buffer.putByte(position++, (byte) '-');
        }
        buffer.putNaturalPaddedIntAscii(position, 2, month);
        position += 2;
        if (dashes) {
            buffer.putByte(position++, (byte) '-');
        }
        buffer.putNaturalPaddedIntAscii(position, 2, dom);
        position += 2;
        return position - index;
    }

    /**
     * Returns true if the specified {@code year}, {@code month}, and {@code dayOfMonth} combination represents a valid
     * date.
     *
     * @param year the year
     * @param month the month
     * @param dayOfMonth the day of the month
     * @return true if a valid date
     */
    public static boolean isValidDate(int year, int month, int dayOfMonth) {
        if (year <= 0 || month <= 0 || month > 12 || dayOfMonth <= 0) {
            return false;
        }

        if (dayOfMonth > 28) {
            int dom;
            switch (month) {
                case 2:
                    dom = isLeapYear(year) ? 29 : 28;
                    break;
                case 4:
                case 6:
                case 9:
                case 11:
                    dom = 30;
                    break;
                default:
                    dom = 31;
                    break;
            }
            return dayOfMonth <= dom;
        }
        return true;
    }
}
