package com.core.platform.fix;

import org.agrona.DirectBuffer;

/**
 * A FIX enumerator represents a set of values for a field.
 */
public interface FixEnum<T extends Enum<T>> {

    /**
     * Returns the FIX enum value.
     *
     * @return the FIX enum value
     */
    DirectBuffer getValue();
}
