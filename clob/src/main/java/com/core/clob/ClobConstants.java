package com.core.clob;

import com.core.infrastructure.encoding.FixedPointValueEncoder;

/**
 * Constants used by the CLOB applications.
 */
public class ClobConstants {

    /**
     * The fixed-point encoder for quantities.
     */
    public static final FixedPointValueEncoder QTY_ENCODER = new FixedPointValueEncoder()
            .setImpliedDecimals(0).setMinDecimals(0);

    /**
     * The fixed-point encoder for price.
     */
    public static final FixedPointValueEncoder PRICE_ENCODER = new FixedPointValueEncoder()
            .setImpliedDecimals(4).setMinDecimals(2);
}
