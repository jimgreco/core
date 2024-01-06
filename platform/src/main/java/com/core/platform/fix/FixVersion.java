package com.core.platform.fix;

import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.DirectBuffer;

/**
 * The FIX protocol version.
 */
public enum FixVersion {

    /**
     * FIX 4.2.
     */
    FIX42("FIX.4.2"),
    /**
     * FIX 4.4.
     */
    FIX44("FIX.4.4");

    private final DirectBuffer version;

    FixVersion(String version) {
        this.version = BufferUtils.fromAsciiString(version);
    }

    /**
     * Returns value of the of the BeginString[8].
     *
     * @return value of the of the BeginString[8]
     */
    public DirectBuffer getBeginString() {
        return version;
    }
}
