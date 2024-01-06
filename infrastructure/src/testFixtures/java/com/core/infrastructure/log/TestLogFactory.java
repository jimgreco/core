package com.core.infrastructure.log;

import com.core.infrastructure.io.SysoutChannel;
import com.core.infrastructure.time.SystemTime;

import java.io.IOException;

/**
 * A {@code LogFactory} designed for use in unit testing.
 */
public class TestLogFactory extends LogFactory {

    /**
     * Creates a {@code TestLogFactory} with a {@code SystemTime} source that writes to a
     * {@code System.out} log sink with a single log identifier, "TEST".
     */
    public TestLogFactory() {
        super(new SystemTime());
        setLogIdentifier(0, "TEST");
        try {
            logSink(new ChannelLogSink(new SysoutChannel()));
            setDebugForAll(true);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
