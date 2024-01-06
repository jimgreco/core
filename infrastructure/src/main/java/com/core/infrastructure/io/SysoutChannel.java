package com.core.infrastructure.io;

/**
 * A {@code WritableBufferChannel} that writes to {@code System.out}.
 */
public class SysoutChannel extends PrintStreamChannel {

    /**
     * Creates the channel with {@code System.out}.
     */
    public SysoutChannel() {
        super(System.out);
    }
}
