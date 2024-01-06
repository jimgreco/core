package com.core.platform.bus.mold;

import com.core.infrastructure.io.DirectBufferChannel;
import com.core.infrastructure.io.SeekableBufferChannel;
import org.agrona.DirectBuffer;

/**
 * A {@code ChannelEventStore} that uses two {@code DirectBufferChannel}s for the index and events files.
 */
public class BufferChannelMessageStore extends ChannelMessageStore {

    /**
     * Creates an empty {@code BufferChannelMessageStore}.
     */
    public BufferChannelMessageStore() {
        super();
    }

    @Override
    protected SeekableBufferChannel createIndexFile(DirectBuffer sessionName) {
        return new DirectBufferChannel();
    }

    @Override
    protected SeekableBufferChannel createMessageFile(DirectBuffer sessionName) {
        return new DirectBufferChannel();
    }
}
