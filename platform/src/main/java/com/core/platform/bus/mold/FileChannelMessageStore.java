package com.core.platform.bus.mold;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.io.SeekableBufferChannel;
import com.core.infrastructure.io.FileChannel;
import org.agrona.DirectBuffer;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * A {@code ChannelEventStore} that uses two {@code NioFileChannel}s for the index and events files.
 *
 * <p>The constructor parameter specifies the root directory where the files are stored.
 * The events file will be stored at: {@code rootDirectory/sessionName.events}.
 * The index file will be stored at: {@code rootDirectory/sessionName.index}.
 * Where {@code sessionName} is the name of the session.
 */
public class FileChannelMessageStore extends ChannelMessageStore {

    private final Path logDirectory;
    private final OpenOption[] openOptions;

    /**
     * Creates a {@code NioFileEventStore} with the root directory of the index and events file.
     *
     * @param logDirectory the directory of the index and events file
     */
    public FileChannelMessageStore(Path logDirectory) {
        super();
        this.logDirectory = Objects.requireNonNull(logDirectory, "logDirectory is null");
        openOptions = new OpenOption[] {
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        };
    }

    @Override
    protected SeekableBufferChannel createIndexFile(DirectBuffer sessionName) throws IOException {
        var session = BufferUtils.toAsciiString(sessionName);
        return new FileChannel(logDirectory.resolve(session + ".index.dat"), openOptions);
    }

    @Override
    protected SeekableBufferChannel createMessageFile(DirectBuffer sessionName) throws IOException {
        var session = BufferUtils.toAsciiString(sessionName);
        return new FileChannel(logDirectory.resolve(session + ".events.dat"), openOptions);
    }
}
