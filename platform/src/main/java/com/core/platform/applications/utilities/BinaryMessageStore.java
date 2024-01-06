package com.core.platform.applications.utilities;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.io.FileChannel;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.messages.Decoder;
import com.core.platform.bus.BusClient;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * The {@code BinaryMessageStore} application stores all messages to a file.
 */
public class BinaryMessageStore {

    private final Log log;
    private final BusClient<?, ?> busClient;
    private final Path logDirectory;
    private final MutableDirectBuffer lengths;
    private boolean open;
    private FileChannel fileChannel;

    /**
     * Constructs a {@code BinaryMessageStore} from the specified parameters.
     *
     * <p>The specified {@code dispatcher} is used to subscribe to all messages so that they may be copied to the
     * specified message store.
     *
     * @param logFactory a factory for creating logs
     * @param busClient the bus client
     * @param logDirectory the log directory
     */
    public BinaryMessageStore(
            LogFactory logFactory,
            BusClient<?, ?> busClient,
            Path logDirectory) {
        Objects.requireNonNull(logFactory, "logFactory is null");
        this.busClient = Objects.requireNonNull(busClient, "busClient is null");
        this.logDirectory = Objects.requireNonNull(logDirectory, "logDirectory is null");

        log = logFactory.create(BinaryMessageStore.class);
        lengths = BufferUtils.allocate(Short.BYTES);

        busClient.addOpenSessionListener(this::onOpenSession);
        busClient.getDispatcher().addListenerBeforeDispatch(this::onMessage);
    }

    private void onOpenSession() {
        try {
            var sessionName = busClient.getSession();
            OpenOption[] options = {
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            };
            fileChannel = new FileChannel(logDirectory.resolve(sessionName + ".events.dat"), options);
            open = true;
        } catch (IOException e) {
            log.error().append("cannot write to file: ").append(e).commit();
            open = false;
        }
    }

    private void onMessage(Decoder decoder) {
        try {
            if (open) {
                lengths.putShort(0, (short) decoder.length());
                fileChannel.write(lengths);
                fileChannel.write(decoder.buffer(), decoder.offset(), decoder.length());
            }
        } catch (IOException e) {
            log.error().append("cannot write to file: ").append(e).commit();
            open = false;
        }
    }
}
