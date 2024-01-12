package com.core.platform.bus.mold;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Directory;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.log.LogFactory;
import com.core.platform.activation.Activatable;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * The {@code MoldRepeater} application services rewind requests over the MoldUDP64 protocol.
 *
 * <p>The MoldUDP64 protocol is located on the
 * <a href="https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/moldudp64.pdf">Nasdaq
 * website</a>
 *
 * <h2>Activation</h2>
 *
 * <p>The application has the following activation dependencies:
 * <ul>
 *     <li>the Mold session has been defined
 *     <li>the Discovery channel socket is open
 *     <li>the Rewind channel socket is open
 * </ul>
 *
 * <p>On activation, the application will:
 * <ul>
 *     <li>service discovery requests
 *     <li>service rewind requests
 * </ul>
 *
 * <p>On deactivation, the application will:
 * <ul>
 *     <li>stop servicing discovery requests
 *     <li>stop servicing rewind requests
 * </ul>
 */
public class MoldRepeater implements Activatable, Encodable {

    private final MoldRewinder rewinder;
    @Directory(path = ".")
    private final Activator activator;

    /**
     * Creates a {@code MoldRepeater} from the specified parameters.
     * An empty index file and a message file are created with the specified {@code name}.
     *
     * @param selector the NIO selector
     * @param logFactory the factory to create logs
     * @param activatorFactory the factory to create activators
     * @param name a unique name for the rewinder
     * @param busClient the bus client
     * @param address the multicast address to listen to discovery requests on
     * @throws IOException if an I/O error occurs creating the index or message file
     */
    public MoldRepeater(
            Selector selector, LogFactory logFactory, ActivatorFactory activatorFactory,
            String name, MoldBusClient<?, ?> busClient, String address)
                throws IOException {
        var messageStore = new FileChannelMessageStore(Path.of("."));
        messageStore.open(BufferUtils.fromAsciiString(name));

        rewinder = new MoldRewinder(
                "MoldRewinder:" + name, selector, logFactory, activatorFactory,
                busClient.getMoldSession(), messageStore, address);
        activator = activatorFactory.createActivator(name, this, rewinder);
        activator.ready();

        var lengths = new int[1];
        busClient.getDispatcher().addListenerBeforeDispatch(decoder -> {
            try {
                var buffer = messageStore.acquire();
                buffer.putShort(0, (short) decoder.length());
                buffer.putBytes(Short.BYTES, decoder.buffer(), decoder.offset(), decoder.length());
                lengths[0] = Short.BYTES + decoder.length();
                messageStore.commit(lengths, 0, 1);
            } catch (IOException e) {
                activator.notReady("I/O error");
            }
        });
    }

    @Override
    public void activate() {
        activator.ready();
    }

    @Override
    public void deactivate() {
        activator.notReady();
    }

    @Command(path = "status", readOnly = true)
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.object(rewinder);
    }
}
