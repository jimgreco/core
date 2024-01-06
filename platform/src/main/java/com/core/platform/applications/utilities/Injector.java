package com.core.platform.applications.utilities;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Directory;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.messages.Provider;
import com.core.platform.activation.Activatable;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.applications.EntityDataRepository;
import com.core.platform.bus.BusClient;
import com.core.platform.shell.BufferCaster;

import java.util.Arrays;
import java.util.Objects;

/**
 * The {@code Injector} is an application that provides a command, {@code send}, that can dynamically build and send
 * core messages.
 *
 * <h2>Activation</h2>
 *
 * <p>The application has the following activation dependencies:
 * <ul>
 *     <li>the message publisher is ready to publish
 * </ul>
 *
 * <p>On activation, the application will:
 * <ul>
 *     <li>set itself as ready
 * </ul>
 *
 * <p>On deactivation, the application will:
 * <ul>
 *     <li>set itself as not ready
 * </ul>
 */
public class Injector implements Activatable {

    /**
     * The activator for this application.
     */
    @Directory(path = ".")
    protected final Activator activator;
    /**
     * The message provider for this application.
     */
    protected final Provider provider;

    private final BufferCaster bufferCaster;
    @Directory(path = ".")
    private final EntityDataRepository entityDataRepository;

    /**
     * Creates an {@code Injector} with the specified parameters.
     * An activator is created with the specified application name and set to ready.
     *
     * @param logFactory a factory to create logs
     * @param activatorFactory a factory of activators
     * @param busClient the bus client
     * @param applicationName the name of this application
     */
    public Injector(
            LogFactory logFactory, ActivatorFactory activatorFactory,
            BusClient<?, ?> busClient, String applicationName) {
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(activatorFactory, "activatorFactory is null");
        Objects.requireNonNull(busClient, "busClient is null");
        Objects.requireNonNull(applicationName, "applicationName is null");

        provider = busClient.getProvider(applicationName, this);
        bufferCaster = new BufferCaster();
        entityDataRepository = new EntityDataRepository(logFactory, busClient);

        activator = activatorFactory.createActivator(applicationName, this, provider);
    }

    @Override
    public void activate() {
        activator.ready();
    }

    @Override
    public void deactivate() {
        activator.notReady();
    }

    /**
     * Sends a message that is dynamically built from the specified name and field arguments.
     * Field arguments are key/value pairs which represent the name of the field and the ASCII string representation of
     * the field value.
     * For each field argument, the ASCII string value is converted into the primary key value if the field is a foreign
     * key, a fixed-point decimal if the field has implied decimals, or into an object using the {@link BufferCaster}.
     * The converted object is then encoded into the message.
     *
     * @param messageName the name of the message
     * @param fieldArgs the field arguments in the format: field1=value1 field2=value2, ..., fieldN=valueN
     * @throws IllegalArgumentException if the field is unknown
     */
    @Command
    public void send(String messageName, String... fieldArgs) {
        var encoder = provider.getEncoder(messageName);

        for (var fieldArg : fieldArgs) {
            var keyValue = fieldArg.split("=");
            if (keyValue.length != 2) {
                throw new IllegalArgumentException("invalid field: " + fieldArg);
            }
            var fieldName = keyValue[0];
            var value = keyValue[1];
            var field = encoder.field(fieldName);

            if (field.isForeignKey()) {
                var key = Arrays.asList(value.split("/"));
                var primaryKey = entityDataRepository.getPrimaryKey(field.getForeignKey(), key);
                if (primaryKey != 0) {
                    if (field.getType() == byte.class) {
                        encoder.set(field.getName(), (byte) primaryKey);
                    } else if (field.getType() == short.class) {
                        encoder.set(field.getName(), (short) primaryKey);
                    } else {
                        encoder.set(field.getName(), primaryKey);
                    }
                }
            } else {
                var bufferValue = BufferUtils.fromAsciiString(value);
                var objectValue = bufferCaster.cast(bufferValue, field.getType());
                encoder.set(field.getName(), objectValue);
            }
        }

        encoder.commit()
                .send();
    }
}
