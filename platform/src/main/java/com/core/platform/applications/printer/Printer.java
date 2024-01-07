package com.core.platform.applications.printer;

import com.core.infrastructure.MemoryUnit;
import com.core.infrastructure.buffer.BufferNumberUtils;
import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreMap;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Directory;
import com.core.infrastructure.command.Preferred;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.FileChannel;
import com.core.infrastructure.io.WritableBufferChannel;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.messages.Decoder;
import com.core.infrastructure.messages.Field;
import com.core.infrastructure.messages.Schema;
import com.core.infrastructure.time.DatestampUtils;
import com.core.infrastructure.time.TimestampDecimals;
import com.core.infrastructure.time.TimestampFormatter;
import com.core.platform.applications.EntityDataRepository;
import com.core.platform.bus.BusClient;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.eclipse.collections.api.map.primitive.MutableShortObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.ShortObjectHashMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The printer application prints the binary core messages as ASCII text to a buffer channel (typically a file).
 *
 * <p>Each message is printed in the following format:
 * {@code timestamp messageName[messageType(schemaVersion)] contributorName[contributorSequenceNumber]
 * field1=value1 field2=value2 ... fieldN=valueN}
 * The {@code schemaVersion} is not printed if the schemaVersion is 1.
 *
 * <p>The default formatting for field values varies by field type:
 * <ul>
 *     <li>byte, short, int, long: the ASCII string representation of the integer number
 *     <li>enum: the name of the enumeration set value
 *     <li>{@code DirectBuffer}: the ASCII string representation of the buffer
 * </ul>
 *
 * <p>If the field contains metadata then the field is formatted as follows:
 * <ul>
 *     <li>timestamp: the value is formatted as date-time {@code yyyy-MM-ddTHH:mm:ss.SSS[ZoneOffset]} with the value
 *         representing the number of nanoseconds since Unix epoch
 *     <li>datestamp: the value is formatted as date-time {@code yyyy-MM-dd} with the value representing the number of
 *         days since Unix epoch
 * </ul>
 *
 * <p>If the field is a copy of a command that was rejected by the Sequencer then the entire field recursively as it's
 * own message.
 *
 * <p>Alternatively, a custom formatter can be specified using a {@link FieldFormatter} with the
 * {@link #setFormatter(String, FieldFormatter) setFormatter} method.
 *
 * <p>Messages and specific fields can be suppressed from the output using the {@link #deny(String)} command.
 * By default, header fields are disabled from output because they are available in the first part of the printed
 * statement or irrelevant to the message.
 * By default, heartbeat messages are disabled from output.
 */
public class Printer implements Encodable {

    @Directory(path = ".")
    protected final EntityDataRepository entityDataRepository;
    @Property
    private final Set<String> suppressed;
    private final MutableDirectBuffer lineBuffer;
    private final Log log;
    private final MutableShortObjectMap<DirectBuffer> idToName;
    private final String applicationDefinitionMessageName;
    private final String applicationDefinitionNameField;
    private final TimestampFormatter timestampFormatter;
    private final Map<String, FieldFormatter> fieldToFormatterMap;

    private WritableBufferChannel channel;
    @Property(write = true)
    private boolean disabled;

    /**
     * Creates a printer application with the specified parameters.
     *
     * @param logFactory a factory to create logs
     * @param busClient the bus client
     * @param logDirectory the log directory to write the printer to
     */
    @Preferred
    public Printer(
            LogFactory logFactory, BusClient<?, ?> busClient, String logDirectory) {
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(busClient, "busClient is null");
        Objects.requireNonNull(logDirectory, "channel is null");

        busClient.addOpenSessionListener(() -> {
            try {
                var target = Path.of(logDirectory)
                        .resolve(busClient.getSession() + ".printer.log")
                        .toAbsolutePath();
                channel = new FileChannel(
                        target,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                var link = Paths.get(logDirectory, "printer.log");
                if (Files.exists(link) || Files.isSymbolicLink(link)) {
                    Files.delete(link);
                }
                Files.createSymbolicLink(link, target);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        var schema = busClient.getSchema();
        applicationDefinitionMessageName = schema.getApplicationDefinitionMessageName();
        applicationDefinitionNameField = schema.getApplicationDefinitionNameField();

        log = logFactory.create(getClass());
        suppressed = new UnifiedSet<>();
        idToName = new ShortObjectHashMap<>();
        fieldToFormatterMap = new CoreMap<>();
        lineBuffer = BufferUtils.allocate((int) MemoryUnit.KILOBYTES.toBytes(10));
        timestampFormatter = new TimestampFormatter(ZoneId.systemDefault(), TimestampDecimals.MICROSECONDS);
        entityDataRepository = new EntityDataRepository(logFactory, busClient);

        doInit(busClient, schema);
    }

    /**
     * Creates a printer application with the specified parameters.
     *
     * @param logFactory a factory to create logs
     * @param busClient the bus client
     * @param channel the channel to write the printed messages to
     */
    public Printer(
            LogFactory logFactory, BusClient<?, ?> busClient, WritableBufferChannel channel) {
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(busClient, "busClient is null");
        this.channel = Objects.requireNonNull(channel, "channel is null");

        var schema = busClient.getSchema();
        applicationDefinitionMessageName = schema.getApplicationDefinitionMessageName();
        applicationDefinitionNameField = schema.getApplicationDefinitionNameField();

        log = logFactory.create(getClass());
        suppressed = new UnifiedSet<>();
        idToName = new ShortObjectHashMap<>();
        fieldToFormatterMap = new CoreMap<>();
        lineBuffer = BufferUtils.allocate((int) MemoryUnit.KILOBYTES.toBytes(10));
        timestampFormatter = new TimestampFormatter(ZoneId.systemDefault(), TimestampDecimals.MICROSECONDS);
        entityDataRepository = new EntityDataRepository(logFactory, busClient);

        doInit(busClient, schema);
    }

    private void doInit(BusClient<?, ?> busClient, Schema<?, ?> schema) {
        var fields = schema.createDecoder(schema.getMessageNames()[0]).fields();
        for (var field : fields) {
            if (field.isHeader()) {
                deny(field.getName());
            }
        }
        deny(schema.getHeartbeatMessageName());

        busClient.getDispatcher().addListenerBeforeDispatch(this::onBefore);
    }

    /**
     * Un-suppresses a message or field with the specified {@code name} from being printed.
     *
     * @param name the name of the message or field
     */
    @Command
    public void allow(String name) {
        suppressed.remove(name);
    }

    /**
     * Suppresses a message or field with the specified {@code name} from being printed.
     *
     * @param name the name of the message or field
     */
    @Command
    public void deny(String name) {
        suppressed.add(name);
    }

    /**
     * Sets the formatter, to be invoked on fields with the specified field name.
     *
     * @param fieldName the name of the field
     * @param formatter the formatter
     */
    protected void setFormatter(String fieldName, FieldFormatter formatter) {
        fieldToFormatterMap.put(fieldName, formatter);
    }

    private void onBefore(Decoder decoder) {
        try {
            if (disabled || suppressed.contains(decoder.messageName())) {
                return;
            }

            if (decoder.messageName().equals(applicationDefinitionMessageName)) {
                var name = (DirectBuffer) decoder.get(applicationDefinitionNameField);
                if (!idToName.containsKey(decoder.getApplicationId())) {
                    idToName.put(decoder.getApplicationId(), BufferUtils.copy(name));
                }
            }

            var position = timestampFormatter.writeDateTime(lineBuffer, 0, decoder.getTimestamp());
            lineBuffer.putByte(position++, (byte) ' ');
            position += print(decoder, position);
            lineBuffer.putByte(position++, (byte) '\n');

            channel.write(lineBuffer, 0, position);
        } catch (IOException e) {
            disabled = true;
            log.info().append("error writing to printer channel, shutting down: ").append(e).commit();
        }
    }

    /**
     * Prints the specified message.
     *
     * @param decoder the message decoder
     * @param offset the first byte of internal buffer to write
     * @return the number of bytes written
     */
    protected int print(Decoder decoder, int offset) {
        var position = offset;

        position += writeHeader(decoder, position);

        var fields = decoder.fields();
        for (var field : fields) {
            if (!suppressed.contains(field.getName()) && decoder.isPresent(field.getName())) {
                var formatter = fieldToFormatterMap.get(field.getName());
                if (formatter == null) {
                    position += writeFieldName(lineBuffer, position, field);
                    var type = field.getType();
                    if (type == byte.class || type == short.class || type == int.class || type == long.class) {
                        position += writeLong(decoder, position, field);
                    } else if (type == float.class || type == double.class) {
                        position += writeDouble(decoder, position, field);
                    } else {
                        position += writeObject(decoder, position, field);
                    }
                } else {
                    position += formatter.format(decoder, field, lineBuffer, position);
                }
            }
        }

        return position - offset;
    }

    /**
     * Writes the field as {@code " fieldName="}.
     *
     * @param buffer the buffer to write the field to
     * @param offset the first byte of the buffer to write
     * @param field the field to write
     * @return the number of bytes written
     */
    protected int writeFieldName(MutableDirectBuffer buffer, int offset, Field field) {
        var position = offset;
        buffer.putByte(position++, (byte) ' ');
        position += buffer.putStringWithoutLengthAscii(position, field.getName());
        buffer.putByte(position++, (byte) '=');
        return position - offset;
    }

    private int writeHeader(Decoder decoder, int offset) {
        var position = offset;

        position += lineBuffer.putStringWithoutLengthAscii(position, decoder.messageName());
        if (decoder.getSchemaVersion() > 1) {
            lineBuffer.putByte(position++, (byte) '[');
            position += lineBuffer.putIntAscii(position, decoder.getSchemaVersion());
            lineBuffer.putByte(position++, (byte) ']');
        }

        lineBuffer.putByte(position++, (byte) ' ');
        var appId = decoder.getApplicationId();
        var name = idToName.get(appId);
        if (name == null) {
            position += lineBuffer.putIntAscii(position, appId);
        } else {
            lineBuffer.putBytes(position, name, 0, name.capacity());
            position += name.capacity();
        }
        lineBuffer.putByte(position++, (byte) '[');
        position += lineBuffer.putIntAscii(position, decoder.getApplicationSequenceNumber());
        lineBuffer.putByte(position++, (byte) ']');

        return position - offset;
    }

    private int writeLong(Decoder decoder, int offset, Field field) {
        var value = decoder.integerValue(field.getName());
        if ("timestamp".equals(field.getMetadata())) {
            if (value > 0) {
                return timestampFormatter.writeDateTime(lineBuffer, offset, value);
            } else {
                lineBuffer.putByte(offset, (byte) '0');
                return 1;
            }
        } else if ("datestamp".equals(field.getMetadata())) {
            if (value > 0) {
                return DatestampUtils.putAsDate(lineBuffer, offset, value, true);
            } else {
                lineBuffer.putByte(offset, (byte) '0');
                return 1;
            }
        } else if (field.isForeignKey()) {
            var keys = entityDataRepository.getKey(field.getForeignKey(), (int) value);
            if (keys == null) {
                return lineBuffer.putLongAscii(offset, value);
            } else {
                var position = offset;
                for (var i = 0; i < keys.size(); i++) {
                    var key = keys.get(i);
                    if (i != 0) {
                        lineBuffer.putByte(position++, (byte) '/');
                    }
                    position += lineBuffer.putStringWithoutLengthAscii(position, key);
                }
                return position - offset;
            }
        } else {
            return lineBuffer.putLongAscii(offset, value);
        }
    }

    private int writeDouble(Decoder decoder, int offset, Field field) {
        var value = decoder.realValue(field.getName());
        return BufferNumberUtils.putAsAsciiDecimal(lineBuffer, offset, value);
    }

    private int writeObject(Decoder decoder, int offset, Field field) {
        var value = decoder.get(field.getName());
        if (value == null) {
            return lineBuffer.putStringWithoutLengthAscii(offset, "null");
        } else if (field.getType() == DirectBuffer.class) {
            var dbValue = (DirectBuffer) value;
            lineBuffer.putBytes(offset, dbValue, 0, dbValue.capacity());
            return dbValue.capacity();
        } else {
            return lineBuffer.putStringWithoutLengthAscii(offset, value.toString());
        }
    }

    @Override
    @Command(path = "status", readOnly = true)
    public void encode(ObjectEncoder objectEncoder) {
        objectEncoder.openMap()
                .string("channel").object(channel)
                .string("disabled").bool(disabled)
                .string("filters").object(suppressed)
                .closeMap();
    }

    @Override
    public String toString() {
        return toEncodedString();
    }

    /**
     * A specialized formatter for a field.
     */
    protected interface FieldFormatter {

        /**
         * Invoked to format the specified {@code field}.
         * The user should write to the specified {@code buffer} and return the number of bytes written.
         *
         * @param decoder the decoder
         * @param field the field
         * @param buffer the buffer to write to
         * @param offset the first byte of the buffer to write to
         * @return the number of bytes written
         */
        int format(Decoder decoder, Field field, MutableDirectBuffer buffer, int offset);
    }
}
