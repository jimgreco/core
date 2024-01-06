package com.core.platform.applications.refdata;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.collections.CoreMap;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.messages.Field;
import com.core.infrastructure.time.DatestampUtils;
import com.core.platform.bus.BusClient;
import com.core.platform.shell.BufferCaster;
import org.agrona.DirectBuffer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

class CsvReferenceDataSource {

    private final Map<String, Map<String, Field>> messageToFields;
    private final Map<String, String> messageNameToEntityName;
    private final BufferCaster bufferCaster;
    private final Publisher publisher;
    private final Log log;
    private Path path;

    CsvReferenceDataSource(
            LogFactory logFactory,
            BusClient<?, ?> busClient,
            Publisher publisher,
            String... messageNames) {
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(busClient, "busClient is null");
        this.publisher = Objects.requireNonNull(publisher, "publisher is null");
        Objects.requireNonNull(messageNames, "messageName is null");

        log = logFactory.create(getClass());
        path = Path.of(".");
        bufferCaster = new BufferCaster();
        messageToFields = new CoreMap<>();
        messageNameToEntityName = new CoreMap<>();

        for (var messageName : messageNames) {
            var decoder = busClient.getSchema().createDecoder(messageName);
            var nameToFields =  new CoreMap<String, Field>();
            messageToFields.put(messageName, nameToFields);
            messageNameToEntityName.put(messageName, decoder.entityName());
            for (var field : decoder.fields()) {
                if (!field.isHeader() && !field.isPrimaryKey()) {
                    nameToFields.put(field.getName(), field);
                }
            }
        }
    }

    /**
     * Sets the path to load CSV files from.
     *
     * @param path the path to load CSV files from
     */
    @Command
    public void setPath(Path path) {
        this.path = path;
    }

    /**
     * Loads a CSV file at the path specified with {@code setPath} for each message specified in the constructor.
     * Each CSV file name is {@code [messageName].csv}.
     *
     * @throws IOException if a CSV file could not be loaded.
     * @throws IllegalArgumentException if the CSV file is improperly formatted
     */
    @Command
    public void loadFiles() throws IOException {
        for (var messageName : messageToFields.keySet()) {
            loadFile(messageName, Path.of(messageName + ".csv"));
        }
    }

    /**
     * Loads a CSV file at the specified {@code path} for a message with the specified message name.
     * Each row in the CSV file will be converted into an entity object and dispatched to the {@link Publisher}
     * specified in the constructor.
     * No entities will be dispatched if there is an error in parsing hte file.
     *
     * @param messageName the name of the message
     * @param path the CSV file
     * @throws IOException if a CSV file could not be loaded.
     * @throws IllegalArgumentException if the CSV file is improperly formatted
     */
    @Command
    public void loadFile(String messageName, Path path) throws IOException {
        log.info().append(messageName).append(" loading CSV file: ").append(path).commit();

        var nameToFields = messageToFields.get(messageName);
        if (nameToFields == null) {
            throw new IOException("unknown messageName: " + messageName);
        }

        var resolvedFile = this.path.resolve(path);
        if (!resolvedFile.toFile().exists()) {
            throw new IllegalArgumentException("cannot find file: " + resolvedFile);
        }
        var rows = new CoreList<Map<Field, Object>>();

        try (var lines = Files.lines(resolvedFile)) {
            final int[] rowNum = { 0 };
            var columnFields = new CoreList<Field>();
            var requiredFields = nameToFields.values().stream()
                    .filter(Field::isRequired)
                    .collect(Collectors.toSet());

            lines.forEach(line -> {
                var rowFields = new CoreMap<Field, Object>();
                var inQuotes = false;
                var startOfField = 0;
                var fieldIndex = 0;
                var headerRow = rowNum[0] == 0;

                if (log.isDebug()) {
                    log.debug().append(messageName).append(" row ").append(rowNum[0]).append(": ").append(line)
                            .commit();
                }

                for (var i = 0; i < line.length(); i++) {
                    var theChar = line.charAt(i);
                    var lastCharInRow = i == line.length() - 1;

                    if (lastCharInRow && inQuotes && theChar != '"') {
                        throw new IllegalArgumentException("cannot end row in quotes");
                    }

                    var separator = !inQuotes && theChar == ',' || lastCharInRow;
                    if (theChar == '"') {
                        inQuotes = !inQuotes;
                    }

                    if (separator) {
                        // end of field
                        var endOfField = i;
                        if (theChar != ',' && lastCharInRow) {
                            endOfField++;
                        }
                        var stringValue = startOfField == endOfField
                                ? null : line.substring(startOfField, endOfField);
                        if (stringValue != null) {
                            stringValue = stringValue.strip();
                            if (stringValue.startsWith("\"") && stringValue.endsWith("\"")) {
                                stringValue = stringValue.substring(1, stringValue.length() - 1);
                            } else if (stringValue.length() == 0) {
                                stringValue = null;
                            }
                        }
                        startOfField = i + 1;

                        if (headerRow) {
                            // header row
                            if (stringValue == null) {
                                throw new IllegalArgumentException("blank header column");
                            }
                            var field = nameToFields.get(stringValue);
                            if (field == null) {
                                throw new IllegalArgumentException("unknown column: " + stringValue);
                            }
                            columnFields.add(field);
                            requiredFields.remove(field);
                        } else {
                            // data row
                            addRow(rowNum, columnFields, rowFields, fieldIndex++, stringValue);
                            if (theChar == ',' && lastCharInRow) {
                                addRow(rowNum, columnFields, rowFields, fieldIndex++, null);
                            }
                        }
                    }
                }

                if (headerRow) {
                    if (requiredFields.size() > 0) {
                        throw new IllegalArgumentException("missing required nameToFields: "
                                + requiredFields.stream().map(Field::getName)
                                .collect(Collectors.joining(", ")));
                    }
                } else {
                    if (columnFields.size() != fieldIndex) {
                        throw new IllegalArgumentException(
                                "invalid number of columns: row=" + rowNum[0]
                                        + ", headerCols=" + columnFields.size()
                                        + ", rowCols=" + (fieldIndex + 1));
                    }

                    if (log.isDebug()) {
                        log.debug().append(messageName).append(" row ").append(rowNum[0]).append(": ").append(rowFields)
                                .commit();
                    }

                    rows.add(rowFields);
                }

                rowNum[0]++;
            });
        }

        log.info().append(messageName).append(" loaded CSV file: file=").append(path)
                .append(", entities=").append(rows.size())
                .commit();

        for (var row : rows) {
            publisher.onEntityRequest(messageName, messageNameToEntityName.get(messageName), row);
        }

        log.info().append(messageName).append(" processed CSV file: file=").append(path)
                .append(", entities=").append(rows.size())
                .commit();
    }

    private void addRow(
            int[] rowNum, CoreList<Field> columnFields, CoreMap<Field, Object> rowFields,
            int fieldIndex, String stringValue) {
        if (fieldIndex == columnFields.size()) {
            throw new IllegalArgumentException(
                    "invalid number of columns: row=" + rowNum[0]
                            + ", headerCols=" + columnFields.size()
                            + ", rowCols=" + fieldIndex);
        }
        var field = columnFields.get(fieldIndex);

        if (stringValue == null) {
            if (field.isRequired()) {
                throw new IllegalArgumentException(
                        "null required field: row=" + rowNum[0]
                                + ", col=" + fieldIndex
                                + ", colName=" + field.getName());
            }
        } else {
            if (field.getForeignKey() != null) {
                // foreign key fields are added as strings to be resolved later
                rowFields.put(field, Arrays.asList(stringValue.split("\\|")));
            } else if ("datestamp".equals(field.getMetadata())) {
                try {
                    var intValue = Integer.parseInt(stringValue);
                    if (intValue == 0) {
                        rowFields.put(field, 0);
                    } else {
                        var value = DatestampUtils.toEpochDay(intValue);
                        rowFields.put(field, value);
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            "could not convert to datestamp: row=" + rowNum[0]
                                    + ", col=" + fieldIndex
                                    + ", colName=" + field.getName()
                                    + ", value=" + stringValue);
                }
            } else {
                // cast to the specified type
                try {
                    var value = field.getType() == DirectBuffer.class
                            ? stringValue : bufferCaster.cast(
                                    BufferUtils.fromAsciiString(stringValue), field.getType());
                    rowFields.put(field, value);
                } catch (ClassCastException e) {
                    throw new IllegalArgumentException(
                            "could not cast: row=" + rowNum[0]
                                    + ", col=" + fieldIndex
                                    + ", colName=" + field.getName()
                                    + ", value=" + stringValue);
                }
            }
        }
    }
}
