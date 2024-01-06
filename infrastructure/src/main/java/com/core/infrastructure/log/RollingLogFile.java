package com.core.infrastructure.log;

import com.core.infrastructure.command.Directory;
import com.core.infrastructure.io.FileChannel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;

/**
 * The {@code RollingLogFile} initializes a {@code FileChannel} at the {@code file} subdirectory with the log file to
 * write to.
 * The path of the log file is specified in the constructor.
 * If the file exists and the constructor specifies append mode, then the existing log file is appended to.
 * If the file exists and the constructor specifies truncate mode, the existing log file is copied to a new file with
 * the date and time appended to the file name, and a new empty file is created in place of the existing log file.
 */
public class RollingLogFile {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .appendLiteral('-')
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .toFormatter();

    @SuppressWarnings("PMD.UnusedPrivateField")
    @Directory(path = "file")
    private final FileChannel file;

    /**
     * Constructs a {@code RollingLogFile}.
     * If the {@code appendLogFile} value is set to true then the existing log file will be appended to.
     * If the {@code appendLogFile} value is set to false then the existing log file will be copied to a new file,
     * with the date and time appended to the file name (i.e., "vm02.log" will become "vm02-20210504-093000.log").
     *
     * @param logFileName the path of the log file
     * @param appendLogFile true if an existing log file with the same {@code logFileName} is appended to
     * @throws IOException if an I/O exception occurs
     */
    public RollingLogFile(String logFileName, boolean appendLogFile) throws IOException {
        var path = Path.of(logFileName);

        if (appendLogFile) {
            file = new FileChannel(
                    path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } else {
            if (path.toFile().exists()) {
                var timestampStr = TIMESTAMP_FORMATTER.format(LocalDateTime.now());
                var fileName = path.getFileName().toString();
                var extIndex = fileName.indexOf('.');
                String newFileName;
                if (extIndex == -1) {
                    newFileName = fileName + "_" + timestampStr;
                } else {
                    newFileName = fileName.substring(0, extIndex) + "_" + timestampStr + fileName.substring(extIndex);
                }
                var newFilePath = path.getParent().resolve(newFileName);
                Files.copy(path, newFilePath, StandardCopyOption.COPY_ATTRIBUTES);
            }

            file = new FileChannel(
                    path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        }
    }

    /**
     * Get the file channel.
     *
     * @return the file channel
     */
    public FileChannel getFile() {
        return this.file;
    }
}
