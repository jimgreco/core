package com.core.platform.shell;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.MemoryUnit;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.collections.CoreMap;
import com.core.infrastructure.collections.ObjectPool;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.encoding.EncoderUtils;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.FileChannel;
import com.core.infrastructure.io.ReadableBufferChannel;
import com.core.infrastructure.io.WritableBufferChannel;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.metrics.MetricFactory;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * The command shell exposes the VM's services to the user and other programs.
 *
 * <h2>Build-in Commands</h2>
 *
 * <p>The following are the shell's built-in commands.
 * <ul>
 *     <li>cd &lt;path&gt;: change the path of the shell instance
 *     <li>create &lt;path&gt; &lt;class&gt; [args ...]: creates a new instance of the specified class at the specified
 *         path
 *     <li>default &lt;key&gt; &lt;value&gt;: creates a variable with the specified key and value at if a variable with
 *         the same key does not exist
 *     <li>echo [args ...]: echos the arguments back to the user, including the stripping of whitespace and the
 *         expansion of any variables
 *     <li>exit: exists the shell context
 *     <li>ls: lists the directory contents
 *     <li>pwd: returns the current directory
 *     <li>set &lt;key&gt; &lt;value&gt;: creates a variable with the specified key and value
 *     <li>source [-s] &lt;file&gt;: loads the specified file in the same shell or a sub-shell is the "-s" switch is
 *         specified
 * </ul>
 *
 * <h2>Registering commands</h2>
 *
 * <p>Objects created with the {@code create} command are registered at the specified path.
 * The object is recursively searched for fields and methods that are annotated with {@code Command},
 * {@code Property}, and {@code Include}.
 *
 * <p>Fields annotated with {@code Property} can be get and set if the {@code read} and {@code write} parameters of the
 * annotation are set.
 * <ul>
 *     <li>Readable properties register a getter command in a sub-directory of the registered object with the name of
 *         the field (e.g., if the object is registered at "/foo/bar" and the field name is "soo", then the path to the
 *         property is "/foo/bar/soo").
 *         The getter command takes no arguments and returns the value of the field.
 *         The {@code getterPath} annotation parameter can specify an alternative command path for the field.
 *     <li>Writable properties register a setter command in a sub-directory of the registered object with "set" and then
 *         the capitalized name of the field (e.g., if the object is registered at "/foo/bar" and the field name is
 *         "soo", then the path to the property is "/foo/bar/setSoo").
 *         The setter method takes one argument of the field type and has no return value.
 *         The {@code setterPath} annotation parameter can specify an alternative command path for the field.
 * </ul>
 *
 * <p>Methods annotated with {@code Command} register the method in a sub-directory of the register object (e.g., if the
 * object is registered at "/foo/bar" and the method name is "soo", then the path to the method is "/foo/bar/soo").
 * THe method uses reflection to determine the parameters and return values of the method.
 * The path parameter of the annotation can specify an alternative path for the method.
 *
 * <p>Fields annotated with {@code Directory} recursively register the field object in a sub-directory of the registered
 * object (e.g., if the object is registered at "/foo/bar" and the field name is "soo", then the field is registered at
 * "/foo/bar/soo").
 * The method uses reflection to search the included object for its fields and methods as if the object was created
 * using the {@code create} built-in command.
 */
public class Shell {

    private static final DirectBuffer NEW_LINE = BufferUtils.fromAsciiString("\n");
    private static final DirectBuffer SHELL_PROMPT = BufferUtils.fromAsciiString(" % ");

    private final MutableDirectBuffer tempBuffer;
    private final Map<DirectBuffer, DirectBuffer> variables;
    private final Map<Class<?>, Object> impliedParams;
    @Property
    private final ObjectPool<ShellContextImpl> instancePool;

    private final CommandProcessor commandProcessor;
    private final Log log;
    private final CommandRegistry commandRegistry;
    private Path[] filePath;
    private Exception processingException;

    /**
     * Creates a shell with the specified log factory to log the results of shell commands.
     *
     * @param logFactory a factory to create logs
     * @param metricFactory a factory to create metrics
     */
    public Shell(LogFactory logFactory, MetricFactory metricFactory) {
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(metricFactory, "metricFactory is null");

        tempBuffer = BufferUtils.allocateExpandable((int) MemoryUnit.KILOBYTES.toBytes(32));
        log = logFactory.create(getClass());
        impliedParams = new CoreMap<>();
        commandRegistry = new CommandRegistry(logFactory, impliedParams);
        var caster = new BufferCaster();
        commandProcessor = new CommandProcessor(metricFactory, logFactory, caster, commandRegistry, impliedParams);
        variables = new CoreMap<>();
        filePath = new Path[] { Path.of(".") };
        instancePool = new ObjectPool<>(() -> new ShellContextImpl(this));
    }

    /**
     * Adds a listener for new command descriptors.
     * The listener will be invoked with all existing command descriptors with a depth-first search.
     *
     * @param listener the listener
     */
    public void addListener(Consumer<CommandDescriptor> listener) {
        commandRegistry.addObjectListener(listener);
    }

    /**
     * Adds all properties to the shell's variable registry.
     *
     * @param properties the properties to add to the shell's variable registry
     */
    public void addVariables(Properties properties) {
        for (var entry : properties.entrySet()) {
            if (entry.getKey() instanceof String && entry.getValue() instanceof  String) {
                log.info().append("env: ").append(entry.getKey())
                        .append("=").append(entry.getValue())
                        .commit();
                variables.put(
                        BufferUtils.fromAsciiString((String) entry.getKey()),
                        BufferUtils.fromAsciiString((String) entry.getValue()));
            }
        }

        var shellPath = properties.get("SHELL_PATH");
        if (shellPath != null) {
            var stringPath = ((String) shellPath).split(":");
            filePath = new Path[stringPath.length];
            for (var i = 0; i < stringPath.length; i++) {
                filePath[i] = Path.of(stringPath[i]);
            }
        }
    }

    /**
     * Associates the specified {@code object} with the specified class.
     *
     * @param clz the class to associate with the {@code object}
     * @param object the object associated with the class
     * @throws IllegalStateException if the {@code object} couldn't be registered in the specified {@code location}
     */
    public void setImpliedParameter(Class<?> clz, Object object) {
        impliedParams.put(clz, object);
    }

    /**
     * Adds the specified {@code object} to the shell's command registry at the specified {@code path}.
     *
     * @param path the path
     * @param object the object
     * @throws CommandException if the specified {@code path} could not be parsed or an object already exists at the
     *     computed path
     */
    public void addObject(DirectBuffer path, Object object) throws CommandException {
        if (path != null) {
            commandRegistry.addObject(commandRegistry.getRoot(), path, object);
        }
    }

    /**
     * Adds the specified {@code object} to the shell's command registry at the specified {@code path}.
     *
     * @param path the path
     * @param object the object
     * @throws CommandException if the specified {@code path} could not be parsed or an object already exists at the
     *     computed path
     */
    public void addObject(String path, Object object) throws CommandException {
        addObject(BufferUtils.fromAsciiString(path), object);
    }

    /**
     * Adds the specified {@code object} to the shell's command registry at the specified {@code path} relative to the
     * specified {@code parent} object.
     *
     * @param parent the parent object
     * @param path the path
     * @param object the object
     * @throws CommandException if the specified {@code path} could not be parsed, the {@code parent} is not registered,
     *     or an object already exists at the computed path
     */
    public void addObject(Object parent, DirectBuffer path, Object object) throws CommandException {
        var parentCommandDescriptor = commandRegistry.resolve(parent);
        commandRegistry.addObject(parentCommandDescriptor, path, object);
    }

    /**
     * Returns the command descriptor for the specified {@code path}.
     *
     * @param path the path
     * @return the command descriptor
     * @throws CommandException if the {@code path} could not be resolved
     */
    public CommandDescriptor getCommandDescriptor(DirectBuffer path) throws CommandException {
        return commandRegistry.resolve(commandRegistry.getRoot(), path);
    }

    /**
     * Returns the object associated with the specified {@code path}.
     *
     * @param path the path
     * @return the object associated with the {@code path}
     * @throws CommandException if the {@code path} could not be resolved
     */
    public Object getObject(DirectBuffer path) throws CommandException {
        return commandRegistry.resolve(commandRegistry.getRoot(), path).getObject();
    }

    /**
     * Returns the object associated with the specified {@code path}.
     *
     * @param path the path
     * @return the object associated with the {@code path}
     * @throws CommandException if the {@code path} could not be resolved
     */
    public Object getObject(String path) throws CommandException {
        return getObject(BufferUtils.fromAsciiString(path));
    }

    /**
     * Returns the path associated with the specified {@code object}.
     *
     * @param object the object
     * @return the path
     */
    public String getPath(Object object) {
        return commandRegistry.getPath(object);
    }

    /**
     * Returns the property value.
     *
     * @param property the property
     * @return the value
     */
    public DirectBuffer getPropertyValue(DirectBuffer property) {
        return variables.get(property);
    }

    /**
     * Sets the {@code property} to the specified {@code value}.
     *
     * @param property the property
     * @param value the property value
     */
    public void setPropertyValue(String property, String value) {
        variables.put(BufferUtils.fromAsciiString(property), BufferUtils.fromAsciiString(value));
    }

    /**
     * Opens a shell context that will read from the {@code input} and write to the {@code output}.
     * If {@code closeOnError} is set, the shell will close the {@code input} channel on any parsing or command
     * executing error.
     *
     * @param input the channel to read statements from the shell
     * @param output the channel to write the output from the shell, null if there is no output channel
     * @param interactive the shell is interactive and will print a prompt
     * @param closeOnError if the shell should be closed on any parsing or command execution error
     * @param shellContextHandler a handler to receive feedback from the shell context
     * @return the new shell context
     */
    @Command
    public ShellContext open(
            ReadableBufferChannel input,
            WritableBufferChannel output,
            boolean interactive,
            boolean closeOnError,
            ShellContextHandler shellContextHandler) {
        var instance = instancePool.borrowObject();
        instance.start(input, output, interactive, closeOnError,
                commandRegistry.getRoot(), variables, null, null, false,
                shellContextHandler);
        return instance;
    }

    static class ShellContextImpl implements ShellContext {

        private final Map<DirectBuffer, DirectBuffer> variables;
        private final List<DirectBuffer> args;
        private final StatementParser statementParser;

        private final Shell shell;
        private final MutableDirectBuffer readBuffer;
        private final Runnable cachedReadListener;

        private ReadableBufferChannel input;
        private WritableBufferChannel output;

        private ShellContextImpl parentShell;
        private boolean subShell;

        private boolean interactive;
        private boolean closeOnError;
        private boolean terminated;

        private int readBufferPosition;
        private CommandRegistry.CommandDescriptorImpl location;
        private Object outputObject;
        private ShellContextHandler shellContextHandler;

        ShellContextImpl(Shell shell) {
            this.shell = shell;
            this.variables = new CoreMap<>();
            this.args = new CoreList<>();
            readBuffer = BufferUtils.allocate((int) MemoryUnit.KILOBYTES.toBytes(10));
            statementParser = new StatementParser(shell.commandProcessor);
            cachedReadListener = this::onRead;
        }

        void start(ReadableBufferChannel input, WritableBufferChannel output, boolean interactive, boolean closeOnError,
                   CommandRegistry.CommandDescriptorImpl location,
                   Map<DirectBuffer, DirectBuffer> variables, List<DirectBuffer> args,
                   ShellContextImpl parentShell, boolean subShell,
                   ShellContextHandler shellContextHandler) {
            this.input = input;
            this.output = output;
            this.interactive = interactive;
            this.closeOnError = closeOnError;
            this.location = location;
            this.variables.putAll(variables);
            this.parentShell = parentShell;
            this.subShell = subShell;
            this.shellContextHandler = shellContextHandler;

            if (args != null) {
                this.args.addAll(args);
            }

            if (interactive) {
                try {
                    writePrompt();
                } catch (IOException e) {
                    shell.log.warn().append("could not write prompt: ").append(e).commit();
                    if (closeOnError) {
                        close();
                    }
                    return;
                }
            }

            if (input != null) {
                input.setReadListener(cachedReadListener);
            }
        }

        void setPath(CommandRegistry.CommandDescriptorImpl path) {
            this.location = path;
        }

        void setOutput(Object outputObject) {
            this.outputObject = outputObject;
        }

        void terminate() {
            this.terminated = true;
        }

        ObjectEncoder getObjectEncoder() {
            return shellContextHandler == null ? EncoderUtils.NULL_ENCODER : shellContextHandler.getObjectEncoder();
        }

        AsyncCommandContext borrowAsyncCommandContext() {
            return shellContextHandler.borrowAsyncCommandContext();
        }

        @Override
        public Map<DirectBuffer, DirectBuffer> getVariables() {
            return variables;
        }

        @Override
        public List<DirectBuffer> getArguments() {
            return args;
        }

        @Override
        public CommandRegistry.CommandDescriptorImpl getPath() {
            return location;
        }

        @Override
        public void loadFile(DirectBuffer filePath, List<DirectBuffer> args, boolean subShell) throws IOException {
            var fileString = BufferUtils.toAsciiString(filePath);

            for (var thePath : shell.filePath) {
                var fullPath = thePath.resolve(fileString);
                if (fullPath.toFile().exists()) {
                    shell.log.info().append("loading file: file=").append(fullPath)
                            .append(", args=").append(args)
                            .append(", copyOfVariables=").append(subShell)
                            .commit();

                    var sourceFile = new FileChannel(fullPath, StandardOpenOption.READ);

                    var instance = shell.instancePool.borrowObject();
                    instance.start(sourceFile, output, interactive, closeOnError,
                            location, variables, args, this, subShell, shellContextHandler);
                    var exception = shell.processingException;
                    shell.processingException = null;
                    if (exception != null) {
                        throw new IOException("could not process file: " + fileString, exception);
                    }
                    return;
                }
            }

            throw new IOException("could not find file in path: " + BufferUtils.toAsciiString(filePath));
        }

        @Override
        public Object executeInline(DirectBuffer buffer, int offset, int length) throws IOException, CommandException {
            // start encoding in case this statement includes an ObjectEncoder argument
            outputObject = null;
            statementParser.parse(this, buffer, offset, length);
            return outputObject;
        }

        @Override
        public int execute(DirectBuffer buffer, int offset, int length) throws IOException, CommandException {
            // start encoding in case this statement includes an ObjectEncoder argument
            var encoder = shellContextHandler == null
                    ? EncoderUtils.NULL_ENCODER : shellContextHandler.getObjectEncoder();
            encoder.start(shell.tempBuffer, 0);

            outputObject = null;
            var lengthParsed = statementParser.parse(this, buffer, offset, length);

            // stop encoding and see if anything was written to the ObjectEncoder and then write to output
            var bytesWritten = encoder.stop();
            if (bytesWritten > 0) {
                shell.tempBuffer.putByte(bytesWritten, (byte) '\n');
                writeToOutput(shell.tempBuffer, 0, bytesWritten + 1);
            }

            if (terminated) {
                return -1;
            } else if (outputObject != null) {
                encoder.start(shell.tempBuffer, 0);
                encoder.object(outputObject);
                var bytes = encoder.stop();
                shell.tempBuffer.putByte(bytes, (byte) '\n');
                writeToOutput(shell.tempBuffer, 0, bytes + 1);
                outputObject = null;
            }

            return lengthParsed;
        }

        @Override
        public void close() {
            if (!subShell) {
                if (parentShell == null) {
                    shell.variables.putAll(variables);
                } else {
                    parentShell.variables.putAll(variables);
                }
            }
            parentShell = null;
            subShell = false;

            try {
                if (input != null) {
                    input.setReadListener(null);
                    input.close();
                }
            } catch (IOException e) {
                shell.log.warn().append("error closing shell input channel: ").append(e).commit();
            }
            input = null;
            output = null;

            if (shellContextHandler != null) {
                shellContextHandler.onClosed();
            }
            shellContextHandler = null;

            variables.clear();
            args.clear();
            statementParser.clear();

            interactive = false;
            closeOnError = false;
            terminated = false;

            readBufferPosition = 0;
            location = null;
            outputObject = null;

            shell.instancePool.returnObject(this);
        }

        private void onRead() {
            try {
                var bytesRead = input.read(
                        readBuffer, readBufferPosition, readBuffer.capacity() - readBufferPosition);
                if (bytesRead == -1) {
                    statementParser.flush(this);
                    close();
                    return;
                }
                readBufferPosition += bytesRead;

                var bufferPosition = 0;
                var lengthParsed = 0;
                do {
                    lengthParsed = execute(readBuffer, bufferPosition, readBufferPosition - bufferPosition);
                    if (lengthParsed == -1) {
                        close();
                        return;
                    }
                    bufferPosition += lengthParsed;
                } while (readBufferPosition - bufferPosition > 0 && lengthParsed > 0);

                BufferUtils.compact(readBuffer, bufferPosition, readBufferPosition - bufferPosition);
                readBufferPosition -= bufferPosition;

                if (interactive && readBufferPosition == 0) {
                    writePrompt();
                }
            } catch (IOException e) {
                shell.processingException = e;
                shell.log.warn().append("error reading from shell input channel, closing: ").append(e).commit();
                close();
            } catch (CommandException e) {
                try {
                    shell.processingException = e;
                    writeToOutput(e);

                    if (closeOnError) {
                        close();
                    } else {
                        readBufferPosition = 0;
                        statementParser.clear();
                        writePrompt();
                    }
                } catch (IOException e2) {
                    close();
                }
            }
        }

        private void writePrompt() throws IOException {
            if (getPath() != null) {
                writeToOutput(getPath().toString());
            }
            writeToOutput(SHELL_PROMPT);
        }

        private void writeToOutput(String stringToWrite) throws IOException {
            var length = shell.tempBuffer.putStringWithoutLengthAscii(0, stringToWrite);
            writeToOutput(shell.tempBuffer, 0, length);
        }

        private void writeToOutput(DirectBuffer bytesToWrite) throws IOException {
            writeToOutput(bytesToWrite, 0, bytesToWrite.capacity());
        }

        private void writeToOutput(DirectBuffer bytesToWrite, int offset, int length) throws IOException {
            if (output != null) {
                var bytesWritten = output.write(bytesToWrite, offset, length);
                if (length != bytesWritten) {
                    shell.log.warn().append("buffer overflow on shell output channel, closing").commit();
                    throw new IOException("buffer overflow on shell output channel");
                }
            }
        }

        private void writeToOutput(Throwable e) throws IOException {
            if (output != null) {
                if (e instanceof CommandException) {
                    shell.log.warn().append(e.getMessage()).commit();

                    writeToOutput(e.getMessage());
                    writeToOutput(NEW_LINE);

                    if (e.getCause() != null) {
                        shell.log.warn().append(e.getCause()).commit();

                        writeToOutput(e.getCause());
                        writeToOutput(NEW_LINE);
                    }
                } else {
                    writeToOutput(e.getClass().getName());
                    if (e.getMessage() != null) {
                        writeToOutput(": ");
                        writeToOutput(e.getMessage());
                        writeToOutput(NEW_LINE);
                    }

                    for (var stackTraceElement : e.getStackTrace()) {
                        writeToOutput(stackTraceElement.toString());
                        writeToOutput(NEW_LINE);
                    }

                    if (e.getCause() != null) {
                        writeToOutput(e.getCause());
                    }
                }
            }
        }
    }
}
