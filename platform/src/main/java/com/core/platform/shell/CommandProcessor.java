package com.core.platform.shell;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.command.Preferred;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.metrics.MetricFactory;
import org.agrona.DirectBuffer;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

@SuppressWarnings("PMD.UnusedFormalParameter")
class CommandProcessor implements StatementParser.CommandParser {

    private static final DirectBuffer CD = BufferUtils.fromAsciiString("cd");
    private static final DirectBuffer CREATE = BufferUtils.fromAsciiString("create");
    private static final DirectBuffer CONTEXT = BufferUtils.fromAsciiString("context");
    private static final DirectBuffer DEFAULT = BufferUtils.fromAsciiString("default");
    private static final DirectBuffer ECHO = BufferUtils.fromAsciiString("echo");
    private static final DirectBuffer EXIT = BufferUtils.fromAsciiString("exit");
    private static final DirectBuffer LS = BufferUtils.fromAsciiString("ls");
    private static final DirectBuffer PWD = BufferUtils.fromAsciiString("pwd");
    private static final DirectBuffer SET = BufferUtils.fromAsciiString("set");
    private static final DirectBuffer SOURCE = BufferUtils.fromAsciiString("source");
    private static final DirectBuffer SUBSHELL_OPTION = BufferUtils.fromAsciiString("-s");
    private static final DirectBuffer ROOT = BufferUtils.fromAsciiString("/");
    private static final DirectBuffer CURRENT_DIRECTORY = BufferUtils.fromAsciiString(".");

    private final MetricFactory metricFactory;
    private final LogFactory logFactory;
    private final Log log;
    private final List<Object> argumentList;
    private final Map<Class<?>, Object> impliedArgs;
    private final BufferCaster caster;
    private final CommandRegistry commandRegistry;
    private final DirectBuffer referenceWrap;

    CommandProcessor(
            MetricFactory metricFactory,
            LogFactory logFactory,
            BufferCaster caster,
            CommandRegistry commandRegistry,
            Map<Class<?>, Object> impliedArgs) {
        this.metricFactory = metricFactory;
        this.logFactory = logFactory;
        this.log = logFactory.create(Shell.class);
        this.caster = caster;
        this.commandRegistry = commandRegistry;
        this.impliedArgs = impliedArgs;

        argumentList = new CoreList<>();
        referenceWrap = BufferUtils.emptyBuffer();
    }

    @Override
    public void process(
            ShellContext context,
            DirectBuffer command, DirectBuffer[] params, int index, int length) throws CommandException {
        var instance = (Shell.ShellContextImpl) context;
        if (instance.getPath() == null) {
            instance.setPath(commandRegistry.getRoot());
        }
        instance.setOutput(null);

        var logger = log.info()
                .append(instance.getPath()).append(" % \"").append(command).append('"');
        for (var i = index; i < index + length; i++) {
            logger.append(" \"").append(params[i]).append('"');
        }
        logger.commit();

        if (CD.equals(command))  {
            processCd(instance, params, index, length);
        } else if (CONTEXT.equals(command)) {
            processContext(instance, params, index, length);
        } else if (CREATE.equals(command)) {
            processCreate(instance, params, index, length);
        } else if (DEFAULT.equals(command)) {
            processDefault(instance, params, index, length);
        } else if (ECHO.equals(command)) {
            processEcho(instance, params, index, length);
        } else if (EXIT.equals(command)) {
            processExit(instance, params, index, length);
        } else if (LS.equals(command)) {
            processLs(instance, params, index, length);
        } else if (PWD.equals(command)) {
            processPwd(instance, params, index, length);
        } else if (SET.equals(command)) {
            processSet(instance, params, index, length);
        } else if (SOURCE.equals(command)) {
            processSource(instance, params, index, length);
        } else {
            executeCommand(instance, command, params, index, length);
        }
    }

    private void processCd(
            Shell.ShellContextImpl context, DirectBuffer[] params, int index, int length) throws CommandException {
        if (length >= 2) {
            throw new CommandException("cd: too many arguments: cd <path>");
        }
        if (length == 0) {
            context.setPath(commandRegistry.resolve(context.getPath(), ROOT));
        } else if (length == 1) {
            context.setPath(commandRegistry.resolve(context.getPath(), params[index]));
        }
    }

    private void processContext(
            Shell.ShellContextImpl context, DirectBuffer[] params, int index, int length) throws CommandException {
        if (length != 1) {
            throw new CommandException("context: invalid number of arguments: context <name>");
        }
        var contextName = BufferUtils.toAsciiString(params[index]);
        if (contextName.equals("-r")) {
            logFactory.setLogIdentifier(1, null);
            metricFactory.removeLabel("context");
        } else {
            logFactory.setLogIdentifier(1, contextName);
            metricFactory.setLabel("context", contextName);
        }
    }

    private void processCreate(Shell.ShellContextImpl context, DirectBuffer[] params, int index, int length)
            throws CommandException {
        if (length < 2) {
            throw new CommandException("create: too few arguments: create <path> <className> [args ...]");
        }
        var object = createObject(
                context, BufferUtils.toAsciiString(params[index + 1]), params, index + 2, length - 2);
        var commandDescriptor = commandRegistry.addObject(context.getPath(), params[index], object);
        if (object instanceof CommandObject) {
            ((CommandObject) object).onRegistered(commandDescriptor.getPath());
        }
        context.setOutput(object);
    }

    private void processDefault(Shell.ShellContextImpl context, DirectBuffer[] params, int index, int length)
            throws CommandException {
        if (length != 2) {
            throw new CommandException("default: too few arguments: default <key> <value>" + length);
        }
        context.getVariables().putIfAbsent(BufferUtils.copy(params[index]), BufferUtils.copy(params[index + 1]));
    }

    private void processEcho(Shell.ShellContextImpl context, DirectBuffer[] params, int index, int length) {
        var result = new StringBuilder();
        for (var i = index; i < index + length; i++) {
            if (i > index) {
                result.append(' ');
            }
            result.append(BufferUtils.toAsciiString(params[i]));
        }
        context.setOutput(result.toString());
    }

    private void processExit(Shell.ShellContextImpl context, DirectBuffer[] params, int index, int length)
            throws CommandException {
        if (length != 0) {
            throw new CommandException("exit: too many arguments");
        }
        context.terminate();
    }

    private void processLs(Shell.ShellContextImpl context, DirectBuffer[] params, int index, int length)
            throws CommandException {
        if (length >= 2) {
            throw new CommandException("cd: too many arguments: cd <path>");
        }
        if (length == 0) {
            var resolved = commandRegistry.resolve(context.getPath(), CURRENT_DIRECTORY);
            resolved.ls(context.getObjectEncoder());
        } else {
            var resolved = commandRegistry.resolve(context.getPath(), params[index]);
            resolved.ls(context.getObjectEncoder());
        }
    }

    private void processPwd(Shell.ShellContextImpl context, DirectBuffer[] params, int index, int length)
            throws CommandException {
        if (length > 0) {
            throw new CommandException("pwd: too many arguments");
        }
        context.setOutput(context.getPath().toString());
    }

    private void processSet(Shell.ShellContextImpl context, DirectBuffer[] params, int index, int length)
            throws CommandException {
        if (length != 2) {
            throw new CommandException("set: invalid number of arguments: set <key> <value>");
        }
        context.getVariables().put(BufferUtils.copy(params[index]), BufferUtils.copy(params[index + 1]));
    }

    private void processSource(Shell.ShellContextImpl context, DirectBuffer[] params, int index, int length)
            throws CommandException {
        if (length == 0) {
            throw new CommandException("source: invalid number of arguments: source [-s] <file>");
        }
        var startOfFileName = index;

        var copyOfVariables = SUBSHELL_OPTION.equals(params[index]);
        if (copyOfVariables) {
            if (length == 1) {
                throw new CommandException("source: invalid number of arguments: source [-s] <file>");
            }
            startOfFileName++;
        }

        var filePath = params[startOfFileName];

        var args = new CoreList<DirectBuffer>();
        for (var i = startOfFileName; i < index + length; i++) {
            args.add(params[i]);
        }

        try {
            context.loadFile(filePath, args, copyOfVariables);
        } catch (IOException e) {
            if (e.getCause() instanceof CommandException) {
                throw (CommandException) e.getCause();
            } else {
                throw new CommandException("source: could not load file: " + BufferUtils.toAsciiString(filePath));
            }
        }
    }

    private void executeCommand(
            Shell.ShellContextImpl context, DirectBuffer command, DirectBuffer[] params, int index, int length)
            throws CommandException {
        var commandDescriptor = commandRegistry.resolve(context.getPath(), command);
        if (!commandDescriptor.isExecutable()) {
            throw new CommandException("exec: command not found: " + BufferUtils.toAsciiString(command));
        }

        var args = buildArgumentList("exec", context, params, index, length,
                commandDescriptor.getParameterTypes(), commandDescriptor.isVarArgs());

        try {
            var result = commandDescriptor.execute(args);
            context.setOutput(result);
        } catch (CommandException e) {
            throw e;
        } catch (Throwable e) {
            throw new CommandException("exec: could not execute command: " + BufferUtils.toAsciiString(command), e);
        }
    }

    private Object createObject(
            Shell.ShellContextImpl context, String className, DirectBuffer[] params, int index, int length)
            throws CommandException {
        var constructor = getConstructor(className, length);
        var args = buildArgumentList("create", context, params, index, length,
                constructor.getParameterTypes(), constructor.isVarArgs());
        try {
            // TODO: cache off this method handle
            var methodHandle = MethodHandles.lookup().unreflectConstructor(constructor);
            if (constructor.isVarArgs()) {
                methodHandle = methodHandle.withVarargs(true);
            }
            return methodHandle.invokeWithArguments(args);
        } catch (CommandException e) {
            throw e;
        } catch (Throwable e) {
            throw new CommandException("exec: could not create object: " + className, e);
        }
    }

    private Constructor<?> getConstructor(String className, int numArgs) throws CommandException {
        Class<?> clz;
        try {
            clz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new CommandException("create: unknown class: " + className);
        }

        Constructor<?> theConstructor = null;
        var theConstructorIsPreferred = false;

        for (var constructor : clz.getConstructors()) {
            var types = constructor.getParameterTypes();

            var impliedTypes = 0;
            for (var type : types) {
                if (impliedArgs.containsKey(type)) {
                    impliedTypes++;
                }
            }

            if (impliedTypes + numArgs == types.length
                    || constructor.isVarArgs() && impliedTypes + numArgs >= types.length - 1) {
                var preferred = constructor.getAnnotation(Preferred.class) != null;
                if (theConstructor != null
                        && (preferred && theConstructorIsPreferred
                        || !preferred && !theConstructorIsPreferred)) {
                    throw new CommandException(
                            "create: multiple matching constructors found: class=" + className
                                    + ", constructorArgs=" + constructor.getParameterCount()
                                    + ", impliedArgs=" +  impliedTypes
                                    + ", explicitArgs=" + numArgs
                                    + ", isVarArgs=" + constructor.isVarArgs()
                                    + ", theConstructor=" + theConstructor
                                    + ", constructor=" + constructor);
                } else if (preferred || theConstructor == null) {
                    theConstructor = constructor;
                    theConstructorIsPreferred = preferred;
                }
            }
        }

        if (theConstructor == null) {
            throw new CommandException(
                    "create: could not find constructor: class=" + className + ", explicitArgs=" + numArgs);
        }
        return theConstructor;
    }

    private List<Object> buildArgumentList(
            String source,
            Shell.ShellContextImpl context,
            DirectBuffer[] params, int index, int length,
            Class<?>[] paramTypes, boolean varArgs)
            throws CommandException {
        argumentList.clear();
        var paramIndex = index;
        for (var i = 0; i < paramTypes.length; i++) {
            var paramType = paramTypes[i];
            var value = impliedArgs.get(paramType);
            if (value == null) {
                if (paramType == ObjectEncoder.class) {
                    argumentList.add(context.getObjectEncoder());
                } else if (paramType == AsyncCommandContext.class) {
                    argumentList.add(context.borrowAsyncCommandContext());
                } else if (varArgs && i == paramTypes.length - 1) {
                    var numComponents = length - (paramIndex - index);
                    var componentType = paramType.getComponentType();
                    for (var j = 0; j < numComponents; j++) {
                        try {
                            value = getValue(source, context, paramIndex, componentType, params[paramIndex]);
                            argumentList.add(value);
                            paramIndex++;
                        } catch (ClassCastException e) {
                            throw new CommandException(source + ": could not cast parameter: index=" + paramIndex
                                    + ", param=" + BufferUtils.toAsciiString(params[paramIndex])
                                    + ", paramType=" + componentType);
                        }
                    }
                } else {
                    if (paramIndex == index + length) {
                        throw new CommandException(source + ": too many arguments: expected=" + paramTypes.length
                                + ", actual=" + length);
                    }

                    var param = params[paramIndex];
                    value = getValue(source, context, paramIndex, paramType, param);
                    argumentList.add(value);
                    paramIndex++;
                }
            } else {
                argumentList.add(value);
            }
        }

        if (paramIndex != index + length) {
            var explicitArgs = paramIndex - index;
            throw new CommandException(source + ": incorrect number of arguments: required=" + paramTypes.length
                    + ", varArgs=" + false
                    + ", total=" + argumentList.size()
                    + ", explicit=" + explicitArgs
                    + ", implicit=" + (argumentList.size() - explicitArgs));
        }

        return argumentList;
    }

    private Object getValue(String source, Shell.ShellContextImpl context, int paramIndex,
                            Class<?> componentType, DirectBuffer param)
            throws CommandException {
        Object value;
        if (param.capacity() > 1 && param.getByte(0) == '@') {
            referenceWrap.wrap(param, 1, param.capacity() - 1);
            var directory = commandRegistry.resolve(context.getPath(), referenceWrap);
            value = directory.getObject();
            if (value == null) {
                throw new CommandException(source + ": no object at: " + BufferUtils.toAsciiString(param));
            }
        } else {
            try {
                value = caster.cast(param, componentType);
            } catch (ClassCastException e) {
                throw new CommandException(
                        source + ": could not cast parameter: index=" + paramIndex
                                + ", param=" + BufferUtils.toAsciiString(param)
                                + ", paramType=" + componentType.getName());
            }
        }
        return value;
    }
}
