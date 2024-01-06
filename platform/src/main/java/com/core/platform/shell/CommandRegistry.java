package com.core.platform.shell;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.collections.CoreMap;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Directory;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.encoding.MutableObjectEncoder;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import org.agrona.DirectBuffer;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

class CommandRegistry {

    private final Map<Class<?>, Object> impliedParams;
    private final Map<Object, CommandDescriptorImpl> objectToDescriptor;
    private final CommandDescriptorImpl root;
    private final DirectBuffer component;
    private final Log log;
    private final List<Consumer<CommandDescriptor>> objectListeners;

    CommandRegistry(LogFactory logFactory, Map<Class<?>, Object> impliedParams) {
        log = logFactory.create(CommandRegistry.class);
        objectListeners = new CoreList<>();
        this.impliedParams = impliedParams;
        root = new CommandDescriptorImpl(null, BufferUtils.fromAsciiString(""));
        component = BufferUtils.emptyBuffer();
        objectToDescriptor = new CoreMap<>();
    }

    void addObjectListener(Consumer<CommandDescriptor> listener) {
        objectListeners.add(listener);

        dispatch(listener, root);
    }

    String getPath(Object object) {
        var descriptor = objectToDescriptor.get(object);
        return descriptor == null ? null : descriptor.getPath();
    }

    private void dispatch(Consumer<CommandDescriptor> listener, CommandDescriptorImpl commandDescriptor) {
        if (commandDescriptor.getObject() != null) {
            listener.accept(commandDescriptor);
        }
        for (var child : commandDescriptor.children) {
            dispatch(listener, child);
        }
    }

    CommandDescriptorImpl getRoot() {
        return root;
    }

    CommandDescriptorImpl addObject(CommandDescriptorImpl from, DirectBuffer to, Object object)
            throws CommandException {
        try {
            var commandDescriptor = resolve(from, to, true);
            if (!commandDescriptor.isEmpty()) {
                throw new CommandException("cannot create command at location: " + commandDescriptor);
            }
            log.info().append("adding object: path=").append(commandDescriptor.getPath())
                    .append(", type=").append(object.getClass().getName())
                    .commit();
            commandDescriptor.object = object;
            objectToDescriptor.put(object, commandDescriptor);
            addFields(object, commandDescriptor, object.getClass());
            addMethods(object, commandDescriptor, object.getClass());

            for (var objectListener : objectListeners) {
                objectListener.accept(commandDescriptor);
            }

            return commandDescriptor;
        } catch (IllegalAccessException e) {
            throw new CommandException("cannot add command", e);
        }
    }

    private void addFieldObject(CommandDescriptorImpl from, DirectBuffer to, Object object, boolean failOnRegistered)
            throws CommandException {
        try {
            var desc = objectToDescriptor.get(object);
            if (desc != null) {
                if (failOnRegistered) {
                    throw new CommandException("object already registered at path: " + desc.getPath());
                }
                return;
            }

            var commandDescriptor = resolve(from, to, true);
            log.info().append("adding field object: path=").append(commandDescriptor.getPath())
                    .append(", type=").append(object.getClass().getName())
                    .commit();
            if (commandDescriptor.isEmpty()) {
                commandDescriptor.object = object;
                objectToDescriptor.put(object, commandDescriptor);
            }
            addFields(object, commandDescriptor, object.getClass());
            addMethods(object, commandDescriptor, object.getClass());
        } catch (IllegalAccessException e) {
            throw new CommandException("cannot add command", e);
        }
    }


    CommandDescriptorImpl resolve(Object object) {
        return objectToDescriptor.get(object);
    }

    CommandDescriptorImpl resolve(CommandDescriptorImpl from, DirectBuffer to) throws CommandException {
        return resolve(from, to, false);
    }

    CommandDescriptorImpl resolve(CommandDescriptorImpl from, DirectBuffer to, boolean create) throws CommandException {
        if (to.capacity() == 0) {
            return from;
        }

        var currentDescriptor = from;

        var startSubcommandDescriptor = 0;
        for (var i = 0; i < to.capacity(); i++) {
            var byte1 = to.getByte(i);
            if (i == 0 && byte1 == '/') {
                currentDescriptor = root;
                startSubcommandDescriptor = i + 1;
            } else if (byte1 == '.') {
                var byte2 = i + 1 < to.capacity() ? to.getByte(i + 1) : 0;
                var byte3 = i + 2 < to.capacity() ? to.getByte(i + 2) : 0;
                if (byte2 == '/') {
                    i++;
                    startSubcommandDescriptor = i + 1;
                } else if (byte2 == '.' && (byte3 == '/' || byte3 == 0)) {
                    i += 2;
                    currentDescriptor = currentDescriptor.parent;
                    startSubcommandDescriptor = i + 1;
                } else if (byte2 != 0 && byte3 != 0) {
                    throw new CommandException("illegal navigation: " + BufferUtils.toAsciiString(to));
                }
            } else if (byte1 == '/' || i == to.capacity() - 1) {
                var length = i - startSubcommandDescriptor;
                if (i == to.capacity() - 1) {
                    length++;
                }
                if (length == 0) {
                    throw new CommandException("cannot have empty path component: " + BufferUtils.toAsciiString(to));
                }
                component.wrap(to, startSubcommandDescriptor, length);
                currentDescriptor = currentDescriptor.child(component, create);
                startSubcommandDescriptor = i + 1;
            }

            if (currentDescriptor == null) {
                throw new CommandException("illegal navigation: " + BufferUtils.toAsciiString(to));
            }
        }

        return currentDescriptor;
    }

    private void addFields(Object object, CommandDescriptorImpl commandDescriptor, Class<?> theClass)
            throws CommandException, IllegalAccessException {
        for (var field : theClass.getDeclaredFields()) {
            var includeAnnotation = field.getAnnotation(Directory.class);
            if (includeAnnotation != null) {
                var path = BufferUtils.fromAsciiString(
                        includeAnnotation.path().isBlank() ? field.getName() : includeAnnotation.path());
                field.setAccessible(true);
                addFieldObject(commandDescriptor, path, field.get(object), includeAnnotation.failIfExists());
            }

            var annotation = field.getAnnotation(Property.class);
            if (annotation != null) {
                field.setAccessible(true);

                if (annotation.read()) {
                    var path = BufferUtils.fromAsciiString(
                            annotation.getterPath().isBlank() ? field.getName() : annotation.getterPath());
                    var childCommandDescriptor = resolve(commandDescriptor, path, true);
                    if (!childCommandDescriptor.isEmpty()) {
                        throw new CommandException("cannot create command at: " + childCommandDescriptor);
                    }

                    log.info().append("adding getter: path=").append(childCommandDescriptor.getPath())
                            .append(", type=").append(object.getClass().getName())
                            .commit();
                    childCommandDescriptor.readOnly = true;
                    childCommandDescriptor.returnType = field.getType();
                    childCommandDescriptor.methodHandle = MethodHandles.publicLookup()
                            .unreflectGetter(field).bindTo(object);
                    childCommandDescriptor.paramTypes = new Class[0];
                    childCommandDescriptor.requiredParamTypes = new Class[0];
                    childCommandDescriptor.paramNames = new String[0];
                }

                if (annotation.write()) {
                    DirectBuffer path;
                    if (annotation.setterPath().isBlank()) {
                        var fieldName = field.getName().substring(0, 1)
                                .toUpperCase(Locale.ROOT) + field.getName().substring(1);
                        path = BufferUtils.fromAsciiString("set" + fieldName);
                    } else {
                        path = BufferUtils.fromAsciiString(annotation.setterPath());
                    }

                    var childCommandDescriptor = resolve(commandDescriptor, path, true);
                    if (!childCommandDescriptor.isEmpty()) {
                        throw new CommandException("cannot create command at: " + childCommandDescriptor);
                    }

                    log.info().append("adding setter: path=").append(childCommandDescriptor.getPath())
                            .append(", type=").append(object.getClass().getName())
                            .commit();
                    childCommandDescriptor.returnType = void.class;
                    childCommandDescriptor.methodHandle = MethodHandles.publicLookup()
                            .unreflectSetter(field).bindTo(object);
                    childCommandDescriptor.paramTypes = new Class[] { field.getType() };
                    childCommandDescriptor.requiredParamTypes = childCommandDescriptor.paramTypes;
                    childCommandDescriptor.paramNames = new String[] { field.getName() };
                }
            }
        }

        if (theClass.getSuperclass() != null) {
            addFields(object, commandDescriptor, theClass.getSuperclass());
        }
    }

    private void addMethods(Object object, CommandDescriptorImpl commandDescriptor, Class<?> theClass)
            throws CommandException, IllegalAccessException {
        for (var method : theClass.getDeclaredMethods()) {
            var commandAnnotation = method.getAnnotation(Command.class);
            if (commandAnnotation != null) {
                DirectBuffer path;
                if (commandAnnotation.path().isBlank()) {
                    var name = method.getName();
                    if (name.length() > 4 && name.startsWith("get")) {
                        path = BufferUtils.fromAsciiString(
                                name.substring(3, 4).toLowerCase(Locale.ROOT) + name.substring(4));
                    } else if (name.length() > 3 && name.startsWith("is")) {
                        path = BufferUtils.fromAsciiString(
                                name.substring(2, 3).toLowerCase(Locale.ROOT) + name.substring(3));
                    } else {
                        path = BufferUtils.fromAsciiString(name);
                    }
                } else {
                    path = BufferUtils.fromAsciiString(commandAnnotation.path());
                }
                method.setAccessible(true);

                var childCommandDescriptor = resolve(commandDescriptor, path, true);
                if (!childCommandDescriptor.isEmpty()) {
                    if (childCommandDescriptor.methodObject == object
                            && childCommandDescriptor.method.getName().equals(method.getName())
                            && Arrays.equals(childCommandDescriptor.getParameterTypes(), method.getParameterTypes())) {
                        // same reference
                        continue;
                    } else {
                        throw new CommandException("duplicate command at: " + childCommandDescriptor.toString());
                    }
                }
                var methodHandle = MethodHandles.publicLookup().unreflect(method).bindTo(object);
                if (method.isVarArgs()) {
                    methodHandle = methodHandle.withVarargs(true);
                }
                log.info().append("adding method: path=").append(childCommandDescriptor.getPath())
                        .append(", type=").append(object.getClass().getName())
                        .commit();
                childCommandDescriptor.readOnly = commandAnnotation.readOnly();
                childCommandDescriptor.methodObject = object;
                childCommandDescriptor.method = method;
                childCommandDescriptor.returnType = method.getReturnType();
                childCommandDescriptor.methodHandle = methodHandle;
                childCommandDescriptor.paramTypes = method.getParameterTypes();
                childCommandDescriptor.varArgs = method.isVarArgs();

                var requiredParamTypes = new CoreList<Class<?>>();
                var paramNames = new CoreList<String>();
                var params = method.getParameters();
                for (var i = 0; i < params.length; i++) {
                    var param = params[i];
                    var paramType = childCommandDescriptor.getParameterTypes()[i];
                    childCommandDescriptor.streaming |= paramType == AsyncCommandContext.class;
                    if (!impliedParams.containsKey(paramType)
                            && paramType != ObjectEncoder.class && paramType != AsyncCommandContext.class) {
                        requiredParamTypes.add(paramType);
                        paramNames.add(param.getName());
                    }
                }
                childCommandDescriptor.requiredParamTypes = requiredParamTypes.toArray(new Class[0]);
                childCommandDescriptor.paramNames = paramNames.toArray(new String[0]);
            }
        }

        if (theClass.getSuperclass() != null) {
            addMethods(object, commandDescriptor, theClass.getSuperclass());
        }
    }

    static class CommandDescriptorImpl implements CommandDescriptor {

        private final CommandDescriptorImpl parent;
        private final DirectBuffer name;
        public boolean readOnly;
        private CommandDescriptorImpl[] children;
        private MethodHandle methodHandle;
        private Class<?>[] paramTypes;
        private Class<?>[] requiredParamTypes;
        private String[] paramNames;
        private boolean varArgs;
        private Object object;
        private Method method;
        private Object methodObject;
        private Class<?> returnType;
        private boolean streaming;
        private String path;
        private String ls;

        CommandDescriptorImpl(CommandDescriptorImpl parent, DirectBuffer name) {
            this.parent = parent;
            this.name = BufferUtils.copy(name);
            children = new CommandDescriptorImpl[0];
        }

        @Override
        public Object execute(List<Object> args) throws Throwable {
            if (methodHandle == null) {
                throw new CommandException("no method in this commandDescriptor");
            }
            return methodHandle.invokeWithArguments(args);
        }

        @Override
        public String getPath() {
            if (path == null) {
                path = "";
                var current = this;
                do {
                    path = "/" + BufferUtils.toAsciiString(current.name) + path;
                    current = current.parent;
                } while (current != null && current.name.capacity() > 0);
            }
            return path;
        }

        @Override
        public Class<?> getReturnType() {
            return returnType;
        }

        @Override
        public boolean isStreaming() {
            return streaming;
        }

        @Override
        public Class<?>[] getParameterTypes() {
            return paramTypes;
        }

        @Override
        public String[] getParameterNames() {
            return paramNames;
        }

        @Override
        public Class<?>[] getRequiredParameterTypes() {
            return requiredParamTypes;
        }

        @Override
        public boolean isVarArgs() {
            return varArgs;
        }

        @Override
        public boolean isExecutable() {
            return methodHandle != null;
        }

        @Override
        public Object getObject() {
            return object;
        }

        @Override
        public CommandDescriptor getParent() {
            return parent;
        }

        @Override
        public boolean isReadOnly() {
            return readOnly;
        }

        @Override
        public boolean isEmpty() {
            return methodHandle == null && object == null;
        }

        @Override
        public void ls(ObjectEncoder encoder) {
            if (encoder.isMachineReadable()) {
                encoder.openList();
                for (var child : children) {
                    encoder.openMap()
                            .string("path").string(child.getPath());

                    if (child.isExecutable()) {
                        if (child.isStreaming()) {
                            encoder.string("stream").bool(true);
                        }
                        if (!child.isReadOnly()) {
                            encoder.string("write").bool(!child.isReadOnly());
                        }
                        if (child.isVarArgs()) {
                            encoder.string("varArgs").bool(true);
                        }

                        encoder.string("exec").bool(true)
                                .string("params").openList();
                        for (var j = 0; j < child.getRequiredParameterTypes().length; j++) {
                            var paramType = child.getParameterTypes()[j];
                            var paramName = child.getParameterNames()[j];
                            encoder.openMap()
                                    .string("type").string(paramType.getName())
                                    .string("name").string(paramName);
                            encoder.closeMap();
                        }
                        encoder.closeList();
                    }
                    encoder.closeMap();
                }
                encoder.closeList();
            } else {
                if (ls == null) {
                    var stringBuilder = new StringBuilder();
                    for (var i = 0; i < children.length; i++) {
                        var child = children[i];
                        stringBuilder.append(BufferUtils.toAsciiString(child.name));
                        if (child.isExecutable()) {
                            for (var j = 0; j < child.getRequiredParameterTypes().length; j++) {
                                var paramType = child.getRequiredParameterTypes()[j];
                                var paramName = child.getParameterNames()[j];
                                stringBuilder.append(" [").append(paramType.getSimpleName())
                                        .append(' ').append(paramName).append(']');
                                if (child.isVarArgs() && j == child.getRequiredParameterTypes().length - 1) {
                                    stringBuilder.append('*');
                                }
                            }

                            stringBuilder.append(' ');
                            if (!child.isReadOnly()) {
                                stringBuilder.append("w");
                            }
                            if (child.isStreaming()) {
                                stringBuilder.append("s");
                            }
                        } else {
                            stringBuilder.append('/');
                        }
                        if (i < children.length - 1) {
                            stringBuilder.append('\n');
                        }
                    }
                    ls = stringBuilder.toString();
                }
                encoder.string(ls);
            }
        }

        private CommandDescriptorImpl child(DirectBuffer childName, boolean create) throws CommandException {
            for (var child : children) {
                if (child.name.equals(childName)) {
                    return child;
                }
            }

            if (create) {
                children = Arrays.copyOf(children, children.length + 1);
                var child = new CommandDescriptorImpl(this, BufferUtils.copy(childName));
                children[children.length - 1] = child;
                return child;
            } else {
                throw new CommandException("unknown command: " + BufferUtils.toAsciiString(childName));
            }
        }

        private boolean isRoot() {
            return name.capacity() == 0;
        }

        @Override
        public String toString() {
            if (isRoot()) {
                return "/";
            } else {
                var string = "";
                var currentComponent = this;
                while (currentComponent != null && !currentComponent.isRoot()) {
                    string =  "/" + BufferUtils.toAsciiString(currentComponent.name) + string;
                    currentComponent = currentComponent.parent;
                }
                return string;
            }
        }
    }
}
