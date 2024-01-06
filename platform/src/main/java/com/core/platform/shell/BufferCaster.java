package com.core.platform.shell;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreMap;
import org.agrona.DirectBuffer;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Modifier;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.function.Function;

/**
 * A {@code BufferCaster} casts a {@code DirectBuffer} into an object using casting functions that are added
 * to the caster.
 *
 * <p>The following default casters are included in the caster:
 * <ul>
 *     <li>DirectBuffer
 *     <li>String
 *     <li>Boolean, boolean
 *     <li>Byte, byte
 *     <li>Short, short
 *     <li>Integer, int
 *     <li>Long, long
 * </ul>
 *
 * <p>New casters can be added with {@link #add(Function, Class)}.
 * If a caster is not associated with the class to cast to, then reflection is used to find a constructor with a single
 * {@code DirectBuffer} parameter.
 * If no appropriate constructor can be found then reflection is used to find a static {@code valueOf} or {@code of}
 * method that has a single {@code String} argument and returns an instance of the object.
 */
public class BufferCaster {

    private final Map<Class<?>, Function<DirectBuffer, Object>> casts;
    private final DirectBuffer trueBuffer;
    private final DirectBuffer falseBuffer;

    /**
     * Creates a {@code DirectBufferCaster} with the default set of casters described in the class description.
     */
    public BufferCaster() {
        casts = new CoreMap<>();
        trueBuffer = BufferUtils.fromAsciiString("true");
        falseBuffer = BufferUtils.fromAsciiString("false");

        add(this::castClass, Class.class);
        add(x -> x, DirectBuffer.class);
        add(BufferUtils::toAsciiString, String.class);
        add(this::castBool, Boolean.class, boolean.class);
        // TODO: check for overflows
        add(x -> (byte) x.parseIntAscii(0, x.capacity()), Byte.class, byte.class);
        add(x -> (short) x.parseIntAscii(0, x.capacity()), Short.class, short.class);
        add(x -> x.parseIntAscii(0, x.capacity()), Integer.class, int.class);
        add(x -> x.parseLongAscii(0, x.capacity()), Long.class, long.class);
        add(x -> Double.parseDouble(BufferUtils.toAsciiString(x)), Double.class, double.class);
        add(x -> Float.parseFloat(BufferUtils.toAsciiString(x)), Float.class, float.class);
        add(x -> StandardOpenOption.valueOf(BufferUtils.toAsciiString(x)), OpenOption.class);
        // TODO: why is this required?
        add(x -> Path.of(BufferUtils.toAsciiString(x)), Path.class);
        add(x -> LocalTime.parse(BufferUtils.toAsciiString(x)), LocalTime.class);
        add(x -> LocalDate.parse(BufferUtils.toAsciiString(x)), LocalDate.class);
        add(x -> LocalDateTime.parse(BufferUtils.toAsciiString(x)), LocalDateTime.class);
        add(x -> ZonedDateTime.parse(BufferUtils.toAsciiString(x)), ZonedDateTime.class);
    }

    /**
     * Associates the specified casting function with the specified class.
     * If the caster already contained a casting function for the class, the old casting function is replaced by the
     * specified casting function.
     *
     * @param castingFunction the casting function
     * @param castFrom the type to castingFunction a buffer
     */
    public void add(Function<DirectBuffer, Object> castingFunction, Class<?> castFrom) {
        casts.put(castFrom, castingFunction);
    }

    private void add(Function<DirectBuffer, Object> castingFunction, Class<?>... castsFrom) {
        for (var castFrom : castsFrom) {
            add(castingFunction, castFrom);
        }
    }

    /**
     * Casts the {@code DirectBuffer} into an object of the specified type.
     *
     * @param buffer the buffer to cast
     * @param castTo the type of the object to cast to
     * @param <T> the type of the object to cast to
     * @return the casted object
     * @throws ClassCastException if there is no cast available from the buffer to the specified cast
     */
    @SuppressWarnings("unchecked")
    public <T> T cast(DirectBuffer buffer, Class<T> castTo) {
        var cast = casts.get(castTo);

        if (cast == null) {
            cast = findConstructorCast(castTo);
            if (cast == null) {
                cast = findValueOfCast(castTo);
                if (cast == null) {
                    throw new ClassCastException("no cast available for: " + castTo.getName());
                }
            }
        }

        try {
            return (T) cast.apply(buffer);
        } catch (Exception e) {
            throw new ClassCastException(
                    "invalid cast: class=" + castTo.getName()
                    + ", buffer=" + BufferUtils.toAsciiString(buffer));
        }
    }

    private <T> Function<DirectBuffer, Object> findConstructorCast(Class<T> castTo) {
        try {
            for (var constructor : castTo.getConstructors()) {
                if (constructor.getParameterCount() == 1
                        && constructor.getParameterTypes()[0] == DirectBuffer.class
                        && !constructor.isVarArgs()) {
                    var methodHandle = MethodHandles.lookup().unreflectConstructor(constructor);
                    Function<DirectBuffer, Object> cast = x -> {
                        try {
                            return methodHandle.invoke(x);
                        } catch (Throwable throwable) {
                            throw new ClassCastException(
                                    "invalid cast: class=" + castTo.getName()
                                            + ", buffer=" + BufferUtils.toAsciiString(x));
                        }
                    };
                    add(cast, castTo);
                    return cast;
                }
            }
        } catch (IllegalAccessException ignored) {
            // ignored
        }
        return null;
    }

    private <T> Function<DirectBuffer, Object> findValueOfCast(Class<T> castTo) {
        try {
            var methods = castTo.getMethods();
            for (var method : methods) {
                if (Modifier.isStatic(method.getModifiers())
                        && ("valueOf".equals(method.getName()) || "of".equals(method.getName()))
                        && method.getReturnType() == castTo
                        && method.getParameterCount() == 1
                        && method.getParameterTypes()[0] == String.class
                        && !method.isVarArgs()) {
                    var methodHandle = MethodHandles.lookup().unreflect(method);
                    Function<DirectBuffer, Object> cast = x -> {
                        try {
                            return methodHandle.invoke(BufferUtils.toAsciiString(x));
                        } catch (Throwable throwable) {
                            throw new ClassCastException(
                                    "invalid cast: class=" + castTo.getName()
                                            + ", buffer=" + BufferUtils.toAsciiString(x));
                        }
                    };
                    add(cast, castTo);
                    return cast;
                }
            }
        } catch (IllegalAccessException ignored) {
            // ignored
        }
        return null;
    }

    private boolean castBool(DirectBuffer buffer) {
        if (trueBuffer.equals(buffer)) {
            return true;
        } else if (falseBuffer.equals(buffer)) {
            return false;
        } else {
            throw new ClassCastException(
                    "invalid cast: class=" + boolean.class.getName()
                            + ", buffer=" + BufferUtils.toAsciiString(buffer));
        }
    }

    private Class<?> castClass(DirectBuffer buffer) {
        var className = BufferUtils.toAsciiString(buffer);
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new ClassCastException("class not found: " + className);
        }
    }
}
