package com.core.infrastructure.command;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A command that executes a method on a registered object.
 */
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
public @interface Command {

    /**
     * The path to the command relative to the object that contains the method.
     * If the path is empty, the resulting path for the setter will be {@code [containingObject]/[methodName]}.
     * If the method name starts with get or set, then those characters will be removed and the next letter with be
     * lower-cased.
     *
     * @return the path to the command
     */
    String path() default "";

    /**
     * Returns true if the command does not change the state of the system.
     *
     * @return true if the command does not change the state of the system
     */
    boolean readOnly() default false;
}
