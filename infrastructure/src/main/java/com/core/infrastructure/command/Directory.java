package com.core.infrastructure.command;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A command that registers the field value as an object, including all it's commands, at the specified path.
 */
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface Directory {

    /**
     * The path to register the object at.
     * If the path is empty, the resulting path for the object will be {@code [containingObject]/[fieldName]}.
     *
     * @return the path to the command
     */
    String path() default "";

    /**
     * Returns true if creating the directory throws an exception if the directory's object is already registered.
     *
     * @return true if creating the directory throws an exception if the directory's object is already registered
     */
    boolean failIfExists() default true;
}
