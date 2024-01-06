package com.core.infrastructure.command;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A command that registers a field value at the specified path.
 */
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface Property {

    /**
     * Returns the path to the command relative to the object that contains the field.
     * If the path is empty, the resulting path for the getter will be {@code [containingObject]/[fieldName]}.
     *
     * @return the path to the command
     */
    String getterPath() default "";

    /**
     * The path to the command relative to the object that contains the method.
     * If the path is empty, the resulting path for the setter will be {@code [containingObject]/set[fieldName]}, with
     * the first letter of the field name capitalized.
     *
     * @return the path to the command
     */
    String setterPath() default "";

    /**
     * Returns true if the value of the property can be read.
     *
     * @return true if the value of the property can be read
     */
    boolean read() default true;

    /**
     * Returns true if the value of the property can be written.
     *
     * @return true if the value of the property can be written
     */
    boolean write() default false;
}
