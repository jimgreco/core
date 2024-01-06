package com.core.infrastructure;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation for methods and classes that the user can expect to allocate new memory during the normal course of
 * operation.
 * This annotation should not be used for methods that allocate memory on startup, methods that allocate memory when a
 * threshold is reached, or on methods that obviously allocate memory.
 * Startup methods can also include the creation of sockets when the application is activated.
 * What constitutes startup is loosely defined, but generally includes everything up to the start of trading, including
 * the initial publish of reference data.
 * Methods that allocate memory when a threshold is reached include constructs like maps, sets, and lists that use a
 * backing array.
 * Obvious methods that allocate memory include {@code toString()}.
 */
@Target({ ElementType.METHOD, ElementType.TYPE, ElementType.CONSTRUCTOR })
@Retention(RetentionPolicy.RUNTIME)
public @interface Allocation {
}
