package com.core.infrastructure.command;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies a constructor that is to be prioritized for use when creating new objects through the command framework.
 *
 * <p>The command framework creates objects based on the number of explicit parameters specified by an object's
 * constructor.
 * However, objects can have many constructors with the same number of parameters.
 * For example,
 * <pre>
 *     public class Foo {
 *
 *         private int value;
 *
 *         public Foo(String value) {
 *             this.value = Integer.parseInt(value);
 *         }
 *
 *         &amp;Preferred
 *         public Foo(int value) {
 *             this.value = value;
 *         }
 *     }
 * </pre>
 *
 * <p>If the command framework finds multiple constructors, with the same number of explicit parameters, then the
 * {@code Preferred} annotation is used to instruct the command framework on which one to use for constructing the
 * object.
 * In this example, the second constructor, with an integer parameter is chosen.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.CONSTRUCTOR })
public @interface Preferred {
}
