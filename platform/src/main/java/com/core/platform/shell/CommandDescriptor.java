package com.core.platform.shell;

import com.core.infrastructure.encoding.MutableObjectEncoder;
import com.core.infrastructure.encoding.ObjectEncoder;

import java.util.List;

/**
 * A command descriptor is a unique path in the shell.
 * It may contain an object or a command that can be executed.
 */
public interface CommandDescriptor {

    /**
     * Returns true if the command descriptor does not contain a registered object or an executable method.
     * An empty command descriptor is the equivalent of an empty directory.
     * An object or method can be registered in an empty command descriptor.
     * An error is thrown if an object or method attempts to register in a non-empty command descriptor.
     *
     * @return true if the command descriptor does not contain a registered object or an executable method
     */
    boolean isEmpty();

    /**
     * Returns true if there is a method associated with the command descriptor.
     *
     * @return  true if there is a method associated with the command descriptor
     */
    boolean isExecutable();

    /**
     * Returns the object associated with the command descriptor, or null if no object is associated with the command
     * descriptor.
     *
     * @return the object associated with the command descriptor
     */
    Object getObject();

    /**
     * Returns the parent of the command descriptor.
     *
     * @return the parent of the command descriptor
     */
    CommandDescriptor getParent();

    /**
     * Returns true if there is a method associated with the command descriptor and its execution will not change the
     * state of the system.
     *
     * @return true if there is a method associated with the command descriptor and its execution will not change the
     *     state of the system
     */
    boolean isReadOnly();

    /**
     * Returns the types of the parameters of the method associated with the command descriptor.
     *
     * @return the types of the parameters of the method associated with the command descriptor
     */
    Class<?>[] getParameterTypes();

    /**
     * Returns the names of the parameters of the method associated with the command descriptor.
     *
     * @return the names of the parameters of the method associated with the command descriptor
     */
    String[] getParameterNames();

    /**
     * Returns the types of the parameters, required to be specified in the {@code execute} command, of the method
     * associated with the command descriptor.
     * Types that are implicitly provided by the shell are not included in the returned type array.
     *
     * @return the types of the parameters, required to be specified in the {@code execute} command, of the method
     *     associated with the command descriptor
     */
    Class<?>[] getRequiredParameterTypes();

    /**
     * Returns true if the method, associated with the command descriptor, is of variable arity.
     *
     * @return true if the method, associated with the command descriptor, is of variable arity
     */
    boolean isVarArgs();

    /**
     * Returns the type of the return value of the method associated with the command descriptor.
     *
     * @return the type of the return value of the method associated with the command descriptor
     */
    Class<?> getReturnType();

    /**
     * Returns true if the command will return streaming results after the invocation of the method.
     *
     * @return true if the command will return streaming results after the invocation of the method
     */
    boolean isStreaming();

    /**
     * Executes the method, associated with the command descriptor, with the specified arguments and returns the result
     * of the executed method.
     *
     * @param args the arguments to the command
     * @return the result
     * @throws Throwable if an error occurs during execution of the command
     */
    Object execute(List<Object> args) throws Throwable;

    /**
     * Returns the absolute path of the command descriptor.
     *
     * @return the absolute path of the command descriptor
     */
    String getPath();

    /**
     * Writes the contents of the directory to the specified {@code encoder}.
     *
     * @param encoder the encoder
     */
    void ls(ObjectEncoder encoder);
}
