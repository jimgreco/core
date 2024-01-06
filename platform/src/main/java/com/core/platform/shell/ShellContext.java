package com.core.platform.shell;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A shell context is an instance of the shell that has its own path, variables, and arguments.
 */
public interface ShellContext {

    /**
     * Returns a map of all variables in the shell context.
     *
     * @return a map of all variables in the shell context
     */
    Map<DirectBuffer, DirectBuffer> getVariables();

    /**
     * Returns the arguments provided to the shell context.
     * The first argument (index 0) is the name of the file.
     * For example, if a sub-shell was loaded with the following command: "source -s /path/to/file.cmd foo bar",
     * then arguments will return the following: "/path/to/file.cmd", "foo", "bar".
     *
     * @return the arguments provided to the shell context
     */
    List<DirectBuffer> getArguments();

    /**
     * Returns a command descriptor for the current path of the shell context.
     *
     * @return a command descriptor for the current path of the shell context
     */
    CommandDescriptor getPath();

    /**
     * Loads a file of commands to be processed by a new shell context.
     * The specified arguments are passed into the new shell context.
     * If {@code subShell} is set to true, changes to the variables inside the new shell context will not affect the
     * variables in this shell.
     *
     * @param filePath the path to the file containing the shell commands
     * @param args the arguments to pass into the new shell context
     * @param subShell true if a sub-shell is to be created and the parent shell will not be affected by additions or
     *     changes to variables in the sub-shell
     * @throws CommandException if an exception is thrown processing any of the commands
     * @throws IOException if the file could not be loaded
     */
    void loadFile(DirectBuffer filePath, List<DirectBuffer> args, boolean subShell) throws
            CommandException, IOException;

    /**
     * Executes a single command contained in the specified {@code buffer}.
     *
     * @param buffer the buffer containing the command
     * @param offset the first byte of the command
     * @param length the length of the command
     * @return the returned object
     * @throws IOException if an I/O error occurs
     * @throws CommandException if an exception occurs executing the command
     */
    Object executeInline(DirectBuffer buffer, int offset, int length) throws IOException, CommandException;

    /**
     * Executes a single command contained in the specified {@code buffer}.
     *
     * @param buffer the buffer containing the command
     * @param offset the first byte of the command
     * @param length the length of the command
     * @return the number of bytes in the buffer containing a command
     * @throws IOException if an I/O error occurs
     * @throws CommandException if an exception occurs executing the command
     */
    int execute(DirectBuffer buffer, int offset, int length) throws IOException, CommandException;

    /**
     * Closes the shell context.
     */
    void close();
}
