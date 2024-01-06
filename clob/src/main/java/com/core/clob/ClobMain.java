package com.core.clob;

import com.core.platform.Main;
import com.core.platform.shell.CommandException;

import java.io.IOException;

/**
 * The main entry point for running the CLOB example applications.
 * In production, {@link Main} should be used, but this makes running examples in the IDE easier.}
 *
 * @see Main for program arguments
 */
public class ClobMain {

    /**
     * Starts a core virtual machine.
     *
     * @param args the arguments to the program
     * @throws IOException if a specified file could not be found
     * @throws CommandException if there is an error processing a command
     * @see Main for program arguments
     */
    public static void main(String[] args) throws IOException, CommandException {
        Main.main(args);
    }
}
