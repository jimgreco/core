package com.core.platform.shell;

/**
 * An exception thrown processing of a command.
 * Command exceptions occur because the command is not recognized, the command path is invalid, the incorrect number of
 * parameters are specified, the parameter couldn't be casted to the correct type, there was an internal error
 * processing the command, or another reason.
 */
public class CommandException extends Exception {

    /**
     * The index of the byte in the input buffer that caused the exception to be thrown.
     */
    private int position;

    CommandException(String message) {
        super(message);
    }

    CommandException(String message, Throwable e) {
        super(message, e);
    }

    CommandException(String message, int position) {
        super(message);
        this.position = position;
    }

    /**
     * Returns the index of the byte in the input buffer that caused the exception to be thrown.
     *
     * @return the index of the byte in the input buffer that caused the exception to be thrown
     */
    public int getPosition() {
        return position;
    }
}
