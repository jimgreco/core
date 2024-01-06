package com.core.platform.shell;

/**
 * An object that is registered with the command shell.
 *
 * <p>When registered, {@link #onRegistered(String) onRegistered} will be invoked with the path the object is registered
 * at.
 */
public interface CommandObject {

    /**
     * Invoked when this object is registered at the specified {@code path}.
     *
     * @param path the registered path
     * @throws CommandException if an error occurs
     */
    void onRegistered(String path) throws CommandException;
}
