package com.core.platform.activation;

/**
 * An activatable is an object that can perform actions (e.g., opening a socket, completing the logon process with a
 * FIX counterparty) before being set active.
 *
 * <p>An object is activated when the {@code Activator} associated with the object is started and all its dependencies
 * are active.
 * When the object is being activated, the {@link #activate()} method is invoked.
 * The object can then perform any required actions before invoking {@code ready()} on the associated activator.
 * Actions can be asynchronous and are not required to be completed in the {@code activate()} method call.
 *
 * <p>An object is deactivated when the {@code Activator} associated with the object is stopped or any of its
 * dependencies go inactive.
 * When the object is being deactivated, the {@link #deactivate()} method is invoked.
 * The object can then perform any required actions before invoking {@code notReady()} on the associated activator.
 * Actions can be asynchronous and are not required be completed in the {@code deactivate()} method call.
 */
public interface Activatable {

    /**
     * Invoked when the activator associated with this object is started and all dependencies are active.
     */
    void activate();

    /**
     * Invoked when the activator associated with this object is stopped or any of the children are deactivated.
     */
    void deactivate();
}
