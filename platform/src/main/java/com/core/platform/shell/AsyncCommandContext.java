package com.core.platform.shell;

import com.core.infrastructure.encoding.ObjectEncoder;

import java.util.function.Consumer;

/**
 * The {@code AsyncCommandContext} is used to communicate the results of a command asynchronously.
 * More than one result can be communicated with the context.
 * With this, the user can stream results to the client.
 *
 * <p>To start encoding, invoke the {@link #start(Object, Consumer)}  start} method with a listener to be invoked when
 * the encoding is stopped (either by the application doing the encoding or from a client disconnect).
 * Maps, lists, strings, numbers, booleans, or nulls can then be encoded with the {@link ObjectEncoder}.
 * {@link #setObjectType(String) type} which the protocol will send if supported.
 * To stop encoding, invoke {@link #stop()}.
 *
 * <p>Some protocols have object identifiers and types that can be specified with the
 * {@link #setObjectId(long) setObjectId} or {@link #setObjectType(String) setObjectType} methods.
 * This is additional metadata that is optionally added to messages on some protocols.
 */
public interface AsyncCommandContext {

    /**
     * Starts encoding results to be returned asynchronously.
     */
    void start();

    /**
     * Starts encoding results to be returned asynchronously.
     *
     * @param associatedObject object associated with the object
     */
    void start(Object associatedObject);

    /**
     * Starts encoding results to be returned asynchronously.
     *
     * @param associatedObject the object associated with the context
     * @param closedListener the listener to be invoked when this context is closed
     */
    void start(Object associatedObject, Consumer<AsyncCommandContext> closedListener);

    /**
     * Sets a task to fire after the specified number of {@code nanoseconds} and a period of every {@code nanoseconds}
     * after that.
     *
     * @param nanoseconds the number of nanoseconds to schedule the task for
     * @param task the task
     * @param name a friendly name for the task in the scheduler
     */
    void setRefreshTask(long nanoseconds, Consumer<AsyncCommandContext> task, String name);

    /**
     * Cancels the the specified refresh task.
     */
    void cancelRefreshTask();

    /**
     * Returns the object associated with this context, specified in the start.
     *
     * @return the object associated with this context, specified in the start
     */
    Object getAssociatedObject();

    /**
     * Stops encoding results.
     */
    void stop();

    /**
     * Returns the {@code ObjectEncoder} to use in encoding results to be returned asynchronously.
     *
     * @return the {@code ObjectEncoder} to use in encoding results to be returned asynchronously
     */
    ObjectEncoder getObjectEncoder();

    /**
     * Returns the identifier of the next encoded object.
     *
     * @return the identifier of the next encoded object
     */
    long getObjectId();

    /**
     * Sets the identifier of the next encoded object.
     *
     * @param objectId the identifier of the next encoded object
     */
    void setObjectId(long objectId);

    /**
     * Sets the type of the next encoded object.
     *
     * @param objectType the type of the next encoded object.
     */
    void setObjectType(String objectType);
}
