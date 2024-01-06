package com.core.infrastructure;

import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.time.Scheduler;
import com.core.infrastructure.time.Time;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * The event loop coordinates a selector and task scheduler.
 *
 * <p>The event loop runs continuously until {@link #exit()} is invoked.
 * First, the event loop queries the tasks scheduler.
 * The scheduler will fire all scheduled tasks at or before the current time and return the time until the next task is
 * to be fired.
 * Second, the event loop selects on the selector.
 * If the event service is in busy poll mode or the next task will occur in less than a millisecond then a non-blocking
 * select is done.
 * If the event service is not in busy poll mode, there is no next scheduled task, or the next task will occur in a
 * millisecond or more then a blocking select is done.
 *
 * <p>Separately, a single iteration of the event loop can be invoked manually using the {@link #runOnce()} method.
 */
public class EventLoop {

    private static final long NANOS_PER_MILLI = TimeUnit.MILLISECONDS.toNanos(1);

    private final Scheduler scheduler;
    private final Selector selector;
    private final Time time;
    private boolean done;
    /**
     * Checks whether the event loop should perform non-blocking selects on the selector.
     */
    @Property(write = true)
    private boolean busyPoll;
    /**
     * Checks whether the event loop should fire events only with no selects, eg for file playback.
     */
    @Property(write = true)
    private boolean eventsOnly;

    /**
     * Creates an {@code EventLoop} with a {@code selector} used to register NIO components and a task {@code scheduler}
     * that the event service will fire before each selection operation.
     *
     * @param time the time
     * @param scheduler the task scheduler
     * @param selector the selector
     */
    public EventLoop(Time time, Scheduler scheduler, Selector selector) {
        this.time = Objects.requireNonNull(time, "time is null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler is null");
        this.selector = Objects.requireNonNull(selector, "selectService is null");
    }

    /**
     * Runs the event loop until the {@code exit()} method is invoked or an I/O error occurs.
     *
     * @throws IOException if an I/O error occurs
     */
    public void run() throws IOException {
        while (!done) {
            time.updateTime();

            if (busyPoll) {
                scheduler.fire();
                selector.selectNow();
            } else {
                var nanosToNextTask = scheduler.fire();
                if (nanosToNextTask == Long.MAX_VALUE) {
                    selector.select();
                } else {
                    if (nanosToNextTask < NANOS_PER_MILLI) {
                        selector.selectNow();
                    } else {
                        selector.select(nanosToNextTask);
                    }
                }
            }
        }
    }

    /**
     * Executes the event loop once, firing the scheduler and doing a non-blocking select on the selector.
     *
     * @throws IOException if an I/O error occurs
     */
    public void runOnce() throws IOException {
        scheduler.fire();
        if (!eventsOnly) {
            selector.selectNow();
        }
    }

    /**
     * Exits the event loop.
     */
    @Command
    public void exit() {
        done = true;
    }

    /**
     * Returns true if the event loop is done.
     *
     * @return true if the event loop is done
     */
    @Command
    public boolean isDone() {
        return done;
    }
}
