package com.core.infrastructure.time;

import com.core.infrastructure.collections.IntrusiveLinkedList;
import com.core.infrastructure.collections.ObjectPool;
import com.core.infrastructure.collections.Resettable;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * The scheduler is used to schedule tasks to be executed at a specified time, after a period of duration, or to repeat
 * every period of duration.
 */
public class Scheduler implements Encodable {

    private final ScheduledTaskPredicate predicate;
    private final Time time;
    private final ScheduledExecutor scheduledExecutor;
    @Property
    private final ObjectPool<ScheduledTask> taskPool;
    @Property
    private final IntrusiveLinkedList<ScheduledTask> tasks;
    @Property
    private int taskId;

    /**
     * Creates a {@code Scheduler} with the specified object that is used as a source of {@code time} to schedule tasks.
     *
     * @param time the source of time to use in scheduling tasks
     */
    public Scheduler(Time time) {
        this.time = Objects.requireNonNull(time);
        scheduledExecutor = new ScheduledExecutor();
        tasks = new IntrusiveLinkedList<>();
        taskPool = new ObjectPool<>(ScheduledTask::new);
        predicate = new ScheduledTaskPredicate();
    }

    /**
     * Reschedules a task to be invoked aat a specific time, in nanoseconds since epoch, January 1, 1970.
     * Equivalent for invoking {@link #cancel(long) cancel(oldTaskId)} and then
     * {@link #scheduleAt(long, Runnable, String, int) scheduleAt(nanos, handle)}.
     *
     * @param oldTaskId the identifier of the task to cancel
     * @param nanos the period of time
     * @param handle the handle to invoke
     * @param source the user-provided name of the task
     * @param sourceId a user-provided identifier of the task
     * @return the identifier of the scheduled task
     */
    public long scheduleAt(long oldTaskId, long nanos, Runnable handle, String source, int sourceId) {
        cancel(oldTaskId);
        return scheduleAt(nanos, handle, source, sourceId);
    }

    /**
     * Schedules the specified {@code handle} to be invoked at the specified time, in nanoseconds since epoch, January
     * 1, 1970, unless the returned scheduled task is canceled.
     * The time period is not guaranteed and is subject to the precision of the {@code Time} object specified in the
     * constructor, among other potential sources of delay.
     *
     * @param nanos the period of time
     * @param handle the handle to invoke
     * @param source the user-provided name of the task
     * @param sourceId a user-provided identifier of the task
     * @return the identifier of the scheduled task
     * @throws IllegalArgumentException if {@code nanos} is non-positive
     */
    public long scheduleAt(long nanos, Runnable handle, String source, int sourceId) {
        if (nanos <= 0) {
            throw new IllegalArgumentException("nanos is non-positive");
        }

        return add(nanos, handle, source, sourceId).taskId;
    }

    /**
     * Reschedules a task to be invoked after the specified duration, in nanoseconds.
     * Equivalent for invoking {@link #cancel(long) cancel(oldTaskId)} and then
     * {@link #scheduleIn(long, Runnable, String, int) schedulerIn(nanos, handle)}.
     *
     * @param oldTaskId the identifier of the task to cancel
     * @param nanos the period of time
     * @param handle the handle to invoke
     * @param source the user-provided name of the task
     * @param sourceId a user-provided identifier of the task
     * @return the identifier of the scheduled task
     * @throws IllegalArgumentException if {@code nanos} is non-positive
     */
    public long scheduleIn(long oldTaskId, long nanos, Runnable handle, String source, int sourceId) {
        if (nanos < 0) {
            throw new IllegalArgumentException("nanos is negative");
        }

        cancel(oldTaskId);
        return scheduleIn(nanos, handle, source, sourceId);
    }

    /**
     * Schedules the specified {@code handle} to be invoked after the specification duration, in nanoseconds, unless the
     * returned scheduled task is canceled.
     * The time period is not guaranteed and is subject to the precision of the {@code Time} object specified in the
     * constructor, among other potential sources of delay.
     *
     * @param nanos the period of time
     * @param handle the handle to invoke
     * @param source the user-provided name of the task
     * @param sourceId a user-provided identifier of the task
     * @return the identifier of the scheduled task
     */
    public long scheduleIn(long nanos, Runnable handle, String source, int sourceId) {
        return add(time.nanos() + nanos, handle, source, sourceId).taskId;
    }

    /**
     * Schedules the specified {@code handle} to be invoked on the next scheduler fire, unless the
     * returned scheduled task is canceled.
     *
     * @param oldTaskId the identifier of the task to cancel
     * @param handle the handle to invoke
     * @param source the user-provided name of the task
     * @param sourceId a user-provided identifier of the task
     * @return the identifier of the scheduled task
     */
    public long scheduleNext(long oldTaskId, Runnable handle, String source, int sourceId) {
        cancel(oldTaskId);
        return add(time.nanos(), handle, source, sourceId).taskId;
    }

    /**
     * Schedules the specified {@code handle} to be invoked on the next scheduler fire, unless the
     * returned scheduled task is canceled.
     *
     * @param handle the handle to invoke
     * @param source the user-provided name of the task
     * @param sourceId a user-provided identifier of the task
     * @return the identifier of the scheduled task
     */
    public long scheduleNext(Runnable handle, String source, int sourceId) {
        return add(time.nanos(), handle, source, sourceId).taskId;
    }

    /**
     * Reschedules a task to be invoked after the specified duration, in nanoseconds, and every period duration after
     * that.
     * Equivalent for invoking {@link #cancel(long) cancel(oldTaskId)} and then
     * {@link #scheduleEvery(long, Runnable, String, int) schedulerEvery(nanos, handle)}.
     *
     * @param oldTaskId the identifier of the task to cancel
     * @param nanos the period of time
     * @param handle the handle to invoke
     * @param source the user-provided name of the task
     * @param sourceId a user-provided identifier of the task
     * @return the identifier of the scheduled task
     */
    public long scheduleEvery(long oldTaskId, long nanos, Runnable handle, String source, int sourceId) {
        cancel(oldTaskId);
        return scheduleEvery(nanos, handle, source, sourceId);
    }

    /**
     * Schedules the specified {@code handle} to be invoked after the specified duration, in nanoseconds, and every
     * period duration after that, until the returned scheduled task is canceled.
     * The time period is not guaranteed and is subject to the precision of the {@code Time} object specified in the
     * constructor, among other potential sources of delay.
     *
     * @param nanos the period of time
     * @param handle the handle to invoke
     * @param source the user-provided name of the task
     * @param sourceId a user-provided identifier of the task
     * @return the identifier of the scheduled task
     * @throws IllegalArgumentException if {@code nanos} is non-positive
     */
    public long scheduleEvery(long nanos, Runnable handle, String source, int sourceId) {
        if (nanos <= 0) {
            throw new IllegalArgumentException("nanos is non-positive");
        }

        var task = add(time.nanos() + nanos, handle, source, sourceId);
        task.repeatTime = nanos;
        return task.taskId;
    }

    /**
     * Cancels the specified scheduled {@code task}.
     *
     * @param taskId the identifier of the task to cancel
     * @return always zero
     */
    public long cancel(long taskId) {
        if (taskId > 0) {
            predicate.taskId = taskId;
            var task = tasks.removeFirst(predicate);
            if (task != null) {
                taskPool.returnObject(task);
            }
        }
        return 0;
    }

    /**
     * Executes all tasks up to and including the current time from the object specified in the constructor of this
     * class.
     * The nanoseconds until the next scheduled task is returned, or {@code Long.MAX_VALUE} if there are no tasks
     * scheduled.
     *
     * @return the nanoseconds until the next schedule task
     */
    public long fire() {
        scheduledExecutor.runTasks();
        var time = this.time.nanos();
        var maxTaskId = taskId;
        while (!tasks.isEmpty()) {
            var currentTask = tasks.getFirst();
            var currentTaskId = currentTask.taskId;

            if (time >= currentTask.time && currentTaskId <= maxTaskId) {
                if (currentTask.repeatTime > 0) {
                    currentTask.handle.run();

                    // checks that this task hasn't been modified in the callback
                    if (currentTask.taskId == currentTaskId) {
                        tasks.removeFirst();
                        currentTask.taskId = ++taskId;
                        currentTask.time = time + currentTask.repeatTime;
                        tasks.insert(currentTask);
                    }
                } else {
                    currentTask.handle.run();

                    // checks that this task hasn't been modified in the callback
                    if (currentTask.taskId == currentTaskId) {
                        tasks.removeFirst();
                        taskPool.returnObject(currentTask);
                    }
                }
            } else {
                return currentTask.time - time;
            }
        }
        return Long.MAX_VALUE;
    }

    private ScheduledTask add(long time, Runnable handle, String source, int sourceId) {
        var task = taskPool.borrowObject();
        task.taskId = ++taskId;
        task.origTaskId = task.taskId;
        task.handle = handle;
        task.time = time;
        task.source = source;
        task.sourceId = sourceId;
        tasks.insert(task);
        return task;
    }

    @Command(path = "status")
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("taskId").number(taskId)
                .string("outstandingTasks").number(tasks.size())
                .closeMap();
    }

    @Override
    public String toString() {
        return toEncodedString();
    }

    public Executor getScheduledExecutor() {
        return scheduledExecutor;
    }

    private static class ScheduledTaskPredicate implements Predicate<ScheduledTask> {

        long taskId;

        @Override
        public boolean test(ScheduledTask scheduledTask) {
            return taskId == scheduledTask.origTaskId;
        }
    }

    /**
     * A scheduled task is a task that is designed to be executed at a specified time in the future.
     */
    private static class ScheduledTask implements
            IntrusiveLinkedList.IntrusiveLinkedListItem<ScheduledTask>, Encodable, Resettable {

        String source;
        long taskId;
        long origTaskId;
        long time;
        long repeatTime;
        int sourceId;
        Runnable handle;

        private IntrusiveLinkedList.IntrusiveLinkedListItem<ScheduledTask> prev;
        private IntrusiveLinkedList.IntrusiveLinkedListItem<ScheduledTask> next;

        /**
         * Creates an empty {@code ScheduledTask}.
         */
        ScheduledTask() {
        }

        @Override
        public IntrusiveLinkedList.IntrusiveLinkedListItem<ScheduledTask> getPrevious() {
            return prev;
        }

        @Override
        public IntrusiveLinkedList.IntrusiveLinkedListItem<ScheduledTask> getNext() {
            return next;
        }

        @Override
        public void setPrevious(IntrusiveLinkedList.IntrusiveLinkedListItem<ScheduledTask> prev) {
            this.prev = prev;
        }

        @Override
        public void setNext(IntrusiveLinkedList.IntrusiveLinkedListItem<ScheduledTask> next) {
            this.next = next;
        }

        @Override
        public ScheduledTask getItem() {
            return this;
        }

        @Override
        public int compareTo(ScheduledTask o) {
            var result = Long.compare(time, o.time);
            return result == 0 ? Long.compare(taskId, o.taskId) : result;
        }

        /**
         * Resets all fields in the task.
         */
        @Override
        public void reset() {
            source = null;
            sourceId = 0;
            taskId = 0;
            origTaskId = 0;
            time = 0;
            repeatTime = 0;
            handle = null;
            prev = null;
            next = null;
        }

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.openMap()
                    .string("source").string(source)
                    .string("sourceId").number(sourceId)
                    .string("taskId").number(taskId)
                    .string("origTaskId").number(origTaskId)
                    .string("time").number(time)
                    .string("repeatTime").number(repeatTime)
                    .closeMap();
        }

        @Override
        public String toString() {
            return toEncodedString();
        }
    }

    static class ScheduledExecutor implements Executor {

        private final ManyToOneConcurrentLinkedQueue<Runnable> tasks;

        ScheduledExecutor() {
            tasks = new ManyToOneConcurrentLinkedQueue<>();
        }

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        void runTasks() {
            Runnable task;
            while ((task = tasks.poll()) != null) {
                task.run();
            }
        }
    }
}
