package com.core.infrastructure.scheduler;

import com.core.infrastructure.time.ManualTime;
import com.core.infrastructure.time.Scheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class SchedulerTest {

    private ManualTime timeSource;
    private Scheduler scheduler;

    @BeforeEach
    void before_each() {
        timeSource = new ManualTime(LocalTime.of(9, 30));
        scheduler = new Scheduler(timeSource);
    }

    @Test
    void do_not_execute_until_scheduled_time() {
        var runnable1 = mock(Runnable.class);
        var runnable2 = mock(Runnable.class);
        scheduler.scheduleIn(TimeUnit.SECONDS.toNanos(5), runnable1, null, 0);
        scheduler.scheduleIn(TimeUnit.SECONDS.toNanos(7), runnable2, null, 0);
        timeSource.advanceTime(Duration.ofSeconds(4));

        scheduler.fire();

        verifyNoInteractions(runnable1, runnable2);
    }

    @Test
    void execute_at_scheduled_time() {
        var runnable1 = mock(Runnable.class);
        var runnable2 = mock(Runnable.class);
        scheduler.scheduleIn(TimeUnit.SECONDS.toNanos(5), runnable1, null, 0);
        scheduler.scheduleIn(TimeUnit.SECONDS.toNanos(7), runnable2, null, 0);
        timeSource.advanceTime(Duration.ofSeconds(5));

        scheduler.fire();

        verify(runnable1).run();
        verifyNoInteractions(runnable2);
    }

    @Test
    void do_not_execute_canceled_timer() {
        var runnable1 = mock(Runnable.class);
        var task1 = scheduler.scheduleIn(TimeUnit.SECONDS.toNanos(5), runnable1, null, 0);
        scheduler.scheduleIn(TimeUnit.SECONDS.toNanos(5), runnable1, null, 0);

        var actual = scheduler.cancel(task1);

        scheduler.fire();
        verifyNoInteractions(runnable1);
        then(actual).isEqualTo(0);
    }

    @Test
    void execute_past_scheduled_time() {
        var runnable1 = mock(Runnable.class);
        var runnable2 = mock(Runnable.class);
        scheduler.scheduleIn(TimeUnit.SECONDS.toNanos(5), runnable1, null, 0);
        scheduler.scheduleIn(TimeUnit.SECONDS.toNanos(7), runnable2, null, 0);
        timeSource.advanceTime(Duration.ofSeconds(6));

        scheduler.fire();

        verify(runnable1).run();
        verifyNoInteractions(runnable2);
    }

    @Test
    void execute_multiple_timers() {
        var runnable1 = mock(Runnable.class);
        var runnable2 = mock(Runnable.class);
        var runnable3 = mock(Runnable.class);
        scheduler.scheduleIn(TimeUnit.SECONDS.toNanos(5), runnable1, null, 0);
        scheduler.scheduleIn(TimeUnit.SECONDS.toNanos(7), runnable2, null, 0);
        scheduler.scheduleIn(TimeUnit.SECONDS.toNanos(9), runnable3, null, 0);
        timeSource.advanceTime(Duration.ofSeconds(7));

        scheduler.fire();

        verify(runnable1).run();
        verify(runnable2).run();
        verifyNoInteractions(runnable3);
    }

    @Test
    void scheduleEvery_fire_again_invokes_handler() {
        var runnable1 = mock(Runnable.class);
        var runnable2 = mock(Runnable.class);
        var runnable3 = mock(Runnable.class);
        scheduler.scheduleEvery(TimeUnit.SECONDS.toNanos(5), runnable1, null, 0);
        scheduler.scheduleIn(TimeUnit.SECONDS.toNanos(7), runnable2, null, 0);
        scheduler.scheduleIn(TimeUnit.SECONDS.toNanos(12), runnable3, null, 0);
        timeSource.advanceTime(Duration.ofSeconds(5));
        scheduler.fire();
        timeSource.advanceTime(Duration.ofSeconds(5));

        scheduler.fire();

        verify(runnable1, times(2)).run();
        verify(runnable2).run();
        verifyNoInteractions(runnable3);
    }

    @Test
    void schedule_timer_during_callback_does_not_fire() {
        var runnable1 = mock(Runnable.class);
        var runnable2 = mock(Runnable.class);
        var runnable3 = mock(Runnable.class);
        doAnswer(x -> {
            scheduler.scheduleIn(0, runnable3, null, 0);
            return null;
        }).when(runnable1).run();
        scheduler.scheduleEvery(TimeUnit.SECONDS.toNanos(5), runnable1, null, 0);
        scheduler.scheduleEvery(TimeUnit.SECONDS.toNanos(7), runnable2, null, 0);

        timeSource.advanceTime(Duration.ofSeconds(8));

        scheduler.fire();

        verify(runnable1).run();
        verify(runnable2).run();
        verifyNoInteractions(runnable3);
    }

    @Test
    void schedule_timer_after_second_fire() {
        var runnable1 = mock(Runnable.class);
        var runnable2 = mock(Runnable.class);
        var runnable3 = mock(Runnable.class);
        doAnswer(x -> {
            scheduler.scheduleIn(0, runnable3, null, 0);
            return null;
        }).when(runnable1).run();
        scheduler.scheduleEvery(TimeUnit.SECONDS.toNanos(5), runnable1, null, 0);
        scheduler.scheduleEvery(TimeUnit.SECONDS.toNanos(7), runnable2, null, 0);

        timeSource.advanceTime(Duration.ofSeconds(8));

        scheduler.fire();

        verify(runnable1).run();
        verify(runnable2).run();
        verifyNoInteractions(runnable3);
    }

    @Nested
    class CancelTimers {

        @Test
        void do_not_execute_canceled_timer() {
            var runnable1 = mock(Runnable.class);
            var runnable2 = mock(Runnable.class);
            var task1 = scheduler.scheduleIn(TimeUnit.SECONDS.toNanos(5), runnable1, null, 0);
            scheduler.scheduleIn(TimeUnit.SECONDS.toNanos(5), runnable2, null, 0);
            timeSource.advanceTime(Duration.ofSeconds(5));

            var actual = scheduler.cancel(task1);

            scheduler.fire();
            verifyNoInteractions(runnable1);
            then(actual).isEqualTo(0);
        }

        @Test
        void reschedule_scheduleIn_cancels_timer_and_schedules_new() {
            var runnable1 = mock(Runnable.class);
            var runnable2 = mock(Runnable.class);
            var task1 = scheduler.scheduleIn(TimeUnit.SECONDS.toNanos(5), runnable1, null, 0);

            scheduler.scheduleIn(task1, TimeUnit.SECONDS.toNanos(5), runnable2, null, 0);

            timeSource.advanceTime(Duration.ofSeconds(5));
            scheduler.fire();
            verifyNoInteractions(runnable1);
            verify(runnable2).run();
        }

        @Test
        void reschedule_scheduleEvery_cancels_timer_and_schedules_new() {
            var runnable1 = mock(Runnable.class);
            var runnable2 = mock(Runnable.class);
            var task1 = scheduler.scheduleEvery(TimeUnit.SECONDS.toNanos(5), runnable1, null, 0);

            scheduler.scheduleEvery(task1, TimeUnit.SECONDS.toNanos(5), runnable2, null, 0);

            timeSource.advanceTime(Duration.ofSeconds(5));
            scheduler.fire();
            verifyNoInteractions(runnable1);
            verify(runnable2).run();
        }
    }
}
