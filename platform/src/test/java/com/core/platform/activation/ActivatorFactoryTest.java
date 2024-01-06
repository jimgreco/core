package com.core.platform.activation;

import com.core.infrastructure.log.TestLogFactory;
import com.core.infrastructure.metrics.MetricFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.BDDAssertions.thenThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ActivatorFactoryTest {

    private ActivatorFactory activatorFactory;

    @BeforeEach
    void before_each() {
        var logFactory = new TestLogFactory();
        activatorFactory = new ActivatorFactory(logFactory, new MetricFactory(logFactory));
    }

    @Nested
    class ActivatorDependencyTests {

        @Test
        void addActivator_with_same_name_throws_IllegalArgumentException() {
            activatorFactory.createActivator("foo", new Object());

            thenThrownBy(() -> activatorFactory.createActivator("foo", new Object()))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void addActivator_with_same_object_throws_IllegalArgumentException() {
            var activator = new Object();
            activatorFactory.createActivator("foo", activator);

            thenThrownBy(() -> activatorFactory.createActivator("bar", activator))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void addActivator_with_dependency_not_activator_throws_IllegalArgumentException() {
            var dependency1 = new Object();
            activatorFactory.createActivator("dependency1", dependency1);
            var dependency2 = new Object();
            var dependency3 = new Object();
            activatorFactory.createActivator("dependency3", dependency3);

            thenThrownBy(() -> activatorFactory.createActivator(
                    "bar", new Object(), dependency1, dependency2, dependency3))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void do_not_stop_child_with_at_least_one_started_parent() {
            var child = new Object();
            var childActivator = activatorFactory.createActivator("child", child);
            childActivator.ready();
            var parent1 = new Object();
            var parent1Activator = activatorFactory.createActivator("parent1", parent1, child);
            parent1Activator.ready();
            parent1Activator.start();
            var parent2 = new Object();
            var parent2Activator = activatorFactory.createActivator("parent2", parent2, child);
            parent2Activator.start();

            parent1Activator.stop();

            then(parent1Activator.isActive()).isFalse();
            then(parent2Activator.isStarted()).isTrue();
            then(childActivator.isActive()).isTrue();
            then(childActivator.isStarted()).isTrue();
            then(childActivator.isReady()).isTrue();
        }

        @Test
        void deactivate_child_with_no_active_parents() {
            var child = new Object();
            var childActivator = activatorFactory.createActivator("child", child);
            childActivator.ready();
            var parent1 = new Object();
            var parent1Activator = activatorFactory.createActivator("parent1", parent1, child);
            parent1Activator.ready();
            parent1Activator.start();
            var parent2 = new Object();
            var parent2Activator = activatorFactory.createActivator("parent2", parent2, child);
            parent2Activator.ready();
            parent2Activator.start();
            parent1Activator.stop();

            parent2Activator.stop();

            then(parent1Activator.isActive()).isFalse();
            then(parent2Activator.isActive()).isFalse();
            then(childActivator.isActive()).isFalse();
        }
    }

    @Nested
    class NoDependenciesTests {

        @Test
        void initial_isReady_returns_false() {
            var activator = activatorFactory.createActivator("bar", new Object());

            then(activator.isReady()).isFalse();
            then(activator.isStarted()).isFalse();
            then(activator.isActive()).isFalse();
            then(activator.getNotReadyReason()).isEqualTo("initial state");
        }

        @Test
        void notReady_changes_notReadyReason() {
            var activator = activatorFactory.createActivator("bar", new Object());
            activator.notReady("this is");

            activator.notReady("bad");

            then(activator.isReady()).isFalse();
            then(activator.isStarted()).isFalse();
            then(activator.isActive()).isFalse();
            then(activator.getNotReadyReason()).isEqualTo("bad");
        }

        @Test
        void ready_isReady_returns_true() {
            var activator = activatorFactory.createActivator("bar", new Object());

            activator.ready();

            then(activator.isReady()).isTrue();
            then(activator.isStarted()).isFalse();
            then(activator.isActive()).isFalse();
            then(activator.getNotReadyReason()).isNull();
        }

        @Test
        void notReady_after_ready_returns_false() {
            var activator = activatorFactory.createActivator("bar", new Object());
            activator.ready();

            activator.notReady("wrong");

            then(activator.isReady()).isFalse();
            then(activator.isStarted()).isFalse();
            then(activator.isActive()).isFalse();
            then(activator.getNotReadyReason()).isEqualTo("wrong");
        }

        @Test
        void activator_with_dependencies() {
            var dependency1 = new Object();
            activatorFactory.createActivator("dependency1", dependency1);
            var dependency2 = new Object();
            activatorFactory.createActivator("dependency2", dependency2);
            var dependency3 = new Object();
            activatorFactory.createActivator("dependency3", dependency3);

            var activator = activatorFactory.createActivator(
                    "bar", new Object(), dependency1, dependency2, dependency3);

            then(activator.isReady()).isFalse();
            then(activator.isStarted()).isFalse();
            then(activator.isActive()).isFalse();
        }

        @Test
        void start_and_ready_a_nonactivatable_with_no_children_isActive_returns_true() {
            var activator = activatorFactory.createActivator("bar", new Object());
            activator.start();

            activator.ready();

            then(activator.isReady()).isTrue();
            then(activator.isStarted()).isTrue();
            then(activator.isActive()).isTrue();
        }

        @Test
        void ready_and_start_a_nonactivatable_with_no_children_isActive_returns_true() {
            var activator = activatorFactory.createActivator("bar", new Object());
            activator.ready();

            activator.start();

            then(activator.isReady()).isTrue();
            then(activator.isStarted()).isTrue();
            then(activator.isActive()).isTrue();
        }

        @Test
        void start_an_activatable_invokes_activatable() {
            var activatable = mock(Activatable.class);
            var activator = activatorFactory.createActivator("bar", activatable);

            activator.start();

            verify(activatable).activate();
            then(activator.isReady()).isFalse();
            then(activator.isStarted()).isTrue();
            then(activator.isActive()).isFalse();
            then(activator.isActivating()).isTrue();
        }

        @Test
        void throw_exception_during_activation_stops_activation() {
            var activatable = mock(Activatable.class);
            doAnswer(x -> {
                throw new RuntimeException("hi");
            }).when(activatable).activate();
            var activator = activatorFactory.createActivator("bar", activatable);

            activator.start();

            verify(activatable).activate();
            verify(activatable).deactivate();
            then(activator.isReady()).isFalse();
            then(activator.isStarted()).isFalse();
            then(activator.isActive()).isFalse();
        }
    }

    @Nested
    class NonActivatableChildTests {

        private Activator child1;
        private Activator child2;
        private Object child1Object;
        private Object child2Object;

        @BeforeEach
        void before_each() {
            child1Object = new Object();
            child2Object = new Object();
            child1 = activatorFactory.createActivator("child1", child1Object);
            child2 = activatorFactory.createActivator("child2", child2Object);
        }

        @Test
        void one_child_not_active_doesnt_activate() {
            child1.ready();
            child2.ready();
            var activatable = mock(Activatable.class);
            var activator = activatorFactory.createActivator(
                    "bar", activatable, child1Object, child2Object);

            activator.start();

            verify(activatable).activate();
            then(activator.isReady()).isFalse();
            then(activator.isStarted()).isTrue();
            then(activator.isActive()).isFalse();
            then(child1.isActive()).isTrue();
            then(child1.isReady()).isTrue();
            then(child1.isStarted()).isTrue();
            then(child2.isActive()).isTrue();
            then(child2.isReady()).isTrue();
            then(child2.isStarted()).isTrue();
        }
    }
}
