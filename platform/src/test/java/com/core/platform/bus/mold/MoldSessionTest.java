package com.core.platform.bus.mold;

import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.log.TestLogFactory;
import com.core.infrastructure.time.ManualTime;
import com.core.infrastructure.metrics.MetricFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.BDDAssertions.thenThrownBy;

public class MoldSessionTest {

    private MoldSession session;
    private ActivatorFactory activatorFactory;
    private Activator activator;

    @BeforeEach
    void before_each() {
        var logFactory = new TestLogFactory();
        activatorFactory = new ActivatorFactory(logFactory, new MetricFactory(logFactory));
        var time = new ManualTime(ZonedDateTime.of(
                2020, 12, 25,
                9, 30, 0, 0,
                ZoneOffset.UTC));
        session = new MoldSession("MoldSession", time, activatorFactory);
        activator = activatorFactory.getActivator(session);
    }

    @Test
    void initial_activator_state_is_not_ready() {
        then(session.getSessionName()).isNull();
        then(activator.isStarted()).isFalse();
        then(activator.isReady()).isFalse();
        then(activator.isActive()).isFalse();
    }

    @Test
    void createSession() {
        session.create(BufferUtils.fromAsciiString("AX"));

        then(session.getSessionName()).isEqualTo(BufferUtils.fromAsciiString("20201225AX"));
        then(activator.isReady()).isTrue();
    }

    @Test
    void createSession_after_session_is_set() {
        session.setSessionName(BufferUtils.fromAsciiString("20201225AX"));

        thenThrownBy(() -> session.create(BufferUtils.fromAsciiString("AX")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void setSession_after_create_session_is_IllegalStateException() {
        session.create(BufferUtils.fromAsciiString("AX"));

        thenThrownBy(() -> session.setSessionName(BufferUtils.fromAsciiString("20201225AX")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void initial_nextSequenceNumber_is_1() {
        then(session.getNextSequenceNumber()).isEqualTo(1);
    }

    @Test
    void set_nextSequenceNumber() {
        session.setNextSequenceNumber(53);

        then(session.getNextSequenceNumber()).isEqualTo(53);
    }

    @Test
    void setSessionName() {
        session.setSessionName(BufferUtils.fromAsciiString("hi"));

        then(session.getSessionName()).isEqualTo(BufferUtils.fromAsciiString("hi"));
        then(activator.isReady()).isTrue();
    }
}
