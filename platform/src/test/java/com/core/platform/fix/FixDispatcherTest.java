package com.core.platform.fix;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.log.TestLogFactory;
import com.core.infrastructure.time.ManualTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.function.Consumer;

import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class FixDispatcherTest {

    private ManualTime time;
    private FixDispatcher dispatcher;
    private Consumer<FixMsg> logonListener;
    private Consumer<FixMsg> heartbeatListener;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void before_each() {
        time = new ManualTime(ZonedDateTime.of(
                LocalDate.of(2021, 5, 4),
                LocalTime.of(9, 30, 0),
                ZoneOffset.UTC));
        dispatcher = new FixDispatcher(time, new TestLogFactory(), fixMsg -> {});
        logonListener = mock(Consumer.class);
        heartbeatListener = mock(Consumer.class);
        dispatcher.setListener(BufferUtils.fromAsciiString("A"), logonListener);
        dispatcher.setListener(BufferUtils.fromAsciiString("0"), heartbeatListener);
    }

    @Test
    void last_received_nanos_is_time_value() {
        time.setNanos(1234);

        dispatcher.onFixMsg(FixMsg.parse("8=FIX.4.2|9=100|35=A|49=SENDER|56=TARGET|34=1|10=100|"));

        then(dispatcher.getInboundMsgTime()).isEqualTo(1234);
    }

    @Test
    void initial_state() {
        then(dispatcher.getInboundMsgSeqNum()).isEqualTo(1);
        then(dispatcher.isLogon()).isFalse();
    }

    @Test
    void setInboundMsgSeqNum_sets_inboundMsgSeqNum() {
        dispatcher.setInboundSeqNum(15);

        then(dispatcher.getInboundMsgSeqNum()).isEqualTo(15);
    }

    @Test
    void logon_sets_logon() {
        dispatcher.logon();

        then(dispatcher.isLogon()).isTrue();
    }

    @Test
    void logoff_resets_state() {
        dispatcher.logon();
        dispatcher.setInboundSeqNum(15);
        dispatcher.onFixMsg(FixMsg.parse("8=FIX.4.2|9=100|35=A|49=SENDER|56=TARGET|34=15|10=100|"));

        dispatcher.logoff(true);

        then(dispatcher.isLogon()).isFalse();
        then(dispatcher.getInboundMsgSeqNum()).isEqualTo(1);
        then(dispatcher.getInboundMsgTime()).isEqualTo(0);
    }

    @Test
    void logoff_false_resetInboundSeqNum_resets_all_state_but_inboundSeqNum() {
        dispatcher.logon();
        dispatcher.setInboundSeqNum(15);
        dispatcher.onFixMsg(FixMsg.parse("8=FIX.4.2|9=100|35=A|49=SENDER|56=TARGET|34=15|10=100|"));

        dispatcher.logoff(false);

        then(dispatcher.isLogon()).isFalse();
        then(dispatcher.getInboundMsgSeqNum()).isEqualTo(16);
        then(dispatcher.getInboundMsgTime()).isEqualTo(0);
    }

    @Test
    void no_MsgSeqNum_field_returns_false() {
        var actual = dispatcher.onFixMsg(FixMsg.parse("8=FIX.4.2|9=100|35=A|49=SENDER|56=TARGET|10=100|"));

        then(actual).isFalse();
        then(dispatcher.getInboundMsgSeqNum()).isEqualTo(1);
        verifyNoInteractions(logonListener);
    }

    @Test
    void too_low_msgSeqNum_returns_false() {
        dispatcher.setInboundSeqNum(15);

        var actual = dispatcher.onFixMsg(FixMsg.parse("8=FIX.4.2|9=100|35=A|49=SENDER|56=TARGET|34=14|10=100|"));

        then(actual).isFalse();
        then(dispatcher.getInboundMsgSeqNum()).isEqualTo(15);
        verifyNoInteractions(logonListener);
    }

    @Test
    void first_message_not_logon_returns_false() {
        var actual = dispatcher.onFixMsg(FixMsg.parse("8=FIX.4.2|9=100|35=1|49=SENDER|56=TARGET|34=1|10=100|"));

        then(actual).isFalse();
        then(dispatcher.getInboundMsgSeqNum()).isEqualTo(1);
        verifyNoInteractions(logonListener);
    }

    @Test
    void expected_logon_msgSeqNum_increments_next_inboundMsgSeqNum() {
        var actual = dispatcher.onFixMsg(FixMsg.parse("8=FIX.4.2|9=100|35=A|49=SENDER|56=TARGET|34=1|10=100|"));

        then(actual).isTrue();
        then(dispatcher.getInboundMsgSeqNum()).isEqualTo(2);
        verify(logonListener).accept(any());
    }

    @Test
    void expected_logon_too_high_msgSeqNum_does_not_increment_next_inboundMsgSeqNum() {
        var actual = dispatcher.onFixMsg(FixMsg.parse("8=FIX.4.2|9=100|35=A|49=SENDER|56=TARGET|34=2|10=100|"));

        then(actual).isTrue();
        then(dispatcher.getInboundMsgSeqNum()).isEqualTo(1);
        verify(logonListener).accept(any());
    }

    @Test
    void after_logon_disconnect_if_incorrect_msgSeqNum() {
        dispatcher.logon();
        dispatcher.setInboundSeqNum(5);

        var actual = dispatcher.onFixMsg(FixMsg.parse("8=FIX.4.2|9=100|35=0|49=SENDER|56=TARGET|34=6|10=100|"));

        then(actual).isFalse();
        then(dispatcher.getInboundMsgSeqNum()).isEqualTo(5);
        verifyNoInteractions(heartbeatListener);
    }

    @Test
    void after_logon_msgSeqNum_increments_msgSeqNum() {
        dispatcher.logon();
        dispatcher.setInboundSeqNum(5);

        var actual = dispatcher.onFixMsg(FixMsg.parse("8=FIX.4.2|9=100|35=0|49=SENDER|56=TARGET|34=5|10=100|"));

        then(actual).isTrue();
        then(dispatcher.getInboundMsgSeqNum()).isEqualTo(6);
        verify(heartbeatListener).accept(any());
    }
}
