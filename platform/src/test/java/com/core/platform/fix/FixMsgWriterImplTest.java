package com.core.platform.fix;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.time.ManualTime;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.function.Consumer;

import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class FixMsgWriterImplTest {

    private String fixMsgString;
    private ManualTime time;
    private Consumer<DirectBuffer> listener;
    private FixMsgWriterImpl writer;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void before_each() {
        listener = mock(Consumer.class);
        doAnswer(x -> {
            var buf = x.getArgument(0);
            fixMsgString = FixUtils.toFixString((DirectBuffer) buf);
            return null;
        }).when(listener).accept(any());
        time = new ManualTime(ZonedDateTime.of(
                LocalDateTime.of(2021, 5, 26, 9, 30, 5, 123456789),
                ZoneOffset.UTC));
        writer = new FixMsgWriterImpl(time, new FixSessionConfiguration(
                FixVersion.FIX42, "SENDER", "TARGET"), listener);
    }

    @Test
    void write_tags() {
        writer.start(BufferUtils.fromAsciiString("AB"), 1)
                .putInteger(38, 1)
                .putDecimal(44, 1.23)
                .putAsciiString(58, "HI")
                .putBuffer(200, BufferUtils.fromAsciiString("FOO"))
                .putBuffer(201, BufferUtils.fromAsciiString("ABARC"), 1, 3)
                .putAsciiCharacter(202, 'Y');
        time.advanceTime(Duration.ofSeconds(1));
        writer.putTimestamp(60, time.nanos())
                .send();

        var fixMsg = FixMsg.parse(fixMsgString);
        then(fixMsg.getTagAt(0)).isEqualTo(8);
        then(fixMsg.getTagAt(1)).isEqualTo(9);
        then(fixMsg.getTagAt(2)).isEqualTo(35);
        then(fixMsg.getTagAt(3)).isEqualTo(49);
        then(fixMsg.getTagAt(4)).isEqualTo(56);
        then(fixMsg.getTagAt(5)).isEqualTo(34);
        then(fixMsg.getTagAt(6)).isEqualTo(52);
        then(fixMsg.getTagAt(7)).isEqualTo(38);
        then(fixMsg.getTagAt(8)).isEqualTo(44);
        then(fixMsg.getTagAt(9)).isEqualTo(58);
        then(fixMsg.getTagAt(10)).isEqualTo(200);
        then(fixMsg.getTagAt(11)).isEqualTo(201);
        then(fixMsg.getTagAt(12)).isEqualTo(202);
        then(fixMsg.getTagAt(13)).isEqualTo(60);
        then(fixMsg.getTagAt(14)).isEqualTo(10);

        then(fixMsg.getValueAt(0)).isEqualTo(BufferUtils.fromAsciiString("FIX.4.2"));
        //then(fixMsg.getValueAt(1)).isEqualTo();
        then(fixMsg.getValueAt(2)).isEqualTo(BufferUtils.fromAsciiString("AB"));
        then(fixMsg.getValueAt(3)).isEqualTo(BufferUtils.fromAsciiString("SENDER"));
        then(fixMsg.getValueAt(4)).isEqualTo(BufferUtils.fromAsciiString("TARGET"));
        then(fixMsg.getValueAt(5)).isEqualTo(BufferUtils.fromAsciiString("1"));
        then(fixMsg.getValueAt(6)).isEqualTo(BufferUtils.fromAsciiString("20210526-09:30:05.123"));
        then(fixMsg.getValueAt(7)).isEqualTo(BufferUtils.fromAsciiString("1"));
        then(fixMsg.getValueAt(8)).isEqualTo(BufferUtils.fromAsciiString("1.23"));
        then(fixMsg.getValueAt(9)).isEqualTo(BufferUtils.fromAsciiString("HI"));
        then(fixMsg.getValueAt(10)).isEqualTo(BufferUtils.fromAsciiString("FOO"));
        then(fixMsg.getValueAt(11)).isEqualTo(BufferUtils.fromAsciiString("BAR"));
        then(fixMsg.getValueAt(12)).isEqualTo(BufferUtils.fromAsciiString("Y"));
        then(fixMsg.getValueAt(13)).isEqualTo(BufferUtils.fromAsciiString("20210526-09:30:06.123"));
        //then(fixMsg.getValueAt(14)).isEqualTo();

        then(fixMsg.getSize()).isEqualTo(15);
    }
}
