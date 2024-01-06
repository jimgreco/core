package com.core.platform.bus.mold;

import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.BDDAssertions.thenThrownBy;

class BufferChannelMessageStoreTest {

    private ChannelMessageStore eventStore;
    private MutableDirectBuffer readBuffer;

    @BeforeEach
    void before_each() throws IOException {
        eventStore = new BufferChannelMessageStore();
        eventStore.open(BufferUtils.fromAsciiString("20210101AA"));
        readBuffer = BufferUtils.allocate(100);
    }

    @Test
    void initial_number_of_events_is_zero() {
        then(eventStore.getNumMessages()).isEqualTo(0);
    }

    @Test
    void commit_increases_number_of_events() throws IOException {
        writeEvents();

        then(eventStore.getNumMessages()).isEqualTo(3);
    }

    @Test
    void read_one_event() throws IOException {
        writeEvents();

        then(eventStore.read(readBuffer, 0, 1)).isEqualTo(3);
        then(BufferUtils.toAsciiString(readBuffer, 0, 3)).isEqualTo("FOO");
        then(eventStore.read(readBuffer, 15, 2)).isEqualTo(6);
        then(BufferUtils.toAsciiString(readBuffer, 15, 6)).isEqualTo("BARSOO");
        then(eventStore.read(readBuffer, 1, 3)).isEqualTo(2);
        then(BufferUtils.toAsciiString(readBuffer, 1, 2)).isEqualTo("ME");
    }

    @Test
    void read_events_out_of_order() throws IOException {
        writeEvents();

        then(eventStore.read(readBuffer, 15, 2)).isEqualTo(6);
        then(BufferUtils.toAsciiString(readBuffer, 15, 6)).isEqualTo("BARSOO");
        then(eventStore.read(readBuffer, 1, 3)).isEqualTo(2);
        then(BufferUtils.toAsciiString(readBuffer, 1, 2)).isEqualTo("ME");
        then(eventStore.read(readBuffer, 0, 1)).isEqualTo(3);
        then(BufferUtils.toAsciiString(readBuffer, 0, 3)).isEqualTo("FOO");
    }

    @Test
    void read_event_0_throws_IllegalArgumentException() throws IOException {
        writeEvents();

        thenThrownBy(() -> eventStore.read(readBuffer, 0, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void read_event_beyond_number_of_events_throws_IllegalArgumentException() throws IOException {
        writeEvents();

        thenThrownBy(() -> eventStore.read(readBuffer, 4, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    private void writeEvents() throws IOException {
        var events = eventStore.acquire();
        events.putStringWithoutLengthAscii(0, "FOO");
        events.putStringWithoutLengthAscii(3, "BARSOO");
        events.putStringWithoutLengthAscii(9, "ME");

        eventStore.commit(new int[]{3, 6, 2}, 0, 3);
    }
}