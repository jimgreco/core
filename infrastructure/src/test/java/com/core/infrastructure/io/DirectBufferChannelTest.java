package com.core.infrastructure.io;

import com.core.infrastructure.buffer.BufferUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.BDDAssertions.thenThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class DirectBufferChannelTest {

    private DirectBufferChannel channel;

    @BeforeEach
    void before_each() {
        channel = new DirectBufferChannel(100);
    }

    @Test
    void initial_size_and_position_is_zero() throws IOException {
        then(channel.isOpen()).isTrue();
        then(channel.position()).isEqualTo(0);
        then(channel.size()).isEqualTo(0);
    }

    @Test
    void write_updates_position_and_size() throws IOException {
        channel.write(BufferUtils.fromAsciiString("himom"));
        channel.position(2);

        channel.write(BufferUtils.fromAsciiString("dad!"));

        then(channel.position()).isEqualTo(6);
        then(channel.size()).isEqualTo(6);
        var readBuffer = BufferUtils.allocate(100);
        channel.position(0);
        var bytesRead = channel.read(readBuffer);
        then(bytesRead).isEqualTo(6);
        then(readBuffer.getStringWithoutLengthAscii(0, 6)).isEqualTo("hidad!");
    }

    @Test
    void read_neg_1_bytes_at_end_of_file() throws IOException {
        channel.write(BufferUtils.fromAsciiString("himom"));

        var actual = channel.read(BufferUtils.allocate(10));

        then(actual).isEqualTo(-1);
    }

    @Test
    void close_isOpen_returns_false() throws IOException {
        channel.close();

        then(channel.isOpen()).isFalse();
    }

    @Test
    void close_causes_IOException_errors() throws IOException {
        channel.write(BufferUtils.fromAsciiString("himom"));
        channel.position(0);
        channel.close();

        thenThrownBy(() -> channel.position()).isInstanceOf(IOException.class);
        thenThrownBy(() -> channel.position(1)).isInstanceOf(IOException.class);
        thenThrownBy(() -> channel.size()).isInstanceOf(IOException.class);
        thenThrownBy(() -> channel.read(BufferUtils.allocate(10))).isInstanceOf(IOException.class);
        thenThrownBy(() -> channel.write(BufferUtils.allocate(10))).isInstanceOf(IOException.class);
        thenThrownBy(() -> channel.truncate(10)).isInstanceOf(IOException.class);
    }

    @Test
    void position_less_than_zero_throws_IllegalArgumentException() {
        thenThrownBy(() -> channel.position(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void position_greater_than_size_increases_size() throws IOException {
        channel.position(105);

        var written = channel.write(BufferUtils.fromAsciiString("hi there"));

        then(written).isEqualTo(8);
        then(channel.size()).isEqualTo(105 + 8);
        then(channel.position()).isEqualTo(105 + 8);
        var actual = BufferUtils.allocate(written);
        channel.position(105);
        channel.read(actual);
        then(BufferUtils.toAsciiString(actual)).isEqualTo("hi there");
    }

    @Test
    void truncate_smaller_than_size_reduces_size_of_file() throws IOException {
        channel.position(105);

        channel.truncate(50);

        then(channel.size()).isEqualTo(50);
        then(channel.position()).isEqualTo(50);
    }

    @Test
    void truncate_smaller_than_size_but_larger_than_position_reduces_size_of_file_keeps_position()
            throws IOException {
        channel.position(105);
        channel.position(30);
        channel.truncate(50);

        then(channel.size()).isEqualTo(50);
        then(channel.position()).isEqualTo(30);
    }

    @Test
    void truncate_larger_than_size_does_nothing() throws IOException {
        channel.position(105);

        channel.truncate(150);

        then(channel.size()).isEqualTo(105);
        then(channel.position()).isEqualTo(105);
    }

    @Test
    void truncate_negative_size_throws_IllegalArgumentException() throws IOException {
        thenThrownBy(() -> channel.truncate(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void readBuffer_fires_until_set_null() throws IOException {
        channel.write(BufferUtils.allocate(50));
        channel.position(0);
        var read = mock(Runnable.class);
        doAnswer(x -> {
            channel.read(BufferUtils.allocate(2));
            return null;
        }).doAnswer(x -> {
            channel.read(BufferUtils.allocate(1));
            return null;
        }).doAnswer(x -> {
            channel.read(BufferUtils.allocate(3));
            channel.setReadListener(null);
            return null;
        }).when(read).run();

        channel.setReadListener(read);

        then(channel.position()).isEqualTo(6);
    }

    @Test
    void writeBuffer_fires_until_set_null() throws IOException {
        var write = mock(Runnable.class);
        doAnswer(x -> {
            channel.write(BufferUtils.allocate(2));
            return null;
        }).doAnswer(x -> {
            channel.write(BufferUtils.allocate(1));
            return null;
        }).doAnswer(x -> {
            channel.write(BufferUtils.allocate(3));
            channel.setWriteListener(null);
            return null;
        }).when(write).run();

        channel.setWriteListener(write);

        then(channel.position()).isEqualTo(6);
        then(channel.size()).isEqualTo(6);
    }
}
