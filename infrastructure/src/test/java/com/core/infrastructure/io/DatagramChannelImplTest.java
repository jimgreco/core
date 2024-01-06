package com.core.infrastructure.io;

import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.assertj.core.api.BDDAssertions.then;

@Disabled
public class DatagramChannelImplTest {

    private NioSelector selector;
    private DatagramChannel channel1;
    private DatagramChannel channel2;
    private MutableDirectBuffer readBuffer;
    private int bytesRead;

    @BeforeEach
    void before_each() throws IOException, InterruptedException {
        selector = new NioSelector();
        channel1 = selector.createDatagramChannel();
        channel2 = selector.createDatagramChannel();

        var eventChannel = "inet:239.100.100.100:10100:lo0";
        channel1.join(eventChannel);
        channel1.connect(eventChannel);
        channel2.join(eventChannel);
        channel2.bind(eventChannel);
        //channel2.connect(eventChannel);
        channel2.setReadListener(this::onRead);
        Thread.sleep(100);

        readBuffer = BufferUtils.allocateDirect(100);
    }

    private void onRead() {
        try {
            bytesRead = channel2.receive(readBuffer);
            //bytesRead = channel2.read(readBuffer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void send() throws IOException, InterruptedException {
        var writeBuffer = BufferUtils.fromAsciiString("hi");
        channel1.write(writeBuffer);

        for (var i = 0; i < 10; i++) {
            Thread.sleep(100);
            selector.selectNow();
        }

        then(BufferUtils.toAsciiString(readBuffer, 0, bytesRead)).isEqualTo("hi");
    }
}
