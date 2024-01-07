package com.core.platform.applications.sequencer;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.EventLoop;
import com.core.infrastructure.collections.CoreMap;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.log.TestLogFactory;
import com.core.infrastructure.metrics.MetricFactory;
import com.core.infrastructure.time.ManualTime;
import com.core.infrastructure.time.Scheduler;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.bus.TestBusServer;
import com.core.platform.bus.TestMessagePublisher;
import com.core.platform.schema.ApplicationDefinitionDecoder;
import com.core.platform.schema.ApplicationDefinitionEncoder;
import com.core.platform.schema.HeartbeatDecoder;
import com.core.platform.schema.TestDispatcher;
import com.core.platform.schema.TestProvider;
import com.core.platform.schema.TestSchema;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Map;

import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.Mockito.mock;

public class SequencerTest {

    private TestBusServer<TestDispatcher, TestProvider> busServer;
    private ManualTime time;
    private EventLoop eventLoop;
    private Sequencer sequencer;
    private TestSchema schema;
    private TestMessagePublisher eventPublisher;
    private Activator activator;
    private Map<String, Short> nameIdMap;

    @BeforeEach
    void before_each() {
        final var applicationName = "SEQ01";

        nameIdMap = new CoreMap<>();
        time = new ManualTime(LocalTime.of(9, 30));
        var scheduler = new Scheduler(time);
        eventLoop = new EventLoop(time, scheduler, mock(Selector.class));
        var logFactory = new TestLogFactory();
        var activatorFactory = new ActivatorFactory(logFactory, new MetricFactory(logFactory));
        schema = new TestSchema();
        busServer = new TestBusServer<>(time, schema, activatorFactory);
        eventPublisher = busServer.getEventPublisher();
        sequencer = new Sequencer(
                time,
                scheduler,
                activatorFactory,
                logFactory,
                busServer,
                applicationName);
        activator = activatorFactory.getActivator(sequencer);
        busServer.getDispatcher().addHeartbeatListener(x -> {
            var buffer = busServer.acquire();
            buffer.putBytes(0, x.buffer(), x.offset(), x.length());
            busServer.commit(x.length());
        });
        busServer.getDispatcher().addApplicationDefinitionListener(x -> {
            var id = nameIdMap.get(x.nameAsString());
            if (id == null) {
                id = (short) (nameIdMap.size() + 1);
                nameIdMap.put(x.nameAsString(), id);
                busServer.setApplicationSequenceNumber(id, 1);
            }

            var buffer = busServer.acquire();
            buffer.putBytes(0, x.buffer(), x.offset(), x.length());
            buffer.putShort(schema.getApplicationIdOffset(), id);
            busServer.commit(x.length());
        });
    }

    @Test
    void commands_update_contributor_sequence_number() {
        activator.start();
        eventPublisher.remove();
        eventPublisher.remove();
        publishAppDefinitionCommand("APP01"); // 1
        publishAppDefinitionCommand("APP02"); // 2

        publishCommand(2, 2, -1, "1");
        publishCommand(3, 2, -1, "12");
        publishCommand(2, 3, -1, "123");
        publishCommand(2, 4, -1, "1234");
        publishCommand(3, 3, -1, "12345");

        then(eventPublisher.size()).isEqualTo(5);
        then(busServer.getApplicationSequenceNumber(2)).isEqualTo(4);
        then(busServer.getApplicationSequenceNumber(3)).isEqualTo(3);
        then(busServer.getApplicationSequenceNumber(4)).isEqualTo(0);
    }

    @Test
    void command_sets_timestamp_of_event() {
        time.advanceTime(Duration.ofMillis(250));
        activator.start();
        eventPublisher.remove();
        eventPublisher.remove();
        publishAppDefinitionCommand("APP01");

        publishCommand(2, 2, -1, "1");

        var decoder = eventPublisher.remove();
        then(decoder.get("timestamp")).isEqualTo(time.nanos());
    }

    @Test
    void invalid_contributor_sequence_number_drops_command() {
        activator.start();
        eventPublisher.remove();
        eventPublisher.remove();
        publishAppDefinitionCommand("APP01"); // 1
        publishCommand(2, 2, -1, "1");
        publishCommand(2, 2, -3, "12");

        then(busServer.getApplicationSequenceNumber(2)).isEqualTo(2);
    }

    @Test
    void too_short_buffer_drops_command() {
        activator.start();
        eventPublisher.remove();
        eventPublisher.remove();
        publishAppDefinitionCommand("APP01"); // 1
        var buffer = BufferUtils.allocate(20);
        buffer.putShort(schema.getApplicationIdOffset(), (short) 1);
        buffer.putInt(schema.getApplicationSequenceNumberOffset(), 2);

        busServer.publishCommand(buffer);

        then(busServer.getApplicationSequenceNumber(2)).isEqualTo(1);
    }

    @Test
    void events_update_contributor_sequence_number() {
        publishAppDefinitionEvent(5, "APP01");
        publishAppDefinitionEvent(6, "APP02");
        publishEvent(5, 5, -1, "1");
        publishEvent(6, 9, -2, "12");

        then(busServer.getApplicationSequenceNumber(5)).isEqualTo(5);
        then(busServer.getApplicationSequenceNumber(6)).isEqualTo(9);
    }

    @Test
    void take_over_as_primary_and_keep_appSeqNum() {
        publishAppDefinitionEvent(1, "SEQ01");
        publishAppDefinitionEvent(5, "APP01");
        publishAppDefinitionEvent(6, "APP02");
        publishEvent(5, 5, -1, "1");
        publishEvent(6, 9, -2, "12");
        activator.start();
        // pop the heartbeat
        eventPublisher.remove();

        publishCommand(5, 6, -1, "");

        then(busServer.getApplicationSequenceNumber(5)).isEqualTo(6);
    }

    @Test
    void take_over_as_primary_and_prevent_event() {
        publishAppDefinitionEvent(1, "SEQ01");
        publishAppDefinitionEvent(3, "APP01");
        publishAppDefinitionEvent(7, "APP02");
        publishEvent(3, 5, -1, "1");
        publishEvent(7, 9, -2, "12");
        activator.start();
        eventPublisher.remove();

        publishCommand(3, 5, -1, "");

        then(busServer.getApplicationSequenceNumber(3)).isEqualTo(5);
        then(busServer.getApplicationSequenceNumber(7)).isEqualTo(9);
    }

    @Test
    void too_short_buffer_drops_event() {
        activator.start();
        eventPublisher.remove();
        eventPublisher.remove();
        var buffer = BufferUtils.allocate(20);
        buffer.putShort(schema.getApplicationIdOffset(), (short) 2);
        buffer.putInt(schema.getApplicationSequenceNumberOffset(), 1);

        busServer.publishEvent(buffer);

        then(busServer.getApplicationSequenceNumber(2)).isEqualTo(0);
    }

    @Test
    void heartbeat_is_published_immediately() throws IOException {
        activator.start();

        eventLoop.runOnce();

        then(eventPublisher.size()).isEqualTo(2);
        ApplicationDefinitionDecoder decoder1 = eventPublisher.remove();
        then(decoder1.getApplicationId()).isEqualTo((short) 1);
        then(decoder1.getApplicationSequenceNumber()).isEqualTo(1);
        then(decoder1.getTimestamp()).isEqualTo(time.nanos());
        HeartbeatDecoder decoder2 = eventPublisher.remove();
        then(decoder2.getApplicationId()).isEqualTo((short) 1);
        then(decoder2.getApplicationSequenceNumber()).isEqualTo(2);
        then(decoder2.getTimestamp()).isEqualTo(time.nanos());
    }

    @Test
    void heartbeat_published_at_specified_time() throws IOException {
        activator.start();
        eventPublisher.remove();
        eventPublisher.remove();
        time.advanceTime(Duration.ofMillis(100));

        eventLoop.runOnce();

        then(eventPublisher.size()).isEqualTo(1);
        HeartbeatDecoder decoder2 = eventPublisher.remove();
        then(decoder2.getApplicationId()).isEqualTo((short) 1);
        then(decoder2.getApplicationSequenceNumber()).isEqualTo(3);
        then(decoder2.getTimestamp()).isEqualTo(time.nanos());
    }

    @Test
    void take_over_as_primary_and_send_heartbeats() throws IOException {
        publishAppDefinitionEvent(1, "SEQ01");
        publishEvent(1, 2, -1, "");
        publishEvent(1, 3, -1, "");
        publishEvent(1, 4, -1, "");
        time.advanceTime(Duration.ofMillis(1500));
        eventPublisher.remove();
        eventPublisher.remove();
        eventPublisher.remove();
        eventPublisher.remove();
        activator.start();
        // sent
        time.advanceTime(Duration.ofMillis(100));
        eventLoop.runOnce();
        time.advanceTime(Duration.ofMillis(200));
        eventLoop.runOnce();
        time.advanceTime(Duration.ofMillis(100));
        eventLoop.runOnce();

        var decoder = eventPublisher.remove();
        then(decoder.getApplicationSequenceNumber()).isEqualTo(5);
        decoder = eventPublisher.remove();
        then(decoder.getApplicationSequenceNumber()).isEqualTo(6);
        decoder = eventPublisher.remove();
        then(decoder.getApplicationSequenceNumber()).isEqualTo(7);
        decoder = eventPublisher.remove();
        then(decoder.getApplicationSequenceNumber()).isEqualTo(8);
    }

    public void publishCommand(int appId, int appSeqNum, int msgType, String message) {
        busServer.publishCommand(buildMessage(appId, appSeqNum, (byte) msgType, message));
    }

    public void publishEvent(int appId, int appSeqNum, int msgType, String message) {
        busServer.publishEvent(buildMessage(appId, appSeqNum, (byte) msgType, message));
    }

    public void publishAppDefinitionCommand(String appName) {
        busServer.publishCommand(new ApplicationDefinitionEncoder()
                .setApplicationId((short) 0)
                .setApplicationSequenceNumber(1)
                .setName(appName));
        busServer.getEventPublisher().remove();
    }

    public void publishAppDefinitionEvent(int appId, String appName) {
        busServer.publishEvent(new ApplicationDefinitionEncoder()
                .setApplicationId((short) appId)
                .setApplicationSequenceNumber(1)
                .setName(appName));
    }

    private MutableDirectBuffer buildMessage(int appId, int appSeqNum, byte msgType, String message) {
        var event = BufferUtils.allocate(
                schema.getMessageHeaderLength() + message.length());
        event.putShort(schema.getApplicationIdOffset(), (short) appId);
        event.putInt(schema.getApplicationSequenceNumberOffset(), appSeqNum);
        event.putByte(schema.getMessageTypeOffset(), msgType);
        event.putStringWithoutLengthAscii(schema.getMessageHeaderLength(), message);
        return event;
    }
}
