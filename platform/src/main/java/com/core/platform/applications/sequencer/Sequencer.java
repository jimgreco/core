package com.core.platform.applications.sequencer;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.command.Directory;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.messages.Dispatcher;
import com.core.infrastructure.messages.Encoder;
import com.core.infrastructure.messages.Schema;
import com.core.infrastructure.time.Scheduler;
import com.core.infrastructure.time.Time;
import com.core.platform.activation.Activatable;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.bus.BusServer;
import com.core.platform.shell.Shell;
import org.agrona.DirectBuffer;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * The sequencer is the central nervous system of a core trading system.
 * When active, it is the only application to listen to the command channel and publish on the event channel (all other
 * applications, including the sequencer when inactive, do the opposite).
 * TODO: more about incoming commands, application id and sequence numbers, validation, etc.
 *
 * <h2>Activation</h2>
 *
 * <p>The application has the following activation dependencies:
 * <ul>
 *     <li>the bus server is active
 * </ul>
 *
 * <p>On activation, the application will:
 * <ul>
 *     <li>if this is the first message of the session, publish an application definition message for the this
 *         application
 *     <li>publish a heartbeat message
 * </ul>
 *
 * <p>On deactivation, the application will:
 * <ul>
 *     <li>publish a {@code ApplicationStatus} message with a status of {@code DOWN}
 *     <li>set itself as not ready
 * </ul>
 */
public class Sequencer implements Activatable, Encodable {

    private static final int DEFAULT_HEARTBEAT_TIMEOUT_MS = 100;
    private static final DirectBuffer VM_PROPERTY = BufferUtils.fromAsciiString("vm_name");

    private final Schema<?, ?> schema;
    private final BusServer<?, ?> busServer;
    @Directory(path = ".")
    private final Activator activator;
    private final Log log;
    private final Time time;
    private final Dispatcher dispatcher;
    private final Scheduler scheduler;

    private final Encoder heartbeatEncoder;
    private final Encoder appDefinitionEncoder;
    private final Encoder appDiscoveryEncoder;

    private final Runnable cachedSendHeartbeat;
    private final Shell shell;

    @Property(write = true)
    private long heartbeatTimeout;
    @Property
    private long heartbeatTaskId;

    /**
     * Creates a {@code Sequencer} from the specified parameters.
     * The sequencer subscribes to the command and event channels through the {@code busServer} to process commands
     * when active and listen to events when inactive.
     * The sequencer will also schedule a recurring heartbeat message, with the name of the heartbeat message and the
     * duration between heartbeats specified by the constructor parameters.
     *
     * @param shell the command shell
     * @param time the time source for event timestamps
     * @param scheduler a scheduler to send heartbeats
     * @param activatorFactory a factory to create activators
     * @param logFactory a factory to create logs
     * @param busServer the bus server
     * @param applicationName the name of the application
     */
    public Sequencer(
            Shell shell,
            Time time,
            Scheduler scheduler,
            ActivatorFactory activatorFactory,
            LogFactory logFactory,
            BusServer<?, ?> busServer,
            String applicationName) {
        this.shell = shell;
        this.time = Objects.requireNonNull(time, "time is null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler is null");
        Objects.requireNonNull(activatorFactory, "activationManager is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        this.busServer = Objects.requireNonNull(busServer, "busServer is null");
        Objects.requireNonNull(applicationName, "applicationName is null");

        cachedSendHeartbeat = this::sendHeartbeat;

        schema = busServer.getSchema();

        heartbeatTimeout = TimeUnit.MILLISECONDS.toNanos(DEFAULT_HEARTBEAT_TIMEOUT_MS);
        heartbeatEncoder = schema.createEncoder(schema.getHeartbeatMessageName())
                .wrap(BufferUtils.allocate(100));
        var appDefinitionMessageName = schema.getApplicationDefinitionMessageName();
        var appDefinitionNameField = schema.getApplicationDefinitionNameField();
        appDefinitionEncoder = schema.createEncoder(appDefinitionMessageName)
                .wrap(BufferUtils.allocate(100))
                .setApplicationSequenceNumber(1)
                .set(appDefinitionNameField, BufferUtils.fromAsciiString(applicationName));
        appDiscoveryEncoder = schema.createEncoder("applicationDiscovery")
                .wrap(BufferUtils.allocate(1000));

        dispatcher = busServer.getDispatcher();
        log = logFactory.create(getClass());

        activator = activatorFactory.createActivator(applicationName, this, busServer);

        busServer.addEventListener(buffer -> onEvent(buffer, 0, buffer.capacity()));
        busServer.setCommandListener(buffer -> onCommand(buffer, 0, buffer.capacity()));
    }

    private void onCommand(DirectBuffer buffer, int offset, int length) {
        if (length < schema.getMessageHeaderLength()) {
            log.warn().append("command received with less bytes than header length: expected=")
                    .append(schema.getMessageHeaderLength())
                    .append(", received=").append(length)
                    .commit();
            return;
        }

        var appId = buffer.getShort(schema.getApplicationIdOffset());
        var appSeqNum = buffer.getInt(schema.getApplicationSequenceNumberOffset());

        if (appSeqNum == 1
                && buffer.getByte(schema.getMessageTypeOffset()) == appDefinitionEncoder.messageType()) {
            dispatcher.dispatch(buffer, offset, length);
            busServer.send();
        } else {
            // handle other messages
            var expectedAppSeqNum = busServer.incrementAndGetApplicationSequenceNumber(appId);
            if (appSeqNum == expectedAppSeqNum) {
                // valid command
                dispatcher.dispatch(buffer, offset, length);
                busServer.send();
            } else {
                // back out the changes
                busServer.setApplicationSequenceNumber(appId, expectedAppSeqNum - 1);
                log.warn().append("command received with incorrect appSeqNum, dropping: appId=").append(appId)
                        .append(", actualAppSeqNum=").append(appSeqNum)
                        .append(", expectedAppSeqNum=").append(expectedAppSeqNum)
                        .commit();
            }
        }
    }

    private void onEvent(DirectBuffer buffer, int offset, int length) {
        if (busServer.isActive()) {
            return;
        }

        if (length < schema.getMessageHeaderLength()) {
            log.warn().append("event received with less bytes than header length: expected=")
                    .append(schema.getMessageHeaderLength())
                    .append(", received=").append(length)
                    .commit();
            return;
        }

        // set app sequence number
        var appId = buffer.getShort(schema.getApplicationIdOffset());
        var appSeqNum = buffer.getInt(schema.getApplicationSequenceNumberOffset());
        busServer.setApplicationSequenceNumber(appId, appSeqNum);
        dispatcher.dispatch(buffer, offset, length);
        busServer.send();
    }

    private void sendHeartbeat() {
        if (busServer.getApplicationId() == 0) {
            // define the sequencer
            onCommand(appDefinitionEncoder.buffer(), appDefinitionEncoder.offset(), appDefinitionEncoder.length());
        }

        // send the heartbeat
        var appSeqNum = busServer.getApplicationSequenceNumber(busServer.getApplicationId()) + 1;
        heartbeatEncoder.setApplicationId(busServer.getApplicationId())
                .setApplicationSequenceNumber(appSeqNum);
        onCommand(heartbeatEncoder.buffer(), heartbeatEncoder.offset(), heartbeatEncoder.length());
    }

    @Command(path = "status", readOnly = true)
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("activator").object(activator)
                .string("busServer").object(busServer)
                .string("time").object(time)
                .closeMap();
    }

    @Override
    public String toString() {
        return toEncodedString();
    }

    @Override
    public void activate() {
        heartbeatTaskId = scheduler.scheduleEvery(
                heartbeatTaskId, heartbeatTimeout, cachedSendHeartbeat, "Sequencer:heartbeat", 0);
        activator.ready();

        sendHeartbeat();
        sendAppDiscovery(1);
    }

    @Override
    public void deactivate() {
        heartbeatTaskId = scheduler.cancel(heartbeatTaskId);
        sendAppDiscovery(2);
        activator.notReady();
    }

    private void sendAppDiscovery(int status) {
        if (shell != null) {
            var appSeqNum = busServer.getApplicationSequenceNumber(busServer.getApplicationId()) + 1;
            appDiscoveryEncoder.setApplicationId(busServer.getApplicationId())
                    .setApplicationSequenceNumber(appSeqNum)
                    .set("vmName", shell.getPropertyValue(VM_PROPERTY))
                    .set("commandPath", BufferUtils.temp(shell.getPath(this)))
                    .set("activationStatus", (byte) status);
            onCommand(appDiscoveryEncoder.buffer(), appDiscoveryEncoder.offset(), appDiscoveryEncoder.length());
        }
    }
}
