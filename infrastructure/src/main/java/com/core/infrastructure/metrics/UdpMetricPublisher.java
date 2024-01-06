package com.core.infrastructure.metrics;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.io.DatagramChannel;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.time.Scheduler;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * The UDP metric publisher registers metrics from applications in the VM and publishes those metrics over a UDP
 * channel.
 *
 * <p>Metrics are always published with the current state of the metric.
 * There is no way to retrieve historical state from this publisher.
 *
 * <p>All metrics registered with the publisher are published every X seconds (configurable with commands) across Y
 * number of packets.
 * Metric packets are self-contained and un-sequenced and contain a header followed by as many metrics as can fit in the
 * packet.
 *
 * <p>Metrics contain two types of values:
 * <ul>
 *     <li>numbers are encoded as 8-byte IEEE-754 double precision binary floating-point value
 *     <li>strings are encoded as a two-byte length followed by the ASCII encoded string
 * </ul>
 *
 * <table>
 *     <caption>Metric packet header</caption>
 *     <tr>
 *          <th>Field</th>
 *          <th>Type</th>
 *          <th>Description</th>
 *     </tr>
 *     <tr>
 *         <td>vm name</td>
 *         <td>string</td>
 *         <td>the name of the VM</td>
 *     </tr>
 *     <tr>
 *         <td>-> metric name</td>
 *         <td>string</td>
 *         <td>the registered name of the metric</td>
 *     </tr>
 *     <tr>
 *         <td>-> number of labels</td>
 *         <td>byte</td>
 *         <td>the number of labels for the metric</td>
 *     </tr>
 *     <tr>
 *         <td>-> -> label</td>
 *         <td>string</td>
 *         <td>the label</td>
 *     </tr>
 *     <tr>
 *         <td>-> -> label value</td>
 *         <td>string</td>
 *         <td>the label value</td>
 *     </tr>
 *     <tr>
 *         <td>-> metric value type</td>
 *         <td>enum</td>
 *         <td>number (1) or string (2)</td>
 *     </tr>
 *     <tr>
 *         <td>-> metric value</td>
 *         <td>string or number</td>
 *         <td>the encoded metric value</td>
 *     </tr>
 * </table>
 */
public class UdpMetricPublisher {

    private static final int DEFAULT_SEND_TIME = 10;

    private final Selector selector;
    private final Scheduler scheduler;
    private final MetricFactory metricFactory;
    private final Log log;
    private final MutableDirectBuffer writeBuffer;
    private final MetricEncoding.Encoder metricEncoder;

    @Property(write = true)
    private String vmName;
    @Property(write = true)
    private String address;
    @Property(write = true)
    private int sendTimeSeconds;
    private DatagramChannel channel;
    private long sendTaskId;

    static final int PACKET_SIZE = 1472;

    /**
     * Creates a {@code UdpMetricFactory} with the specified parameters.
     *
     * @param selector a factory to create UDP sockets
     * @param scheduler a real-time task scheduler
     * @param metricFactory a registry of metrics
     * @param logFactory a factory to create logs
     */
    public UdpMetricPublisher(
            Selector selector,
            Scheduler scheduler,
            MetricFactory metricFactory,
            LogFactory logFactory) {
        this.selector = Objects.requireNonNull(selector, "selector is null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler is null");
        this.metricFactory = Objects.requireNonNull(metricFactory, "metricRegistry is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        this.sendTimeSeconds = DEFAULT_SEND_TIME;

        vmName = "VM";
        log = logFactory.create(getClass());
        writeBuffer = BufferUtils.allocate(2 * PACKET_SIZE);
        metricEncoder = new MetricEncoding.Encoder(writeBuffer);
    }

    /**
     * Opens a UDP datagram channel and joins the channel to the specified address.
     *
     * @throws IOException if an I/O error occurs
     */
    @Command
    public void open() throws IOException {
        if (channel != null) {
            throw new IOException("already open");
        }
        if (address == null) {
            throw new IOException("metricChannelAddress is not set");
        }

        log.info().append("joining: ").append(address).commit();

        channel = selector.createDatagramChannel();
        channel.configureBlocking(false);
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        channel.join(address);
        channel.connect(address);

        onPublish();
        sendTaskId = scheduler.scheduleEvery(
                sendTaskId, TimeUnit.SECONDS.toNanos(sendTimeSeconds), this::onPublish,
                "UdpMetricPublisher:publish", 0);
    }

    /**
     * Closes the UDP datagram channel.
     *
     * @throws IOException if an I/O error occurs
     */
    @Command
    public void close() throws IOException {
        sendTaskId = scheduler.cancel(sendTaskId);

        if (channel != null) {
            log.info().append("closing").commit();
            channel.close();
            channel = null;
        }
    }

    private void onPublish() {
        var position = metricEncoder.encodeString(0, vmName);

        var metrics = metricFactory.getMetrics();
        for (var metric : metrics) {
            var metricPosition = position;
            metricPosition += metricEncoder.encodeMetric(metricPosition, metric);

            // write to socket
            try {
                if (metricPosition <= PACKET_SIZE) {
                    position = metricPosition;
                } else {
                    channel.write(writeBuffer, 0, position);
                    var newLength = metricPosition - position;
                    var newPosition = metricEncoder.encodeString(0, vmName);
                    // rewrite the (unwritten) current metric at the start of the buffer
                    writeBuffer.putBytes(newPosition, writeBuffer, position, newLength);
                    position = newPosition + newLength;
                }
            } catch (Throwable e) {
                tryClose(e);
                return;
            }
        }

        try {
            if (position > 0) {
                channel.write(writeBuffer, 0, position);
            }
        } catch (IOException e) {
            tryClose(e);
        }
    }

    private void tryClose(Throwable throwable) {
        try {
            log.warn().append("error retrieving or writing metric to socket: ").append(throwable).commit();
            close();
        } catch (IOException e) {
            log.warn().append("error closing socket: ").append(e).commit();
        }
    }
}
