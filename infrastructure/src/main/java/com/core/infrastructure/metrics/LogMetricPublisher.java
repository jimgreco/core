package com.core.infrastructure.metrics;

import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.log.ChannelLogSink;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.log.RollingLogFile;
import com.core.infrastructure.time.Scheduler;
import com.core.infrastructure.time.Time;
import org.agrona.DirectBuffer;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * The {@code LogMetricsPublisher} registers metrics from applications in the VM and publishes those metrics to a file
 * channel.
 *
 * <p>Metrics are always published with the current state of the metric.
 * There is no way to retrieve historical state from this publisher.
 *
 * <p>All metrics registered with the publisher are published every X seconds (configurable with command shell).
 *
 * <p>Metrics contain two types of values:
 * <ul>
 *     <li>numbers are encoded as 8-byte IEEE-754 double precision binary floating-point value
 *     <li>strings are encoded as a two-byte length followed by the ASCII encoded string
 * </ul>
 */
public class LogMetricPublisher {

    private static final int DEFAULT_SEND_TIME = 10;

    private final Scheduler scheduler;
    private final MetricFactory metricFactory;
    private final Log log;
    private final Log metricsLog;

    @Property(write = true)
    private int sendTimeSeconds;
    private long sendTaskId;

    /**
     * Creates a {@code LogMetricsPublisher} with the specified parameters.
     *
     * @param time the real-time source
     * @param scheduler a real-time task scheduler
     * @param metricFactory a registry of metrics
     * @param logFactory a factory to create logs
     * @param metricsLogFileName the log file name
     * @throws IOException if an I/O error occurs opening the metrics file
     */
    public LogMetricPublisher(
            Time time,
            Scheduler scheduler,
            MetricFactory metricFactory,
            LogFactory logFactory,
            String metricsLogFileName) throws IOException {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler is null");
        this.metricFactory = Objects.requireNonNull(metricFactory, "metricRegistry is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        this.sendTimeSeconds = DEFAULT_SEND_TIME;

        log = logFactory.create(getClass());

        var metricsLogFile = new RollingLogFile(metricsLogFileName, true);
        var metricsLogChannel = new ChannelLogSink(metricsLogFile.getFile());
        var metricsLogFactory = new LogFactory(time);
        metricsLogFactory.logSink(metricsLogChannel);
        metricsLog = metricsLogFactory.create("metrics");
    }

    /**
     * Start publishing all metrics to the metrics log.
     *
     * @throws IllegalStateException if the publisher has already been started
     */
    @Command
    public void start() {
        if (sendTaskId != 0) {
            throw new IllegalStateException("already started");
        }

        log.info().append("Starting metrics log publisher").commit();

        onPublish();
        sendTaskId = scheduler.scheduleEvery(
                sendTaskId, TimeUnit.SECONDS.toNanos(sendTimeSeconds), this::onPublish,
                "LogMetricsPublisher:publish", 0);
    }

    /**
     * Stop publishing metrics to the metrics log.
     */
    @Command
    public void stop() {
        sendTaskId = scheduler.cancel(sendTaskId);
    }

    private void onPublish() {
        var metrics = metricFactory.getMetrics();
        for (var metric : metrics) {
            var statement = metricsLog.info().append(metric.getMetricTypeString())
                    .append(": ")
                    .append(metric.getName())
                    .append('=');
            var metricType = metric.getMetricType();
            switch (metricType) {
                case MetricFactory.DOUBLE_TYPE -> {
                    statement = statement.append(metric.getValueAsDouble());
                }
                case MetricFactory.LONG_TYPE -> {
                    statement = statement.append(metric.getValueAsLong());
                }
                case MetricFactory.BOOLEAN_TYPE -> {
                    statement = statement.append(metric.getValueAsBoolean());
                }
                default -> {
                    var value = metric.getValue();
                    if (value == null) {
                        statement = statement.append("");
                    } else if (value instanceof String) {
                        var strValue = (String) value;
                        statement = statement.append('"').append(strValue).append('"');
                    } else if (value instanceof DirectBuffer) {
                        var dbValue = (DirectBuffer) value;
                        statement = statement.append('"').append(dbValue).append('"');
                    }
                }
            }

            statement = statement.append(", labels=[");
            var writeComma = false;
            var labels = metric.getLabels();
            for (var i = 0; i < labels.length; i += 2) {
                if (writeComma) {
                    statement = statement.append(',');
                } else {
                    writeComma = true;
                }
                statement = statement.append("{\"")
                        .append(labels[i])
                        .append("\":\"")
                        .append(labels[i + 1])
                        .append("\"}");
            }

            statement = statement.append(']');
            statement.commit();
        }
    }
}
