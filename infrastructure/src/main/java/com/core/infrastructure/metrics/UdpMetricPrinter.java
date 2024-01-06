package com.core.infrastructure.metrics;

import com.core.infrastructure.io.NioSelector;
import org.agrona.ExpandableDirectByteBuffer;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * This is a non-core application for logging metrics produced by the UdpMetricPublisher to stdout.
 *
 * <p>It subscribes to the UDP address specified in {@code CORE_METRICS_CHANNEL} environment variable, decodes any
 * packets it receives as metrics, and outputs in the InfluxDB line format.
 *
 * <p>This provides a simple mechanism for integrating metric collection pipelines (eg Telegraf or Vector) while
 * avoiding complicated logic around data loss in the event of partitions etc.
 *
 * <p>Syntax of InfluxDb line protocol:<pre>
 *
 * &lt;measurement>[,&lt;tag_key&gt;=&lt;tag_value&gt;[,&lt;tag_key&gt;=&lt;tag_value&gt;]]
 *     &lt;field_key&gt;=&lt;field_value&gt;[,&lt;field_key&gt;=&lt;field_value&gt;] [&lt;timestamp&gt;]
 *
 * Example:
 *
 * measurementName,tagKey=tagValue fieldKey="fieldValue" 1465839830100400200
 * --------------- --------------- --------------------- -------------------
 *    |                 |                 |                    |
 * Measurement       Tag set           Field set            Timestamp
 *
 * Timestamp is in nanos.
 * </pre>
 *
 * @see UdpMetricPublisher
 */
public class UdpMetricPrinter {

    private static boolean done = false;

    /**
     * Runs the {@code UdpMetricPrinter}.
     * This class will exit immediately if the {@code CORE_METRICS_CHANNEL} environmental variable is not set, or if
     * any command line arguments are provided.
     *
     * <p>See the class description for more information about the InfluxDB protocol.
     *
     * @param args the command line args (must be empty)
     */
    public static void main(String[] args) {
        if (args.length != 0) {
            System.err.println("ERROR - unexpected argument(s):");
            System.err.println(String.join("\n", Arrays.asList(args)));
            System.exit(1);
        }
        var metricsChannel = System.getenv("CORE_METRICS_CHANNEL");
        if (metricsChannel == null) {
            System.err.println("Unset CORE_METRICS_CHANNEL variable.");
            System.exit(1);
        }

        handleSigQuit();

        var exitStatus = 0;

        try (var selector = new NioSelector();
                var datagramChannel = selector.createDatagramChannel()) {
            var buffer = new ExpandableDirectByteBuffer(UdpMetricPublisher.PACKET_SIZE);
            var decoder = new MetricEncoding.Decoder(buffer);

            datagramChannel.configureBlocking(true);
            datagramChannel.join(metricsChannel);
            datagramChannel.bind(metricsChannel);

            while (!done) {
                var nbBytes = datagramChannel.receive(buffer);
                if (nbBytes == 0) {
                    System.err.println("Spurious wakeup during receive");
                    continue;
                }
                var decodedVmName = decoder.decodeString(0);
                var vmName = decodedVmName.value;
                var index = decodedVmName.nbBytesDecoded;

                for (int i = index; i < nbBytes; ) {
                    var decodedMetric = decoder.decodeMetric(i);
                    i += decodedMetric.nbBytesDecoded;

                    printInfluxDbMetric(vmName, decodedMetric.metric);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            exitStatus = 1;
        }

        System.exit(exitStatus);
    }

    private static void printInfluxDbMetric(String vmName, MetricFactory.Metric metric) {
        // Measurement
        var measurementName = metric.getName();
        System.out.print(measurementName);

        // Tag set
        System.out.print(',');
        System.out.print("vmName=");
        System.out.print(vmName);

        var labels = metric.getLabels();
        for (int i = 0; i < labels.length; i += 2) {
            System.out.print(',');
            System.out.print(labels[i]);
            System.out.print('=');
            System.out.print(labels[i + 1]);
        }

        // Field set
        System.out.print(' ');
        System.out.print("value=");

        switch (metric.getMetricType()) {
            case MetricFactory.DOUBLE_TYPE -> System.out.print(metric.getValueAsDouble());
            case MetricFactory.LONG_TYPE -> {
                System.out.print(metric.getValueAsLong());
                System.out.print('i');
            }
            case MetricFactory.BOOLEAN_TYPE -> {
                var value = metric.getValueAsBoolean();
                System.out.print(value);
            }
            default -> {
                System.out.print('"');
                System.out.print(metric.getValueAsString());
                System.out.print('"');
            }
        }

        // Timestamp
        System.out.print(' ');
        System.out.print(TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis()));

        // EOL
        System.out.println();
    }

    private static void handleSigQuit() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> done = true));
    }
}