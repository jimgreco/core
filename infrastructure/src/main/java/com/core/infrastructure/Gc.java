package com.core.infrastructure;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.Pipe;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.metrics.MetricFactory;
import com.sun.management.GarbageCollectionNotificationInfo;
import org.agrona.MutableDirectBuffer;

import javax.management.Notification;
import javax.management.NotificationBroadcaster;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The {@code Gc} class uses the JDK platform management beans to log and record information about garbage collection
 * events in the system.
 *
 * <p>Each garbage collection event is logged and includes:
 * <ul>
 *     <li>the garbage collector name
 *     <li>the garbage collection action
 *     <li>the garbage collection cause
 *     <li>the before and after memory for each memory area (e.g., G1 Eden, G1 Old Gen, G1 Survivor)
 *     <li>the before and after memory total memory
 *     <li>the duration of the garbage collection pause
 * </ul>
 *
 * <p>The following metrics are created:
 * <ul>
 *      <li>Gc_Events: the total number of garbage collection events
 *      <li>Gc_Size: the total number of bytes reclaimed by the garbage collector
 *      <li>Gc_Duration: the total number of nanoseconds spent in garbage collection
 * </ul>
 */
public class Gc implements NotificationListener, Encodable {

    private static final long MB = MemoryUnit.MEGABYTES.toBytes(1);

    private final AtomicInteger events;
    private final AtomicLong duration;
    private final AtomicLong bytes;
    private final Log log;
    private final Pipe.SourceChannel source;
    private final Pipe.SinkChannel sink;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer readBuffer;

    /**
     * Creates a {@code Gc} with a {@code selector} used to create a pipe to send information from the garbage
     * collection thread to the core thread, a log factory create a log to log the GC information, and a metric factory
     * to create metrics track the status of garbage collection events.
     *
     * @param selector a factory to create I/O components
     * @param logFactory a factory to create logs
     * @param metricFactory a factory to create metrics
     * @throws IOException if an I/O error occurs
     */
    public Gc(Selector selector, LogFactory logFactory, MetricFactory metricFactory) throws IOException {
        Objects.requireNonNull(selector, "selector is null");
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(metricFactory, "metricFactory is null");

        writeBuffer = BufferUtils.allocate(10000);
        readBuffer = BufferUtils.allocateExpandable(writeBuffer.capacity());
        log = logFactory.create(getClass());
        events = new AtomicInteger();
        duration = new AtomicLong();
        bytes = new AtomicLong();

        var pipe = selector.createPipe();
        source = pipe.getSource();
        source.configureBlocking(false);
        source.setReadListener(this::onRead);
        sink = pipe.getSink();
        sink.configureBlocking(true);

        var beans = ManagementFactory.getPlatformMXBeans(GarbageCollectorMXBean.class);
        for (var bean : beans) {
            if (bean instanceof NotificationBroadcaster) {
                ((NotificationBroadcaster) bean).addNotificationListener(this, null, null);
            } else {
                log.warn().append("PlatformMXBean is not a NotificationBroadcaster: ").append(bean.getName()).commit();
            }
        }

        metricFactory.registerGaugeMetric("Gc_Events", events::get);
        metricFactory.registerGaugeMetric("Gc_Size", bytes::get);
        metricFactory.registerGaugeMetric("Gc_Duration", duration::get);
    }

    /**
     * Executes a garbage collection in the VM with {@code System.gc()}.
     */
    @Command
    public void gc() {
        System.gc();
    }

    @Override
    public void handleNotification(Notification notification, Object handback) {
        var userData = notification.getUserData();
        if (!(userData instanceof CompositeData)) {
            return;
        }

        var compositeData = (CompositeData) userData;
        var info = GarbageCollectionNotificationInfo.from(compositeData);

        var index = 0;
        index += writeBuffer.putStringWithoutLengthAscii(index, info.getGcName());
        index += writeBuffer.putStringWithoutLengthAscii(index, ": ");
        index += writeBuffer.putStringWithoutLengthAscii(index, info.getGcAction());
        index += writeBuffer.putStringWithoutLengthAscii(index, " (");
        index += writeBuffer.putStringWithoutLengthAscii(index, info.getGcCause());
        index += writeBuffer.putStringWithoutLengthAscii(index, ") ");

        var gcInfo = info.getGcInfo();
        final var duration = gcInfo.getDuration();

        var memBefore = gcInfo.getMemoryUsageBeforeGc();
        var memAfter = gcInfo.getMemoryUsageAfterGc();
        var totalBeforeMem = 0L;
        var totalAfterMem = 0L;
        for (var memBeforeEntry : memBefore.entrySet()) {
            var key = memBeforeEntry.getKey();
            var memBeforeValue = memBeforeEntry.getValue();
            var memBeforeUsed = memBeforeValue.getUsed();
            totalBeforeMem += memBeforeUsed;
            var memAfterValue = memAfter.get(key);
            var memAfterUsed = memAfterValue.getUsed();
            totalAfterMem += memAfterUsed;

            if (memBeforeUsed != memAfterUsed) {
                writeBuffer.putByte(index++, (byte) '[');
                index += writeBuffer.putStringWithoutLengthAscii(index, key);
                index += writeBuffer.putStringWithoutLengthAscii(index, ": ");
                index += writeBuffer.putLongAscii(index, memAfterUsed / MB);
                index += writeBuffer.putStringWithoutLengthAscii(index, "mb -> ");
                index += writeBuffer.putLongAscii(index,  memBeforeUsed / MB);
                index += writeBuffer.putStringWithoutLengthAscii(index, "mb] ");
            }
        }

        index += writeBuffer.putLongAscii(index, totalBeforeMem / MB);
        index += writeBuffer.putStringWithoutLengthAscii(index, "mb -> ");
        index += writeBuffer.putLongAscii(index, totalAfterMem / MB);
        index += writeBuffer.putStringWithoutLengthAscii(index, "mb");

        index += writeBuffer.putStringWithoutLengthAscii(index, " (");
        index += writeBuffer.putLongAscii(index, duration);
        index += writeBuffer.putStringWithoutLengthAscii(index, "ms)");

        events.incrementAndGet();
        this.duration.addAndGet(TimeUnit.MILLISECONDS.toNanos(1) * duration);
        bytes.addAndGet(totalBeforeMem - totalAfterMem);

        try {
            if (sink.isOpen()) {
                sink.write(writeBuffer, 0, index);
            }
        } catch (IOException e) {
            try {
                sink.close();
            } catch (IOException ignored) {
                // do nothing
            }
        }
    }

    private void onRead() {
        try {
            if (source.isOpen()) {
                var index = source.read(readBuffer);
                log.info().append(readBuffer, 0, index).commit();
            }
        } catch (IOException e) {
            try {
                log.error().append("error reading from GC logger source, closing: ").append(e).commit();
                source.close();
            } catch (IOException e2) {
                log.warn().append("could not close GC logger: ").append(e2).commit();
            }
        }
    }

    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("events").number(events.get())
                .string("bytesMb").number((double) bytes.get() / MemoryUnit.MEGABYTES.toBytes(1))
                .string("durationSec").number((double) duration.get() / TimeUnit.NANOSECONDS.toSeconds(1))
                .closeMap();
    }

    @Override
    public String toString() {
        return toEncodedString();
    }
}
