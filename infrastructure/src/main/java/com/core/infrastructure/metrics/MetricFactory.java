package com.core.infrastructure.metrics;

import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import org.agrona.DirectBuffer;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * A factory to create metrics.
 */
public class MetricFactory {

    static final byte DOUBLE_TYPE = 1;
    static final byte LONG_TYPE = 2;
    static final byte BOOLEAN_TYPE = 3;
    static final byte STRING_TYPE = 4;

    private final List<Metric> metrics;
    private final Set<Metric> metricsSet;
    private final Log log;
    private final Map<String, String> defaultLabels;

    /**
     * Creates a metric registry with the specified parameters.
     *
     * @param logFactory a factory to create logs
     */
    public MetricFactory(LogFactory logFactory) {
        log = logFactory.create(getClass());
        metrics = new CoreList<>();
        defaultLabels = new LinkedHashMap<>();
        metricsSet = new UnifiedSet<>();
    }

    /**
     * Associates the specified value with the specified label.
     *
     * @param label the label with which the specified value is to be associated
     * @param value the value to be associated with the specified label
     */
    @Command
    public void setLabel(String label, String value) {
        defaultLabels.put(label, value);
    }

    /**
     * Removes the specified label.
     *
     * @param label the label who is to be removed
     */
    @Command
    public void removeLabel(String label) {
        defaultLabels.remove(label);
    }

    /**
     * Registers a gauge metric with the specified {@code name}, {@code labels}, and value {@code supplier}.
     *
     * @param name the name of the metric
     * @param supplier a supplier of the metric value
     * @param additionalLabels additional labels for the metric
     * @throws IllegalArgumentException if the metric name + labels have been previously registered
     */
    public void registerGaugeMetric(String name, LongSupplier supplier, String... additionalLabels) {
        var labels = createLabels(additionalLabels);
        log.info().append("registering gauge metric: name=").append(name)
                .append(", labels=").append(labels)
                .commit();
        addMetric(new Metric(name, supplier, labels));
    }

    /**
     * Registers a gauge metric with the specified {@code name}, {@code label}, and value {@code supplier}.
     *
     * @param name the name of the metric
     * @param supplier a supplier of the metric value
     * @param additionalLabels additional labels for the metric
     * @throws IllegalArgumentException if the metric name + labels have been previously registered
     */
    public void registerGaugeMetric(String name, DoubleSupplier supplier, String... additionalLabels) {
        var labels = createLabels(additionalLabels);
        log.info().append("registering gauge metric: name=").append(name)
                .append(", labels=").append(labels)
                .commit();
        addMetric(new Metric(name, supplier, labels));
    }

    /**
     * Registers a switch metric with the specified {@code name}, {@code label}, and value {@code supplier}.
     *
     * @param name the name of the metric
     * @param supplier a supplier of the metric value
     * @param additionalLabels additional labels for the metric
     * @throws IllegalArgumentException if the metric name + labels have been previously registered
     */
    public void registerSwitchMetric(String name, BooleanSupplier supplier, String... additionalLabels) {
        var labels = createLabels(additionalLabels);
        log.info().append("registering switch metric: name=").append(name)
                .append(", labels=").append(labels)
                .commit();
        addMetric(new Metric(name, supplier, labels));
    }

    /**
     * Registers a state metric with the specified {@code name}, {@code label}, and value {@code supplier}.
     *
     * @param name the name of the metric
     * @param supplier a supplier of the metric value
     * @param additionalLabels additional labels for the metric
     * @throws IllegalArgumentException if the metric name + labels have been previously registered
     */
    public void registerStateMetric(String name, Supplier<String> supplier, String... additionalLabels) {
        var labels = createLabels(additionalLabels);
        log.info().append("registering state metric: name=").append(name)
                .append(", labels=").append(labels)
                .commit();
        addMetric(new Metric(name, supplier::get, labels));
    }

    /**
     * Registers a state metric with the specified {@code name}, {@code label}, and value {@code supplier}.
     *
     * @param name the name of the metric
     * @param supplier a supplier of the metric value
     * @param additionalLabels additional labels for the metric
     * @throws IllegalArgumentException if the metric name + labels have been previously registered
     */
    public void registerStateBufferMetric(String name, Supplier<DirectBuffer> supplier, String... additionalLabels) {
        var labels = createLabels(additionalLabels);
        log.info().append("registering state metric: name=").append(name)
                .append(", labels=").append(labels)
                .commit();
        addMetric(new Metric(name, supplier::get, labels));
    }

    List<Metric> getMetrics() {
        return metrics;
    }

    private FastList<String> createLabels(String... additionalLabels) {
        if (additionalLabels.length % 2 != 0) {
            throw new IllegalArgumentException("additionalLabels must include label and label value pairs");
        }
        var labels = new CoreList<String>();
        for (var entry : defaultLabels.entrySet()) {
            labels.add(entry.getKey());
            labels.add(entry.getValue());
        }
        for (var additionalLabel : additionalLabels) {
            labels.add(additionalLabel);
        }
        return labels;
    }

    private void addMetric(Metric metric) {
        if (metricsSet.add(metric)) {
            metrics.add(metric);
        } else {
            throw new IllegalArgumentException("metric is not unique: " + metric.id);
        }
    }

    static class Metric {

        private final String name;
        private final byte metricType;
        private final String[] labels;
        private final String id;
        private Supplier<Object> objectSupplier = null;
        private DoubleSupplier doubleSupplier = null;
        private LongSupplier longSupplier = null;
        private BooleanSupplier booleanSupplier = null;

        private Metric(String name, List<String> labels, byte metricType) {
            this.name = name;
            this.labels = labels.toArray(new String[0]);
            this.metricType = metricType;

            var id = name;
            for (var label : labels) {
                id += '|' + label;
            }
            this.id = id;
        }

        Metric(String name, Supplier<Object> supplier, List<String> labels) {
            this(name, labels, MetricFactory.STRING_TYPE);
            this.objectSupplier = supplier;
        }

        Metric(String name, DoubleSupplier supplier, List<String> labels) {
            this(name, labels, MetricFactory.DOUBLE_TYPE);
            doubleSupplier = supplier;
        }

        Metric(String name, LongSupplier supplier, List<String> labels) {
            this(name, labels, MetricFactory.LONG_TYPE);
            longSupplier = supplier;
        }

        Metric(String name, BooleanSupplier supplier, List<String> labels) {
            this(name, labels, MetricFactory.BOOLEAN_TYPE);
            booleanSupplier = supplier;
        }

        String getName() {
            return name;
        }

        String[] getLabels() {
            return labels;
        }

        byte getMetricType() {
            return metricType;
        }

        String getMetricTypeString() {
            switch (metricType) {
                case DOUBLE_TYPE -> {
                    return "Double";
                }
                case LONG_TYPE -> {
                    return "Long";
                }
                case BOOLEAN_TYPE -> {
                    return "Boolean";
                }
                case STRING_TYPE -> {
                    return "String";
                }
                default -> {
                    throw new IllegalStateException();
                }
            }
        }

        Object getValue() {
            return objectSupplier.get();
        }

        double getValueAsDouble() {
            if (booleanSupplier != null) {
                return booleanSupplier.getAsBoolean() ? 1 : 0;
            }
            if (longSupplier != null) {
                return longSupplier.getAsLong();
            }
            if (doubleSupplier != null) {
                return doubleSupplier.getAsDouble();
            }
            throw new UnsupportedOperationException();
        }

        long getValueAsLong() {
            if (booleanSupplier != null) {
                return booleanSupplier.getAsBoolean() ? 1 : 0;
            }
            if (longSupplier != null) {
                return longSupplier.getAsLong();
            }
            throw new UnsupportedOperationException();
        }

        boolean getValueAsBoolean() {
            if (booleanSupplier != null) {
                return booleanSupplier.getAsBoolean();
            }
            throw new UnsupportedOperationException();
        }

        String getValueAsString() {
            var value = objectSupplier.get();
            if (value instanceof String) {
                return (String) value;
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            return id.equals(((Metric) o).id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }
}