package com.core.infrastructure.metrics;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.Objects;

class MetricEncoding {

    static class Encoder {

        private final MutableDirectBuffer writeBuffer;

        Encoder(MutableDirectBuffer writeBuffer) {
            Objects.requireNonNull(writeBuffer);
            this.writeBuffer = writeBuffer;
        }

        int encodeMetric(int position, MetricFactory.Metric metric) {
            var metricPosition = position;
            // name
            metricPosition += encodeString(metricPosition, metric.getName());

            // labels
            var labels = metric.getLabels();
            var nbPairs = (byte) (labels.length / 2);
            writeBuffer.putByte(metricPosition++, nbPairs);
            for (var label : labels) {
                metricPosition += encodeString(metricPosition, label);
            }

            // type
            var metricType = metric.getMetricType();
            writeBuffer.putByte(metricPosition++, metric.getMetricType());

            // value
            switch (metricType) {
                case MetricFactory.DOUBLE_TYPE -> {
                    writeBuffer.putDouble(metricPosition, metric.getValueAsDouble());
                    metricPosition += Double.BYTES;
                }
                case MetricFactory.LONG_TYPE -> {
                    writeBuffer.putLong(metricPosition, metric.getValueAsLong());
                    metricPosition += Long.BYTES;
                }
                case MetricFactory.BOOLEAN_TYPE -> {
                    writeBuffer.putByte(metricPosition, (byte) (metric.getValueAsBoolean() ? 1 : 0));
                    metricPosition += Byte.BYTES;
                }
                default -> {
                    var value = metric.getValue();
                    if (value == null) {
                        metricPosition += encodeString(metricPosition, "");
                    } else if (value instanceof String) {
                        var strValue = (String) value;
                        metricPosition += encodeString(metricPosition, strValue);
                    } else if (value instanceof DirectBuffer) {
                        var dbValue = (DirectBuffer) value;
                        writeBuffer.putShort(metricPosition, (short) dbValue.capacity());
                        metricPosition += Short.BYTES;
                        writeBuffer.putBytes(metricPosition, dbValue, 0, dbValue.capacity());
                        metricPosition += dbValue.capacity();
                    }
                }
            }

            return metricPosition - position;
        }

        int encodeString(int position, String string) {
            writeBuffer.putShort(position, (short) string.length());
            writeBuffer.putStringWithoutLengthAscii(position + Short.BYTES, string);
            return Short.BYTES + string.length();
        }

    }

    static class DecodedString {

        final String value;
        final int nbBytesDecoded;

        DecodedString(String value, int nbBytesDecoded) {
            this.value = value;
            this.nbBytesDecoded = nbBytesDecoded;
        }
    }

    static class DecodedMetric {

        final MetricFactory.Metric metric;
        final int nbBytesDecoded;

        DecodedMetric(MetricFactory.Metric metric, int nbBytesDecoded) {
            Objects.requireNonNull(metric);
            this.metric = metric;
            this.nbBytesDecoded = nbBytesDecoded;
        }
    }

    static class Decoder {
        private final DirectBuffer readBuffer;

        Decoder(DirectBuffer readBuffer) {
            Objects.requireNonNull(readBuffer);
            this.readBuffer = readBuffer;
        }

        DecodedMetric decodeMetric(int position) {
            var metricPosition = position;
            // name
            var decodedString = decodeString(metricPosition);
            var name = decodedString.value;
            metricPosition += decodedString.nbBytesDecoded;

            // labels (come in name/value pairs)
            var nbLabelPairs = readBuffer.getByte(metricPosition++);
            FastList<String> labels = FastList.newList();

            for (int i = 0; i < nbLabelPairs * 2; ++i) {
                var s = decodeString(metricPosition);
                labels.add(s.value);
                metricPosition += s.nbBytesDecoded;
            }

            // type
            var metricType = readBuffer.getByte(metricPosition++);

            // value
            MetricFactory.Metric metric;
            switch (metricType) {
                case MetricFactory.DOUBLE_TYPE -> {
                    var theDouble = readBuffer.getDouble(metricPosition);
                    metricPosition += Double.BYTES;
                    metric = new MetricFactory.Metric(name, () -> theDouble, labels);
                }
                case MetricFactory.LONG_TYPE -> {
                    var theLong = readBuffer.getLong(metricPosition);
                    metricPosition += Long.BYTES;
                    metric = new MetricFactory.Metric(name, () -> theLong, labels);
                }
                case MetricFactory.BOOLEAN_TYPE -> {
                    var theBoolean = readBuffer.getByte(metricPosition) != 0;
                    metricPosition += Byte.BYTES;
                    metric = new MetricFactory.Metric(name, () -> theBoolean, labels);
                }
                default -> {
                    var decodedValue = decodeString(metricPosition);
                    metricPosition += decodedValue.nbBytesDecoded;
                    metric = new MetricFactory.Metric(name, () -> decodedValue.value, labels);
                }
            }

            return new DecodedMetric(metric, metricPosition - position);
        }

        DecodedString decodeString(int position) {
            var length = readBuffer.getShort(position);
            var stringPosition = position + Short.BYTES;
            var value = readBuffer.getStringWithoutLengthAscii(stringPosition, length);
            stringPosition += length;
            return new DecodedString(value, stringPosition - position);
        }
    }
}