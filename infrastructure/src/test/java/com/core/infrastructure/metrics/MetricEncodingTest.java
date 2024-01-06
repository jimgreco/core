package com.core.infrastructure.metrics;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

public class MetricEncodingTest {

    MutableDirectBuffer writeBuffer = new ExpandableArrayBuffer();
    MetricEncoding.Encoder encoder = new MetricEncoding.Encoder(writeBuffer);
    MetricEncoding.Decoder decoder = new MetricEncoding.Decoder(writeBuffer);

    MetricFactory.Metric stringMetric = new MetricFactory.Metric(
            "test-string-metric",
            () -> "AAAAAARG",
            FastList.newListWith(
                    "Probable", "True",
                    "WillHappen", "Definitely")
    );

    MetricFactory.Metric doubleMetric = new MetricFactory.Metric(
            "test-double-metric",
            () -> 1.5,
            FastList.newList());

    MetricFactory.Metric longMetric = new MetricFactory.Metric(
            "test-integer-metric",
            () -> 1,
            FastList.newList());

    MetricFactory.Metric booleanMetric = new MetricFactory.Metric(
            "test-boolean-metric",
            () -> true,
            FastList.newList());

    @Test
    void encode_decode_string() {
        then(stringMetric.getMetricType()).isEqualTo(MetricFactory.STRING_TYPE);
        thenValidateEncodeDecode(stringMetric);
    }

    @Test
    void encode_decode_double() {
        then(doubleMetric.getMetricType()).isEqualTo(MetricFactory.DOUBLE_TYPE);
        thenValidateEncodeDecode(doubleMetric);
    }

    @Test
    void encode_decode_integer() {
        then(longMetric.getMetricType()).isEqualTo(MetricFactory.LONG_TYPE);
        thenValidateEncodeDecode(longMetric);
    }

    @Test
    void encode_decode_boolean() {
        then(booleanMetric.getMetricType()).isEqualTo(MetricFactory.BOOLEAN_TYPE);
        thenValidateEncodeDecode(booleanMetric);
    }

    @Test
    void encode_decode_all() {
        var pos = 0;
        String expectedVmName = "some-vm-name";
        pos += encoder.encodeString(pos, expectedVmName);
        pos += encoder.encodeMetric(pos, doubleMetric);
        pos += encoder.encodeMetric(pos, longMetric);
        pos += encoder.encodeMetric(pos, booleanMetric);
        pos += encoder.encodeMetric(pos, stringMetric);

        var readPos = 0;
        var actualVmName = decoder.decodeString(readPos);
        readPos += actualVmName.nbBytesDecoded;
        then(actualVmName.value).isEqualTo(expectedVmName);

        var actualDoubleMetric = decoder.decodeMetric(readPos);
        readPos += actualDoubleMetric.nbBytesDecoded;
        thenIsEqual(actualDoubleMetric.metric, doubleMetric);

        var actualLongMetric = decoder.decodeMetric(readPos);
        readPos += actualLongMetric.nbBytesDecoded;
        thenIsEqual(actualLongMetric.metric, longMetric);

        var actualBooleanMetric = decoder.decodeMetric(readPos);
        readPos += actualBooleanMetric.nbBytesDecoded;
        thenIsEqual(actualBooleanMetric.metric, booleanMetric);

        var actualStringMetric = decoder.decodeMetric(readPos);
        readPos += actualStringMetric.nbBytesDecoded;
        thenIsEqual(actualStringMetric.metric, stringMetric);
    }

    private void thenValidateEncodeDecode(MetricFactory.Metric expectedMetric) {
        String expectedHeaderField = "VM name here";
        var pos = encoder.encodeString(0, expectedHeaderField);
        pos += encoder.encodeMetric(pos, expectedMetric);

        var decodedString = decoder.decodeString(0);
        var readPos = decodedString.nbBytesDecoded;
        then(decodedString.value).isEqualTo(expectedHeaderField);

        var decodedMetric = decoder.decodeMetric(readPos);
        var actualMetric = decodedMetric.metric;
        readPos += decodedMetric.nbBytesDecoded;

        then(readPos).isEqualTo(pos);
        thenIsEqual(expectedMetric, actualMetric);
    }

    private void thenIsEqual(MetricFactory.Metric expectedMetric, MetricFactory.Metric actualMetric) {
        then(actualMetric.getName()).isEqualTo(expectedMetric.getName());
        then(actualMetric.getMetricType()).isEqualTo(expectedMetric.getMetricType());
        then(actualMetric.getLabels()).isEqualTo(expectedMetric.getLabels());

        switch (actualMetric.getMetricType()) {
            case MetricFactory.DOUBLE_TYPE -> then(actualMetric.getValueAsDouble())
                    .isEqualTo(expectedMetric.getValueAsDouble());
            case MetricFactory.LONG_TYPE -> then(actualMetric.getValueAsLong())
                    .isEqualTo(expectedMetric.getValueAsLong());
            case MetricFactory.BOOLEAN_TYPE -> then(actualMetric.getValueAsBoolean())
                    .isEqualTo(expectedMetric.getValueAsBoolean());
            default -> then(actualMetric.getValue())
                    .isEqualTo(expectedMetric.getValue());
        }
    }
}