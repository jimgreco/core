package com.core.platform.applications.utilities;

import com.core.infrastructure.log.TestLogFactory;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.bus.TestBusClient;
import com.core.platform.bus.TestMessagePublisher;
import com.core.infrastructure.metrics.MetricFactory;
import com.core.platform.schema.CurrencyDecoder;
import com.core.platform.schema.TestSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

public class InjectorTest {

    private Injector injector;
    private TestMessagePublisher messagePublisher;

    @BeforeEach
    void before_each() {
        var logFactory = new TestLogFactory();
        var activatorFactory = new ActivatorFactory(logFactory, new MetricFactory(logFactory));
        var busClient = new TestBusClient<>(new TestSchema(), activatorFactory);
        injector = new Injector(logFactory, activatorFactory, busClient, "INJECT01");
        activatorFactory.getActivator(injector).start();
        messagePublisher = busClient.getMessagePublisher("INJECT01");
    }

    @Test
    void inject_converts_key_value_pair_into_message() {
        injector.send("currency", "currencyId=2", "name=BTC");

        CurrencyDecoder currencyDecoder = messagePublisher.remove();
        then(currencyDecoder.getCurrencyId()).isEqualTo((short) 2);
        then(currencyDecoder.nameAsString()).isEqualTo("BTC");
    }

    @Test
    void inject_does_not_set_fields_not_set() {
        injector.send("currency", "currencyId=2");

        CurrencyDecoder currencyDecoder = messagePublisher.remove();
        then(currencyDecoder.getCurrencyId()).isEqualTo((short) 2);
        then(currencyDecoder.isNamePresent()).isFalse();
    }
}
