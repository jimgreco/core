package com.core.platform.applications.refdata;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.log.TestLogFactory;
import com.core.infrastructure.messages.Field;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.applications.EntityDataRepository;
import com.core.platform.bus.TestBusClient;
import com.core.infrastructure.metrics.MetricFactory;
import com.core.platform.schema.CurrencyDecoder;
import com.core.platform.schema.CurrencyEncoder;
import com.core.platform.schema.SequencerRejectEncoder;
import com.core.platform.schema.SpotDecoder;
import com.core.platform.schema.SpotEncoder;
import com.core.platform.schema.TestDispatcher;
import com.core.platform.schema.TestSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class CoreReferenceDataSourceTest {

    private String messageName;
    private Object primaryKey;
    private Map<Field, Object> entity;
    private Field nameField;
    private Field baseCurrencyIdField;
    private Field quoteCurrencyIdField;
    private TestDispatcher dispatcher;
    private CurrencyEncoder usd;
    private CurrencyEncoder btc;
    private CurrencyEncoder eth;
    private SpotEncoder btcUsd;
    private SpotEncoder btcEth;
    private SequencerRejectEncoder rejected;

    @BeforeEach
    void before_each() {
        var entityPublisher = mock(Publisher.class);
        doAnswer(x -> {
            this.messageName = x.getArgument(0);
            this.entity = x.getArgument(2);
            this.primaryKey = x.getArgument(3);
            return null;
        }).when(entityPublisher).onEntityAccepted(any(), any(), any(), any());
        doAnswer(x -> {
            this.messageName = x.getArgument(0);
            this.entity = x.getArgument(2);
            return null;
        }).when(entityPublisher).onEntityRejected(any(), any(), any());

        var logFactory = new TestLogFactory();
        var busClient = new TestBusClient<>(
                new TestSchema(), new ActivatorFactory(logFactory, new MetricFactory(logFactory)));
        dispatcher = busClient.getDispatcher();
        nameField = new CurrencyDecoder().field("name");
        baseCurrencyIdField = new SpotDecoder().field("baseCurrencyId");
        quoteCurrencyIdField = new SpotDecoder().field("quoteCurrencyId");

        var entityMap = new EntityDataRepository(logFactory, busClient);

        new CoreReferenceDataSource(
                logFactory,
                busClient, "PUB01",
                entityMap, entityPublisher, "currency", "spot");

        usd = new CurrencyEncoder();
        usd.setCurrencyId(1);
        usd.setName(BufferUtils.fromAsciiString("USD"));

        btc = new CurrencyEncoder();
        btc.setCurrencyId(2);
        btc.setName(BufferUtils.fromAsciiString("BTC"));

        eth = new CurrencyEncoder();
        eth.setCurrencyId(3);
        eth.setName(BufferUtils.fromAsciiString("ETH"));

        btcUsd = new SpotEncoder();
        btcUsd.setInstrumentId(1);
        btcUsd.setBaseCurrencyId(1);
        btcUsd.setQuoteCurrencyId(2);

        btcEth = new SpotEncoder();
        btcEth.setInstrumentId(2);
        btcEth.setBaseCurrencyId(2);
        btcEth.setQuoteCurrencyId(3);

        rejected = new SequencerRejectEncoder();
        rejected.setApplicationId(busClient.getMessagePublisher("PUB01").getApplicationId());
        rejected.setCommand(BufferUtils.copy(btcEth.buffer(), btcEth.offset(), btcEth.length()));
    }

    @Test
    void publish_entity_with_no_dependencies() {
        dispatcher.dispatch(usd.toDecoder());

        then(messageName).isEqualTo("currency");
        then(primaryKey).isEqualTo(1);
        then(entity.values().size()).isEqualTo(1);
        then(entity.get(nameField)).isEqualTo("USD");
    }

    @Test
    void publish_entity_with_dependencies() {
        dispatcher.dispatch(usd.toDecoder());
        dispatcher.dispatch(btc.toDecoder());
        dispatcher.dispatch(eth.toDecoder());

        dispatcher.dispatch(btcEth.toDecoder());

        then(messageName).isEqualTo("spot");
        then(primaryKey).isEqualTo(2);
        then(entity.values().size()).isEqualTo(2);
        then(entity.get(baseCurrencyIdField)).isEqualTo("BTC");
        then(entity.get(quoteCurrencyIdField)).isEqualTo("ETH");
    }

    @Test
    void do_not_entity_with_invalid_foreign_key_references() {
        dispatcher.dispatch(usd.toDecoder());
        dispatcher.dispatch(btc.toDecoder());
        clear();

        dispatcher.dispatch(btcEth.toDecoder());

        then(messageName).isNull();
        then(primaryKey).isNull();
        then(entity).isNull();
    }

    @Test
    void publish_rejected_entity() {
        dispatcher.dispatch(usd.toDecoder());
        dispatcher.dispatch(btc.toDecoder());
        dispatcher.dispatch(eth.toDecoder());
        clear();

        dispatcher.dispatch(rejected.toDecoder());

        then(messageName).isEqualTo("spot");
        then(primaryKey).isNull();
        then(entity.values().size()).isEqualTo(2);
        then(entity.get(baseCurrencyIdField)).isEqualTo(List.of("BTC"));
        then(entity.get(quoteCurrencyIdField)).isEqualTo(List.of("ETH"));
    }

    @Test
    void do_not_publish_rejected_entity_with_invalid_foreign_keys() {
        dispatcher.dispatch(btc.toDecoder());
        clear();

        dispatcher.dispatch(rejected.toDecoder());

        then(messageName).isNull();
    }

    @Test
    void do_not_publish_rejected_entity_from_another_contributor() {
        dispatcher.dispatch(usd.toDecoder());
        dispatcher.dispatch(btc.toDecoder());
        dispatcher.dispatch(eth.toDecoder());
        clear();
        rejected.setApplicationId((short) 123);

        dispatcher.dispatch(rejected.toDecoder());

        then(messageName).isNull();
    }

    private void clear() {
        messageName = null;
        primaryKey = null;
        entity = null;
    }
}
