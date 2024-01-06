package com.core.platform.applications.refdata;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.log.TestLogFactory;
import com.core.infrastructure.metrics.MetricFactory;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.bus.TestBusClient;
import com.core.platform.bus.TestMessagePublisher;
import com.core.platform.schema.CurrencyDecoder;
import com.core.platform.schema.CurrencyEncoder;
import com.core.platform.schema.FutureDecoder;
import com.core.platform.schema.FutureEncoder;
import com.core.platform.schema.SpotDecoder;
import com.core.platform.schema.SpotEncoder;
import com.core.platform.schema.TestDispatcher;
import com.core.platform.schema.TestProvider;
import com.core.platform.schema.TestSchema;
import com.core.platform.schema.VenueDecoder;
import com.core.platform.schema.VenueEncoder;
import com.core.platform.schema.VenueInstrumentDecoder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.BDDAssertions.then;

@SuppressWarnings("PMD.UnusedLocalVariable")
public class ReferenceDataPublisherTest {

    private CsvReferenceDataSource dataSource;
    private TestBusClient<TestDispatcher, TestProvider> busClient;
    private TestMessagePublisher commandPublisher;
    private TestDispatcher dispatcher;
    private ReferenceDataPublisher referenceDataPublisher;
    private Activator activator;

    private CurrencyEncoder usd;
    private CurrencyEncoder btc;
    private CurrencyEncoder eth;
    private SpotEncoder btcUsd;
    private SpotEncoder ethUsd;
    private FutureEncoder btcFuture;
    private FutureEncoder ethFuture;
    private VenueEncoder coinbase;
    private VenueEncoder okx;

    @BeforeEach
    void before_each() {
        var logFactory = new TestLogFactory();
        var activationFactory = new ActivatorFactory(logFactory, new MetricFactory(logFactory));
        busClient = new TestBusClient<>(new TestSchema(), activationFactory);

        referenceDataPublisher = new ReferenceDataPublisher(
                logFactory, activationFactory,
                busClient, "PUB01",
                "currency", "spot", "future", "venue", "venueInstrument");
        dataSource = referenceDataPublisher.getCsvReferenceDataSource();

        usd = new CurrencyEncoder().setCurrencyId(1).setName("USD");
        btc = new CurrencyEncoder().setCurrencyId(2).setName("BTC");
        eth = new CurrencyEncoder().setCurrencyId(3).setName("ETH");
        btcUsd = new SpotEncoder().setInstrumentId(1).setBaseCurrencyId(2).setQuoteCurrencyId(1);
        ethUsd = new SpotEncoder().setInstrumentId(2).setBaseCurrencyId(3).setQuoteCurrencyId(1);
        btcFuture = new FutureEncoder().setInstrumentId(3)
                .setBaseCurrencyId(2).setQuoteCurrencyId(1).setExpirationDate(20210625);
        ethFuture = new FutureEncoder().setInstrumentId(4)
                .setBaseCurrencyId(3).setQuoteCurrencyId(1).setExpirationDate(20210825);
        coinbase = new VenueEncoder().setVenueId(1).setName("COINBASE");
        okx = new VenueEncoder().setVenueId(2).setName("OKX");

        dispatcher = busClient.getDispatcher();
        commandPublisher = busClient.getMessagePublisher("PUB01");
        activator = activationFactory.getActivator(referenceDataPublisher);
        activator.start();
    }

    @Test
    void publish_entity_with_no_dependencies() throws IOException {
        var path = write(
                "name",
                "USD",
                "BTC",
                "ETH");

        dataSource.loadFile("currency", path);

        CurrencyDecoder currency = commandPublisher.remove();
        then(BufferUtils.toAsciiString(currency.getName())).isEqualTo("USD");
        currency = commandPublisher.remove();
        then(BufferUtils.toAsciiString(currency.getName())).isEqualTo("BTC");
        currency = commandPublisher.remove();
        then(BufferUtils.toAsciiString(currency.getName())).isEqualTo("ETH");
    }

    @Test
    void publish_entity_with_dependencies() throws IOException {
        publishCurrencies();
        var path = write(
                "baseCurrencyId,quoteCurrencyId",
                "BTC,USD",
                "ETH,USD",
                "BTC,ETH");

        dataSource.loadFile("spot", path);

        SpotDecoder spot = commandPublisher.remove();
        then(spot.getBaseCurrencyId()).isEqualTo(2);
        then(spot.getQuoteCurrencyId()).isEqualTo(1);
        spot = commandPublisher.remove();
        then(spot.getBaseCurrencyId()).isEqualTo(3);
        then(spot.getQuoteCurrencyId()).isEqualTo(1);
        spot = commandPublisher.remove();
        then(spot.getBaseCurrencyId()).isEqualTo(2);
        then(spot.getQuoteCurrencyId()).isEqualTo(3);
    }

    @Test
    void do_not_publish_entity_with_unknown_foreign_keys() throws IOException {
        publishCurrencies();
        var path = write(
                "baseCurrencyId,quoteCurrencyId",
                "BTCX,USDX",
                "ETH,USDX",
                "BTCX,ETH",
                "BTC,ETH");
        dataSource.loadFile("spot", path);

        SpotDecoder spot = commandPublisher.remove();
        then(spot.getBaseCurrencyId()).isEqualTo(2);
        then(spot.getQuoteCurrencyId()).isEqualTo(3);
        then(commandPublisher.isEmpty()).isTrue();
    }

    @Test
    void nothing_published_before_active() throws IOException {
        activator.stop();
        var path = write(
                "name",
                "USD",
                "BTC",
                "ETH");

        dataSource.loadFile("currency", path);

        then(commandPublisher.isEmpty()).isTrue();
    }

    @Test
    void publish_pending_entities_on_active() throws IOException {
        activator.stop();
        var path = write(
                "name",
                "USD",
                "BTC",
                "ETH");
        dataSource.loadFile("currency", path);

        activator.start();

        CurrencyDecoder currency = commandPublisher.remove();
        then(BufferUtils.toAsciiString(currency.getName())).isEqualTo("USD");
        currency = commandPublisher.remove();
        then(BufferUtils.toAsciiString(currency.getName())).isEqualTo("BTC");
        currency = commandPublisher.remove();
        then(BufferUtils.toAsciiString(currency.getName())).isEqualTo("ETH");
    }

    @Test
    void publish_dependency_on_foreign_key_definition() throws IOException {
        var path = write(
                "name",
                "USD",
                "BTC",
                "ETH");
        dataSource.loadFile("currency", path);
        commandPublisher.remove();
        commandPublisher.remove();
        commandPublisher.remove();
        path = write(
                "baseCurrencyId,quoteCurrencyId",
                "BTC,USD",
                "ETH,USD",
                "BTC,ETH");
        dataSource.loadFile("spot", path);
        dispatcher.dispatch(usd.toDecoder());

        dispatcher.dispatch(eth.toDecoder());

        SpotDecoder spot = commandPublisher.remove();
        then(spot.getBaseCurrencyId()).isEqualTo(3);
        then(spot.getQuoteCurrencyId()).isEqualTo(1);
        then(commandPublisher.isEmpty()).isTrue();
    }

    @Test
    void publish_multiple_dependencies_on_foreign_key_definition() throws IOException {
        var path = write(
                "name",
                "USD",
                "BTC",
                "ETH");
        dataSource.loadFile("currency", path);
        commandPublisher.remove();
        commandPublisher.remove();
        commandPublisher.remove();
        path = write(
                "baseCurrencyId,quoteCurrencyId",
                "BTC,USD",
                "ETH,USD",
                "BTC,ETH");
        dataSource.loadFile("spot", path);
        dispatcher.dispatch(usd.toDecoder());
        dispatcher.dispatch(eth.toDecoder());
        // ETH/USD
        commandPublisher.remove();

        dispatcher.dispatch(btc.toDecoder());

        // BTC/USD
        SpotDecoder spot = commandPublisher.remove();
        then(spot.getBaseCurrencyId()).isEqualTo(2);
        then(spot.getQuoteCurrencyId()).isEqualTo(1);
        // BTC/ETH
        spot = commandPublisher.remove();
        then(spot.getBaseCurrencyId()).isEqualTo(2);
        then(spot.getQuoteCurrencyId()).isEqualTo(3);
        then(commandPublisher.isEmpty()).isTrue();
    }

    @Test
    void do_not_republish_pending_that_are_the_same() throws IOException {
        activator.stop();
        var path = write(
                "name",
                "USD",
                "BTC",
                "ETH");
        dataSource.loadFile("currency", path);
        path = write(
                "name",
                "ETH",
                "BTC",
                "USD");
        dataSource.loadFile("currency", path);

        activator.start();

        CurrencyDecoder currency = commandPublisher.remove();
        then(BufferUtils.toAsciiString(currency.getName())).isEqualTo("USD");
        currency = commandPublisher.remove();
        then(BufferUtils.toAsciiString(currency.getName())).isEqualTo("BTC");
        currency = commandPublisher.remove();
        then(BufferUtils.toAsciiString(currency.getName())).isEqualTo("ETH");
        then(commandPublisher.isEmpty()).isTrue();
    }

    @Test
    void do_not_republish_inFlight_that_are_the_same() throws IOException {
        var path = write(
                "name",
                "USD",
                "BTC",
                "ETH");
        dataSource.loadFile("currency", path);
        commandPublisher.remove();
        commandPublisher.remove();
        commandPublisher.remove();
        path = write(
                "name",
                "ETH",
                "BTC",
                "USD");

        dataSource.loadFile("currency", path);

        then(commandPublisher.isEmpty()).isTrue();
    }

    @Test
    void do_not_republish_confirmed_that_are_the_same() throws IOException {
        publishCurrencies();
        var path = write(
                "name",
                "ETH",
                "BTC",
                "USD");

        dataSource.loadFile("currency", path);

        then(commandPublisher.isEmpty()).isTrue();
    }

    @Test
    void do_not_republish_unresolved_that_are_the_same() throws IOException {
        var path = write(
                "name",
                "USD",
                "BTC",
                "ETH");
        dataSource.loadFile("currency", path);
        commandPublisher.remove();
        commandPublisher.remove();
        commandPublisher.remove();
        path = write(
                "baseCurrencyId,quoteCurrencyId",
                "BTC,USD",
                "ETH,USD",
                "BTC,ETH");
        dataSource.loadFile("spot", path);
        path = write(
                "baseCurrencyId,quoteCurrencyId",
                "BTC,ETH",
                "BTC,USD",
                "ETH,USD");

        dataSource.loadFile("spot", path);

        // BTC/USD
        then(commandPublisher.isEmpty()).isTrue();
    }

    @Test
    void publish_dependency_on_multi_key_foreign_key() throws IOException {
        publishForVenueInstrument();
        var path = write(
                "baseCurrencyId,quoteCurrencyId,expirationDate,multiplier",
                "BTC,USD,20210625,1",
                "ETH,USD,20210825,5");
        dataSource.loadFile("future", path);
        FutureDecoder decoder4 = commandPublisher.remove();
        decoder4 = commandPublisher.remove();
        dispatcher.dispatch(btcFuture.toDecoder());
        dispatcher.dispatch(ethFuture.toDecoder());
        path = write(
                "instrumentId,venueId,minQty",
                "BTC|USD,COINBASE,100",
                "ETH|USD,OKX,1000",
                "ETH|USD|20210825,OKX,10000");

        dataSource.loadFile("venueInstrument", path);

        VenueInstrumentDecoder decoder = commandPublisher.remove();
        then(decoder.getInstrumentId()).isEqualTo(1);
        then(decoder.getVenueId()).isEqualTo(1);
        then(decoder.getMinQty()).isEqualTo(100);
        decoder = commandPublisher.remove();
        then(decoder.getInstrumentId()).isEqualTo(2);
        then(decoder.getVenueId()).isEqualTo(2);
        then(decoder.getMinQty()).isEqualTo(1000);
        decoder = commandPublisher.remove();
        then(decoder.getInstrumentId()).isEqualTo(4);
        then(decoder.getVenueId()).isEqualTo(2);
        then(decoder.getMinQty()).isEqualTo(10000);
    }

    @Test
    void publish_before_foreign_keys_are_resolved() throws IOException {
        var path = write(
                "instrumentId,venueId,minQty",
                "BTC|USD,COINBASE,100",
                "ETH|USD,OKX,1000",
                "ETH|USD|20210825,OKX,10000");
        dataSource.loadFile("venueInstrument", path);

        publishForVenueInstrument();

        VenueInstrumentDecoder decoder = commandPublisher.remove();
        then(decoder.getInstrumentId()).isEqualTo(1);
        then(decoder.getVenueId()).isEqualTo(1);
        then(decoder.getMinQty()).isEqualTo(100);
        decoder = commandPublisher.remove();
        then(decoder.getInstrumentId()).isEqualTo(2);
        then(decoder.getVenueId()).isEqualTo(2);
        then(decoder.getMinQty()).isEqualTo(1000);
    }

    @Test
    void publish_before_foreign_keys_are_resolved2() throws IOException {
        var path = write(
                "instrumentId,venueId,minQty",
                "BTC|USD,COINBASE,100",
                "ETH|USD,OKX,1000",
                "ETH|USD|20210825,OKX,10000");
        dataSource.loadFile("venueInstrument", path);
        publishForVenueInstrument();
        VenueInstrumentDecoder decoder = commandPublisher.remove();
        decoder = commandPublisher.remove();
        path = write(
                "baseCurrencyId,quoteCurrencyId,expirationDate,multiplier",
                "BTC,USD,20210625,1",
                "ETH,USD,20210825,5");
        dataSource.loadFile("future", path);
        FutureDecoder decoder4 = commandPublisher.remove();
        decoder4 = commandPublisher.remove();
        dispatcher.dispatch(btcFuture.toDecoder());

        dispatcher.dispatch(ethFuture.toDecoder());

        decoder = commandPublisher.remove();
        then(decoder.getInstrumentId()).isEqualTo(4);
        then(decoder.getVenueId()).isEqualTo(2);
        then(decoder.getMinQty()).isEqualTo(10000);
    }

    private void publishCurrencies() throws IOException {
        var path = write(
                "name",
                "USD",
                "BTC",
                "ETH");

        dataSource.loadFile("currency", path);

        CurrencyDecoder currency = commandPublisher.remove();
        then(BufferUtils.toAsciiString(currency.getName())).isEqualTo("USD");
        currency = commandPublisher.remove();
        then(BufferUtils.toAsciiString(currency.getName())).isEqualTo("BTC");
        currency = commandPublisher.remove();
        then(BufferUtils.toAsciiString(currency.getName())).isEqualTo("ETH");

        dispatcher.dispatch(usd.toDecoder());
        dispatcher.dispatch(btc.toDecoder());
        dispatcher.dispatch(eth.toDecoder());
    }

    private void publishForVenueInstrument() throws IOException {
        var path = write(
                "name",
                "COINBASE",
                "OKX");
        dataSource.loadFile("venue", path);
        VenueDecoder decoder1 = commandPublisher.remove();
        decoder1 = commandPublisher.remove();
        dispatcher.dispatch(coinbase.toDecoder());
        dispatcher.dispatch(okx.toDecoder());

        path = write(
                "name",
                "USD",
                "BTC",
                "ETH");
        dataSource.loadFile("currency", path);
        CurrencyDecoder decoder2 = commandPublisher.remove();
        decoder2 = commandPublisher.remove();
        decoder2 = commandPublisher.remove();
        dispatcher.dispatch(usd.toDecoder());
        dispatcher.dispatch(btc.toDecoder());
        dispatcher.dispatch(eth.toDecoder());

        path = write(
                "baseCurrencyId,quoteCurrencyId",
                "BTC,USD",
                "ETH,USD");
        dataSource.loadFile("spot", path);
        SpotDecoder decoder3 = commandPublisher.remove();
        decoder3 = commandPublisher.remove();
        dispatcher.dispatch(btcUsd.toDecoder());
        dispatcher.dispatch(ethUsd.toDecoder());
    }

    private Path write(String... rows) throws IOException {
        return Files.write(Files.createTempFile("entity", null), List.of(rows));
    }
}
