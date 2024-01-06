package com.core.platform.applications.refdata;

import com.core.infrastructure.log.TestLogFactory;
import com.core.infrastructure.messages.Field;
import com.core.infrastructure.metrics.MetricFactory;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.bus.TestBusClient;
import com.core.platform.schema.CurrencyDecoder;
import com.core.platform.schema.FutureDecoder;
import com.core.platform.schema.SpotDecoder;
import com.core.platform.schema.TestSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.BDDAssertions.thenThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class CsvReferenceDataSourceTest {

    private List<Map<Field, Object>> entities;
    private CsvReferenceDataSource dataSource;
    private Field nameField;
    private Field baseCurrencyIdField;
    private Field quoteCurrencyIdField;

    @BeforeEach
    void before_each() {
        entities = new ArrayList<>();
        var entityPublisher = mock(Publisher.class);
        doAnswer(x -> {
            entities.add(x.getArgument(2));
            return null;
        }).when(entityPublisher).onEntityRequest(any(), any(), any());

        nameField = new CurrencyDecoder().field("name");
        baseCurrencyIdField = new SpotDecoder().field("baseCurrencyId");
        quoteCurrencyIdField = new SpotDecoder().field("quoteCurrencyId");

        var logFactory = new TestLogFactory();
        var busClient = new TestBusClient<>(
                new TestSchema(), new ActivatorFactory(logFactory, new MetricFactory(logFactory)));
        dataSource = new CsvReferenceDataSource(
                logFactory,
                busClient,
                entityPublisher, "currency", "spot", "future");
    }

    @Test
    void publish_entity_with_no_dependencies() throws IOException {
        var path = write(
                "name",
                "USD",
                "BTC",
                "ETH");

        dataSource.loadFile("currency", path);

        then(entities.size()).isEqualTo(3);
        then(entities.get(0).keySet()).containsExactly(nameField);
        then(entities.get(0).get(nameField)).isEqualTo("USD");
        then(entities.get(1).get(nameField)).isEqualTo("BTC");
        then(entities.get(2).get(nameField)).isEqualTo("ETH");
    }

    @Test
    void publish_empty_required_field_throws_IllegalArgumentException() throws IOException {
        var path = write(
                "name",
                "USD",
                "",
                "ETH");

        thenThrownBy(() -> dataSource.loadFile("currency", path))
                .isInstanceOf(IllegalArgumentException.class);
        then(entities).isEmpty();
    }

    @Test
    void publish_entity_with_weird_whitespace() throws IOException {
        var path = write(
                "  name",
                "USD  ",
                "  BTC",
                "  ETH  ");

        dataSource.loadFile("currency", path);

        then(entities.size()).isEqualTo(3);
        then(entities.get(0).keySet()).containsExactly(nameField);
        then(entities.get(0).get(nameField)).isEqualTo("USD");
        then(entities.get(1).get(nameField)).isEqualTo("BTC");
        then(entities.get(2).get(nameField)).isEqualTo("ETH");
    }

    @Test
    void publish_entity_with_weird_whitespace2() throws IOException {
        var path = write(
                "name  ",
                "USD  ",
                "  BTC",
                "  ETH  ");

        dataSource.loadFile("currency", path);

        then(entities.size()).isEqualTo(3);
        then(entities.get(0).keySet()).containsExactly(nameField);
    }

    @Test
    void publish_entity_with_weird_whitespace3() throws IOException {
        var path = write(
                " name ",
                "USD ",
                " BTC",
                " ETH ");

        dataSource.loadFile("currency", path);

        then(entities.size()).isEqualTo(3);
        then(entities.get(0).keySet()).containsExactly(nameField);
    }

    @Test
    void publish_entity_with_quotes() throws IOException {
        var path = write(
                " \"name\" ",
                " \" USD \" ",
                "\" BTC\"",
                " \"ETH \"");

        dataSource.loadFile("currency", path);

        then(entities.size()).isEqualTo(3);
        then(entities.get(0).keySet()).containsExactly(nameField);
        then(entities.get(0).get(nameField)).isEqualTo(" USD ");
        then(entities.get(1).get(nameField)).isEqualTo(" BTC");
        then(entities.get(2).get(nameField)).isEqualTo("ETH ");
    }

    @Test
    void publish_entity_with_foreign_keys() throws IOException {
        var path = write(
                "baseCurrencyId,quoteCurrencyId",
                "BTC,USD",
                "ETH,USD",
                "BTC,EUR");

        dataSource.loadFile("spot", path);

        then(entities.size()).isEqualTo(3);
        then(entities.get(0).keySet()).containsExactlyInAnyOrder(baseCurrencyIdField, quoteCurrencyIdField);
        then(entities.get(0).get(baseCurrencyIdField)).isEqualTo(List.of("BTC"));
        then(entities.get(0).get(quoteCurrencyIdField)).isEqualTo(List.of("USD"));
        then(entities.get(1).get(baseCurrencyIdField)).isEqualTo(List.of("ETH"));
        then(entities.get(1).get(quoteCurrencyIdField)).isEqualTo(List.of("USD"));
        then(entities.get(2).get(baseCurrencyIdField)).isEqualTo(List.of("BTC"));
        then(entities.get(2).get(quoteCurrencyIdField)).isEqualTo(List.of("EUR"));
    }

    @Test
    void publish_entity_with_foreign_keys_with_spaces_and_quotes() throws IOException {
        var path = write(
                "baseCurrencyId,quoteCurrencyId",
                "    BTC,  \"USD\" ",
                "\"ETH, FOO\",USD",
                "BTC,    EUR");

        dataSource.loadFile("spot", path);

        then(entities.size()).isEqualTo(3);
        then(entities.get(0).keySet()).containsExactlyInAnyOrder(baseCurrencyIdField, quoteCurrencyIdField);
        then(entities.get(0).get(baseCurrencyIdField)).isEqualTo(List.of("BTC"));
        then(entities.get(0).get(quoteCurrencyIdField)).isEqualTo(List.of("USD"));
        then(entities.get(1).get(baseCurrencyIdField)).isEqualTo(List.of("ETH, FOO"));
        then(entities.get(1).get(quoteCurrencyIdField)).isEqualTo(List.of("USD"));
        then(entities.get(2).get(baseCurrencyIdField)).isEqualTo(List.of("BTC"));
        then(entities.get(2).get(quoteCurrencyIdField)).isEqualTo(List.of("EUR"));
    }

    @Test
    void publish_entity_with_missing_required_foreign_key_fields_throws_IllegalArgumentException() throws IOException {
        var path = write(
                "baseCurrencyId,quoteCurrencyId",
                "BTC,",
                "ETH,USD",
                "BTC,EUR");

        thenThrownBy(() -> dataSource.loadFile("spot", path))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void publish_entity_with_missing_required_foreign_key_fields2_throws_IllegalArgumentException() throws IOException {
        var path = write(
                "baseCurrencyId,quoteCurrencyId",
                "BTC,USD",
                ",USD",
                "BTC,EUR");

        thenThrownBy(() -> dataSource.loadFile("spot", path))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void publish_entity_with_missing_required_foreign_key_fields3_throws_IllegalArgumentException() throws IOException {
        var path = write(
                "baseCurrencyId,quoteCurrencyId",
                "BTC,USD",
                "ETH,USD",
                ",");

        thenThrownBy(() -> dataSource.loadFile("spot", path))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void publish_entity_with_empty_header_column_throws_IllegalArgumentException() throws IOException {
        var path = write(
                ",quoteCurrencyId",
                "BTC,USD",
                "ETH,USD",
                "BTC,EUR");

        thenThrownBy(() -> dataSource.loadFile("spot", path))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void publish_entity_with_empty_header_column2_throws_IllegalArgumentException() throws IOException {
        var path = write(
                "baseCurrencyId,",
                "BTC,USD",
                "ETH,USD",
                "BTC,EUR");

        thenThrownBy(() -> dataSource.loadFile("spot", path))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void publish_entity_with_empty_header_column3_throws_IllegalArgumentException() throws IOException {
        var path = write(
                ",",
                "BTC,USD",
                "ETH,USD",
                "BTC,EUR");

        thenThrownBy(() -> dataSource.loadFile("spot", path))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void publish_entity_with_metadata() throws IOException {
        var path = write(
                "baseCurrencyId,quoteCurrencyId,expirationDate,multiplier",
                "BTC,USD,0,1.25",
                "ETH,USD,20210602,2",
                "BTC,EUR,20210603,2.5");

        dataSource.loadFile("future", path);

        var baseCurrencyIdField = new FutureDecoder().field("baseCurrencyId");
        var quoteCurrencyIdField = new FutureDecoder().field("quoteCurrencyId");
        var expirationDateField = new FutureDecoder().field("expirationDate");
        var multiplierField = new FutureDecoder().field("multiplier");
        then(entities.size()).isEqualTo(3);
        then(entities.get(0).keySet()).containsExactlyInAnyOrder(
                baseCurrencyIdField, quoteCurrencyIdField, expirationDateField, multiplierField);
        then(entities.get(0).get(baseCurrencyIdField)).isEqualTo(List.of("BTC"));
        then(entities.get(0).get(quoteCurrencyIdField)).isEqualTo(List.of("USD"));
        then(entities.get(0).get(expirationDateField)).isEqualTo(0);
        then(entities.get(0).get(multiplierField)).isEqualTo(1.25);
        then(entities.get(1).get(baseCurrencyIdField)).isEqualTo(List.of("ETH"));
        then(entities.get(1).get(quoteCurrencyIdField)).isEqualTo(List.of("USD"));
        then(entities.get(1).get(expirationDateField)).isEqualTo(18780);
        then(entities.get(1).get(multiplierField)).isEqualTo(2.0);
        then(entities.get(2).get(baseCurrencyIdField)).isEqualTo(List.of("BTC"));
        then(entities.get(2).get(quoteCurrencyIdField)).isEqualTo(List.of("EUR"));
        then(entities.get(2).get(expirationDateField)).isEqualTo(18781);
        then(entities.get(2).get(multiplierField)).isEqualTo(2.5);
    }

    @Test
    void publish_entity_with_optional_last_field() throws IOException {
        var path = write(
                "baseCurrencyId,quoteCurrencyId,expirationDate,multiplier,venueId",
                "BTC,USD,0,1.25,COINBASE",
                "ETH,USD,20210602,2,");

        dataSource.loadFile("future", path);

        var baseCurrencyIdField = new FutureDecoder().field("baseCurrencyId");
        var quoteCurrencyIdField = new FutureDecoder().field("quoteCurrencyId");
        var venueId = new FutureDecoder().field("venueId");
        then(entities.size()).isEqualTo(2);
        then(entities.get(0).get(baseCurrencyIdField)).isEqualTo(List.of("BTC"));
        then(entities.get(0).get(quoteCurrencyIdField)).isEqualTo(List.of("USD"));
        then(entities.get(0).get(venueId)).isEqualTo(List.of("COINBASE"));
        then(entities.get(1).get(baseCurrencyIdField)).isEqualTo(List.of("ETH"));
        then(entities.get(1).get(quoteCurrencyIdField)).isEqualTo(List.of("USD"));
        then(entities.get(1).get(venueId)).isNull();
    }

    @Test
    void publish_entity_with_incorect_number_of_columns_throws_IllegalArgumentException() throws IOException {
        var path = write(
                "baseCurrencyId,quoteCurrencyId,expirationDate,multiplier",
                "BTC,USD,0,1.25",
                "ETH,USD,20210602",
                "BTC,EUR,20210603,2.5");

        thenThrownBy(() -> dataSource.loadFile("future", path))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private Path write(String... rows) throws IOException {
        return Files.write(Files.createTempFile("entity", null), List.of(rows));
    }
}
