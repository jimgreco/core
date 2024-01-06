package com.core.platform.applications;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.collections.CoreMap;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.messages.Decoder;
import com.core.infrastructure.messages.Field;
import com.core.platform.bus.BusClient;
import org.agrona.DirectBuffer;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The entity data repository maintains relationships between entities that are represented by messages on the core.
 *
 * <p>Entity messages are defined by a <i>primary key</i> (an integer number) that is unique across all entities of the
 * same type and a set of <i>key</i> fields that taken token uniquely define an entity.
 * In a core system, the key fields are defined by the application that publishes the message and the primary key is
 * assigned by the sequencer.
 * All core messages (entities and non-entities) can have <i>foreign-key</i> fields that reference the entity
 * primary keys.
 *
 * <p>The following example shows two entities, currency and instrument, which are defined by the
 * {@code CurrencyDefinition} and {@code SpotInstrumentDefinition} messages.
 * The currency entity has one key field, {@code Code}, which defines its uniqueness across all entities.
 * THe sequencer will assign a new primary key, {@code CurrencyId}, for each unique {@code Code} value.
 * For example, {currencyId=1, code='USD'}, {currencyId=2, code='BTC'}, {currencyId=3, code='ETH'}.
 * The instrument entity has two key fields which are also foreign keys to the currency entity, {@code BaseCurrencyId}
 * and {@code QuoteCurrencyId}.
 * The sequencer will assign a new primary key, {@code Instrument}, for each unique pair of currency identifiers.
 * For example, {instrumentId=1, baseCurrencyId=2, quoteCurrency=1},
 * {instrumentId=2, baseCurrencyId=3, quoteCurrencyId=1}.
 *
 * <pre>
 * &lt;message id="1" name="CurrencyDefinition" entity="currency"&gt;
 *     &lt;field name="CurrencyId" type="short" primary-key="true"/"&gt;
 *     &lt;optional id="1" name="Code" type="DirectBuffer" key="true"/"&gt;
 * &lt;/message>
 *
 * &lt;message id="2" name="SpotInstrumentDefinition" entity="instrument""&gt;
 *     &lt;field name="InstrumentId" type="int" primary-key="true"/"&gt;
 *     &lt;field name="BaseCurrencyId" type="short" key="true" foreign-key="currency"/"&gt;
 *     &lt;field name="QuoteCurrencyId" type="short" key="true" foreign-key="currency"/"&gt;
 * &lt;/message"&gt;
 * </pre>
 */
public class EntityDataRepository {

    private final Map<String, MutableIntObjectMap<List<String>>> entityToPrimaryKeyToKeyMap;
    private final Map<String, MutableObjectIntMap<List<String>>> entityToKeyToPrimaryKeyMap;
    private final String applicationDefinitionMessageName;
    private final String applicationIdField;

    /**
     * Creates the {@code EntityDataRepository} and subscribes to messages associated with each entity.
     * The bus client's message schema is scanned for messages associated with a valid entity.
     * A valid entity has a primary key that is a byte, short, or int type and one or more key fields.
     *
     * @param logFactory a factory to create logs
     * @param busClient the bus client
     */
    public EntityDataRepository(LogFactory logFactory, BusClient<?, ?> busClient) {
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(busClient, "busClient is null");

        entityToPrimaryKeyToKeyMap = new CoreMap<>();
        entityToKeyToPrimaryKeyMap = new CoreMap<>();
        var log = logFactory.create(getClass());

        var schema = busClient.getSchema();
        applicationDefinitionMessageName = schema.getApplicationDefinitionMessageName();
        applicationIdField = schema.getApplicationIdField();
        for (var messageName : schema.getMessageNames()) {
            var encoder = schema.createEncoder(messageName);
            var entityName = encoder.baseEntityName();

            if (entityName != null) {
                Field primaryKeyField = null;
                List<Field> keyFields = null;
                var error = false;

                for (var field : encoder.fields()) {
                    var fieldType = field.getType();
                    if (field.isPrimaryKey()) {
                        if (fieldType == byte.class || fieldType == short.class || fieldType == int.class) {
                            primaryKeyField = field;
                        } else {
                            error = true;
                            log.warn().append("invalid primary key field: message=").append(messageName)
                                    .append(", field=").append(field)
                                    .commit();
                        }
                    } else if (field.isKey()) {
                        if (keyFields == null) {
                            keyFields = new CoreList<>();
                        }
                        keyFields.add(field);
                    }
                }

                if (!error && keyFields != null) {
                    entityToPrimaryKeyToKeyMap.put(entityName, new IntObjectHashMap<>());
                    entityToKeyToPrimaryKeyMap.put(entityName, new ObjectIntHashMap<>());
                    log.info().append("found entity message: message=").append(messageName)
                            .append(", entity=").append(entityName)
                            .append(", primaryKey=").append(primaryKeyField)
                            .append(", key=").append(keyFields)
                            .commit();
                    busClient.getDispatcher().addListener(messageName, this::onEntity);
                }
            }
        }
    }

    /**
     * Returns a list of keys associated with the specified entity name and primary key, or null if no entity is
     * associated with the entity name and primary key.
     * The list will sized such that the number of elements in the list corresponds to the number of keys in the entity.
     *
     * <p>All keys are resolved as {@code String}s.
     * <ul>
     *     <li>{@code DirectBuffers} fields are represented as ASCII strings
     *     <li>enum fields are represented as the name of the enumeration set value (e.g., Side.BUY is represented as
     *         "BUY")
     *     <li>primitive fields are represented as ASCII strings (e.g., 123 is represented as "123")
     *     <li>foreign-keys are resolved to a forward slash ('/') delimited list of the keys associated with the entity
     *         referenced by the foreign key. (e.g., a key of "BTC/USD" would be resolved for the entity
     *         {instrumentId=1, quoteCurrencyId=2, baseCurrencyId=1} which refers to the entities {currencyId=2,
     *         code=BTC}, and {currencyId=1, code=USD})
     * </ul>
     *
     * @param entityName the name of the entity
     * @param primaryKey the primary
     * @return the keys
     */
    @Command(readOnly = true)
    public List<String> getKey(String entityName, int primaryKey) {
        return entityToPrimaryKeyToKeyMap.get(entityName).get(primaryKey);
    }

    /**
     * Returns the primary key associated with the specified entity name and list of keys.
     * See {@link #getKey(String, int)} for information on how the {@code key} list should be formatted.
     *
     * @param entityName the name of the entity
     * @param key the entity keys
     * @return the primary key
     */
    @Command(readOnly = true)
    public int getPrimaryKey(String entityName, List<String> key) {
        return entityToKeyToPrimaryKeyMap.get(entityName).get(key);
    }

    private void onEntity(Decoder decoder) {
        var primaryKey = 0;
        var key = new CoreList<String>();

        for (var field : decoder.fields()) {
            var fieldType = field.getType();

            // this check for applicationId is because the ApplicationDefinition message uses the ApplicationId field
            // as it's key, but it is in the header and so every entity has this field
            if (field.isPrimaryKey() && (!field.getName().equals(applicationIdField)
                    || decoder.messageName().equals(applicationDefinitionMessageName))) {
                if (fieldType == byte.class) {
                    primaryKey = (byte) decoder.get(field.getName());
                } else if (fieldType == short.class) {
                    primaryKey = (short) decoder.get(field.getName());
                } else {
                    primaryKey = (int) decoder.get(field.getName());
                }
            } else if (field.isKey()) {
                var fieldValue = decoder.isPresent(field.getName()) ? decoder.get(field.getName()) : null;

                if (fieldValue == null) {
                    key.add("");
                } else if (field.isForeignKey()) {
                    var primaryKeyMap = entityToPrimaryKeyToKeyMap.get(field.getForeignKey());
                    int foreignKey = 0;
                    if (fieldType == byte.class) {
                        foreignKey = (byte) fieldValue;
                    } else if (fieldType == short.class) {
                        foreignKey = (short) fieldValue;
                    } else {
                        foreignKey = (int) fieldValue;
                    }
                    var foreignKeyValue = primaryKeyMap.get(foreignKey);
                    if (foreignKeyValue == null) {
                        key.add("");
                    } else {
                        key.add(String.join("\\|", foreignKeyValue));
                    }
                } else if (fieldType == DirectBuffer.class) {
                    key.add(BufferUtils.toAsciiString((DirectBuffer) fieldValue));
                } else {
                    key.add(fieldValue.toString());
                }
            }
        }

        if (primaryKey != 0) {
            var primaryKeyToKeyMap = entityToPrimaryKeyToKeyMap.get(decoder.baseEntityName());
            primaryKeyToKeyMap.put(primaryKey, key);

            var keyToPrimaryKeyMap = entityToKeyToPrimaryKeyMap.get(decoder.baseEntityName());
            keyToPrimaryKeyMap.put(key, primaryKey);
        }
    }
}
