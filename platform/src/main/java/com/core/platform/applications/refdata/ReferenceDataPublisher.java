package com.core.platform.applications.refdata;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.collections.CoreMap;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Directory;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.messages.Encoder;
import com.core.infrastructure.messages.Field;
import com.core.infrastructure.messages.Provider;
import com.core.platform.activation.Activatable;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.applications.EntityDataRepository;
import com.core.platform.bus.BusClient;
import org.agrona.DirectBuffer;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The reference data publisher publishes messages that define entities onto the core.
 *
 * <p>TODO: needs description of how entities are published from CSV
 *
 * @see EntityDataRepository
 */
public class ReferenceDataPublisher implements Publisher, Activatable {

    private final Log log;
    private final Map<String, ReferenceData> msgToRefData;
    @Directory(path = ".")
    private final Activator activator;
    @Directory(path = ".")
    private final CsvReferenceDataSource csvRefDataSource;
    @Directory(path = ".")
    private final EntityDataRepository entityDataRepository;

    /**
     * Creates a reference data publisher from the specified parameters.
     *
     * @param logFactory a factory to create logs
     * @param activatorFactory a factory to create activators
     * @param busClient the bus client
     * @param applicationName the name of the application
     * @param messageNames the name of the messages that should be published as reference data
     */
    public ReferenceDataPublisher(
            LogFactory logFactory, ActivatorFactory activatorFactory,
            BusClient<?, ?> busClient, String applicationName,
            String... messageNames) {
        Objects.requireNonNull(logFactory, "logFactory is null");
        Objects.requireNonNull(activatorFactory, "activationManager is null");
        Objects.requireNonNull(busClient, "busClient is null");
        Objects.requireNonNull(applicationName, "applicationName is null");
        Objects.requireNonNull(messageNames, "messageNames is null");

        entityDataRepository = new EntityDataRepository(logFactory, busClient);
        log = logFactory.create(getClass());
        msgToRefData = new CoreMap<>();

        var provider = busClient.getProvider(applicationName, this);
        for (var messageName : messageNames) {
            msgToRefData.put(messageName, new ReferenceData(provider, messageName));
        }

        csvRefDataSource = new CsvReferenceDataSource(logFactory, busClient, this, messageNames);
        new CoreReferenceDataSource(logFactory, busClient, entityDataRepository, this, messageNames);

        activator = activatorFactory.createActivator(applicationName, this, provider);
    }

    /**
     * Returns the CSV reference data source.
     *
     * @return the CSV reference data source
     */
    public CsvReferenceDataSource getCsvReferenceDataSource() {
        return csvRefDataSource;
    }

    @Override
    public void activate() {
        activator.notReady("sending pending entities");

        for (var refData : msgToRefData.values()) {
            var pendingIterator = refData.pending.entrySet().iterator();
            while (pendingIterator.hasNext()) {
                var entry = pendingIterator.next();
                var key = entry.getKey();
                var entity = entry.getValue();
                if (send(refData.getEncoder(), entity)) {
                    if (log.isDebug()) {
                        log.debug().append(refData.messageName).append(" pending => inFlight: ").append(key)
                                .append("=").append(entity)
                                .commit();
                    }
                    refData.inFlight.put(key, entity);
                } else {
                    if (log.isDebug()) {
                        log.debug().append(refData.messageName).append(" pending => unresolved: ").append(key)
                                .append("=").append(entity)
                                .commit();
                    }
                    refData.unresolved.put(key, entity);
                }
                pendingIterator.remove();
            }
        }

        activator.ready();
    }

    @SuppressWarnings("unchecked")
    private boolean send(Encoder encoder, Map<Field, Object> entity) {
        for (var entry : entity.entrySet()) {
            var field = entry.getKey();
            var value = entry.getValue();
            if (field.isForeignKey()) {
                var fkValue = entityDataRepository.getPrimaryKey(field.getForeignKey(), (List<String>) value);
                if (fkValue == 0) {
                    return false;
                }
                if (field.getType() == byte.class) {
                    encoder.set(field.getName(), (byte) fkValue);
                } else if (field.getType() == short.class) {
                    encoder.set(field.getName(), (short) fkValue);
                } else {
                    encoder.set(field.getName(), fkValue);
                }
            } else if (!field.isHeader() && !field.isPrimaryKey()) {
                if (field.getType() == DirectBuffer.class) {
                    encoder.set(field.getName(), BufferUtils.fromAsciiString((String) value));
                } else {
                    encoder.set(field.getName(), value);
                }
            }
        }

        encoder.commit().send();
        return true;
    }

    @Override
    public void deactivate() {
        activator.notReady();
    }

    @Override
    public void onEntityRequest(String messageName, String entityName, Map<Field, Object> entity) {
        var key = getKey(messageName, entity);
        var entityData = msgToRefData.get(messageName);

        if (!activator.isActive()) {
            if (log.isDebug()) {
                if (entity.equals(entityData.pending.get(key))) {
                    log.debug().append(entityData.messageName).append(" pending (dupe): ").append(key)
                            .append("=").append(entity)
                            .commit();
                } else {
                    log.debug().append(entityData.messageName).append(" pending (new): ").append(key)
                            .append("=").append(entity)
                            .commit();
                }
            }
            entityData.pending.put(key, entity);
            return;
        }

        Log.Statement logger = null;
        if (log.isDebug()) {
            logger = log.debug().append(messageName).append(" ");
        }

        if (equals(entityData.confirmed, key, entity, logger, "confirmed")
                || equals(entityData.inFlight, key, entity, logger, "inFlight")
                || equals(entityData.rejected, key, entity, logger, "rejected")
                || equals(entityData.unresolved, key, entity, logger, "unresolved")) {
            // no change in state
            return;
        }

        // entity is no longer in confirmed, pending, rejected, or unresolved maps
        String action;
        if (send(entityData.getEncoder(), entity)) {
            action = "inFlight";
            entityData.inFlight.put(key, entity);
        } else {
            action = "unresolved";
            entityData.unresolved.put(key, entity);
        }
        if (log.isDebug()) {
            logger.append(action).append(": ").append(key).append("=").append(entity).commit();
        }
    }

    private boolean equals(
            Map<List<Object>, Map<Field, Object>> entityData, List<Object> key, Map<Field, Object> entity,
            Log.Statement logger, String actionName) {
        var storedEntity = entityData.get(key);
        if (entity.equals(storedEntity)) {
            if (log.isDebug()) {
                logger.append(actionName).append(" (dupe): ").append(key)
                        .append("=").append(entity)
                        .commit();
            }
            return true;
        } else if (storedEntity == null) {
            return false;
        } else {
            // remove from the map
            if (log.isDebug()) {
                logger.append(actionName).append(" => ");
            }
            entityData.remove(key);
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onEntityAccepted(String messageName, String entityName, Map<Field, Object> entity, Object primaryKey) {
        var key = getKey(messageName, entity);
        var entityData = msgToRefData.get(messageName);

        var pendingEntity = entityData.inFlight.get(key);
        if (entity.equals(pendingEntity)) {
            // same entity, input matches accepted output
            if (log.isDebug()) {
                log.debug().append(entityData.messageName).append(" inFlight => confirmed: ").append(key)
                        .append("=").append(entity)
                        .commit();
            }
            entityData.inFlight.remove(key);
            entityData.confirmed.put(key, entity);
        } else {
            if (log.isDebug()) {
                log.debug().append(entityData.messageName).append(" inFlight (no action): key=").append(key)
                        .append(", acceptedEntity=").append(entity)
                        .append(", inFlightEntity=").append(pendingEntity)
                        .commit();
            }
        }

        // exhaustive search for unresolved entities
        msgToRefData.values().stream()
                .filter(x -> !x.unresolved.isEmpty())
                .filter(x -> Arrays.stream(x.foreignKeys).anyMatch(y -> y.getForeignKey().equals(entityName)))
                .forEach(x -> {
                    var unresolvedIterator = x.unresolved.entrySet().iterator();
                    while (unresolvedIterator.hasNext()) {
                        var entry = unresolvedIterator.next();
                        var unresolvedEntity = entry.getValue();
                        for (var foreignKey : x.foreignKeys) {
                            var unresolvedForeignKey = (List<Object>) unresolvedEntity.get(foreignKey);
                            if (compareKeys(key, unresolvedForeignKey)) {
                                if (send(x.getEncoder(), unresolvedEntity)) {
                                    if (log.isDebug()) {
                                        log.debug().append(entityData.messageName)
                                                .append(" unresolved => inFlight: ").append(entry.getKey())
                                                .append("=").append(unresolvedEntity)
                                                .commit();
                                    }
                                    unresolvedIterator.remove();
                                    x.inFlight.put(entry.getKey(), entry.getValue());
                                } else {
                                    if (log.isDebug()) {
                                        log.debug().append(entityData.messageName)
                                                .append(" unresolved (no action): ").append(entry.getKey())
                                                .append("=").append(unresolvedEntity)
                                                .commit();
                                    }
                                }
                                break;
                            }
                        }
                    }
                });
    }

    private boolean compareKeys(List<Object> key, List<Object> unresolvedForeignKey) {
        if (unresolvedForeignKey == null || key.size() != unresolvedForeignKey.size()) {
            return false;
        }
        for (var i = 0; i < key.size(); i++) {
            var keyA = key.get(i);
            var keyB = unresolvedForeignKey.get(i);
            if (!keyA.equals(keyB)
                    && keyA.getClass() == keyB.getClass()
                    && !keyA.toString().equals(keyB.toString())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void onEntityRejected(String messageName, String entityName, Map<Field, Object> entity) {
        var key = getKey(messageName, entity);
        var entityData = msgToRefData.get(messageName);

        var pendingEntity = entityData.inFlight.get(key);
        if (entity.equals(pendingEntity)) {
            // same entity, input matches rejected output
            if (log.isDebug()) {
                log.debug().append(entityData.messageName).append(" inFlight => rejected: ").append(key)
                        .append("=").append(entity)
                        .commit();
            }
            entityData.rejected.put(key, entity);
        } else if (pendingEntity == null) {
            // different entity
            log.debug().append(entityData.messageName).append(" rejected (new): ").append(key)
                    .append("=").append(entity)
                    .commit();
        } else {
            log.debug().append(entityData.messageName).append(" rejected (no action): key=").append(key)
                    .append(", rejectedEntity=").append(entity)
                    .append(", inFlightEntity=").append(pendingEntity)
                    .commit();
        }
    }

    private List<Object> getKey(String messageName, Map<Field, Object> entity) {
        var keys = new CoreList<>();
        var keyFields = msgToRefData.get(messageName).keys;
        for (var keyField : keyFields) {
            keys.add(entity.get(keyField));
        }
        return keys;
    }

    /**
     * Returns the keys corresponding to reference data entities that has not been confirmed for the specified message
     * name.
     *
     * @param messageName the name of the message
     * @return the keys corresponding to reference data entities that has not been confirmed
     * @throws IllegalArgumentException if the message name is unknown
     */
    @Command(readOnly = true)
    public String unconfirmed(String messageName) {
        var entityData = msgToRefData.get(messageName);
        if (entityData == null) {
            throw new IllegalArgumentException("unknown messageName: " + messageName);
        }
        return entityData.unconfirmedKeys();
    }

    @Command(path = "status", readOnly = true)
    @Override
    public String toString() {
        return msgToRefData.toString();
    }

    private static class ReferenceData implements Encodable {

        final Provider provider;
        final String messageName;
        final Field[] keys;
        final Field[] foreignKeys;
        final Map<List<Object>, Map<Field, Object>> pending;
        final Map<List<Object>, Map<Field, Object>> inFlight;
        final Map<List<Object>, Map<Field, Object>> confirmed;
        final Map<List<Object>, Map<Field, Object>> rejected;
        final Map<List<Object>, Map<Field, Object>> unresolved;

        ReferenceData(Provider provider, String messageName) {
            this.provider = provider;
            this.messageName = messageName;
            var encoder = getEncoder();
            keys = Arrays.stream(encoder.fields()).filter(Field::isKey).toArray(Field[]::new);
            foreignKeys = Arrays.stream(encoder.fields()).filter(Field::isForeignKey).toArray(Field[]::new);
            pending = new LinkedHashMap<>();
            inFlight = new LinkedHashMap<>();
            confirmed = new LinkedHashMap<>();
            rejected = new LinkedHashMap<>();
            unresolved = new LinkedHashMap<>();
        }

        Encoder getEncoder() {
            return provider.getEncoder(messageName);
        }

        public String unconfirmedKeys() {
            return "{"
                    + "pending=" + pending.keySet()
                    + ", inFlight=" + inFlight.keySet()
                    + ", rejected=" + rejected.keySet()
                    + ", unresolved=" + unresolved.keySet()
                    + '}';
        }

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.openMap()
                    .string("pending").number(pending.size())
                    .string("inFlight").number(inFlight.size())
                    .string("confirmed").number(confirmed.size())
                    .string("rejected").number(rejected.size())
                    .string("unresolved").number(unresolved.size())
                    .closeMap();
        }

        @Override
        public String toString() {
            return toEncodedString();
        }
    }
}
