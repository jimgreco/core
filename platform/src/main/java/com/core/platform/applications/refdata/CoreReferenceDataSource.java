package com.core.platform.applications.refdata;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreMap;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.messages.Decoder;
import com.core.infrastructure.messages.Field;
import com.core.infrastructure.messages.Provider;
import com.core.infrastructure.messages.Schema;
import com.core.platform.applications.EntityDataRepository;
import com.core.platform.bus.BusClient;
import org.agrona.DirectBuffer;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

class CoreReferenceDataSource {

    private final Log log;
    private final Publisher publisher;
    private final MutableIntObjectMap<Decoder> messageIdToDecoder;
    private final String commandFieldName;
    private final Schema<?, ?> schema;
    private final Provider provider;
    private final EntityDataRepository entityDataRepository;

    CoreReferenceDataSource(
            LogFactory logFactory,
            BusClient<?, ?> busClient, String applicationName,
            EntityDataRepository entityDataRepository, Publisher publisher, String... messageNames) {
        this.commandFieldName = busClient.getSchema().getSequencerRejectCommandField();
        this.publisher = publisher;
        this.entityDataRepository = entityDataRepository;

        schema = busClient.getSchema();
        provider = busClient.getProvider(applicationName, publisher);
        log = logFactory.create(getClass());
        messageIdToDecoder = new IntObjectHashMap<>();

        var dispatcher = busClient.getDispatcher();
        var schema = busClient.getSchema();
        for (var messageName : messageNames) {
            messageIdToDecoder.put(schema.getMessageType(messageName), schema.createDecoder(messageName));
            dispatcher.addListener(messageName, this::onEntityMessage);
        }

        var rejectMessageName = busClient.getSchema().getProperty("sequencerRejectMessageName");
        dispatcher.addListener(rejectMessageName, this::onSequencerReject);
    }

    private void onSequencerReject(Decoder rejectDecoder) {
        if (rejectDecoder.getApplicationId() != provider.getMessagePublisher().getApplicationId()) {
            return;
        }

        var originalCommand = (DirectBuffer) rejectDecoder.get(commandFieldName);
        var rejectedMsgType = originalCommand.getByte(schema.getMessageTypeOffset());
        var entityDecoder = messageIdToDecoder.get(rejectedMsgType);
        entityDecoder.wrap(originalCommand);

        var fields = new CoreMap<Field, Object>();
        var messageName = entityDecoder.messageName();

        for (var field : entityDecoder.fields()) {
            if (field.isForeignKey()) {
                var foreignKeyEntity = field.getForeignKey();
                var foreignKeyValue = (int) entityDecoder.integerValue(field.getName());
                var key = entityDataRepository.getKey(foreignKeyEntity, foreignKeyValue);
                if (key == null) {
                    log.warn().append("onSequencerReject unknown foreign key reference: message=").append(messageName)
                            .append(", foreignKeyEntity=").append(foreignKeyEntity)
                            .append(", foreignKeyValue=").append(foreignKeyValue)
                            .commit();
                    return;
                }
                fields.put(field, key);
            } else if (!field.isHeader() && !field.isPrimaryKey()) {
                var value = entityDecoder.get(field.getName());
                if (value != null) {
                    if (field.getType() == DirectBuffer.class) {
                        value = BufferUtils.toAsciiString((DirectBuffer) value);
                    }
                    fields.put(field, value);
                }
            }
        }

        log.warn().append(messageName).append(" rejected: ").append(fields).commit();

        publisher.onEntityRejected(messageName, entityDecoder.entityName(), fields);
    }

    private void onEntityMessage(Decoder entityDecoder) {
        var messageName = entityDecoder.messageName();
        var fields = new CoreMap<Field, Object>();
        Object primaryKey = null;

        for (var field : entityDecoder.fields()) {
            if (!entityDecoder.isPresent(field.getName())) {
                continue;
            }

            if (field.isPrimaryKey()) {
                primaryKey = entityDecoder.get(field.getName());
            } else if (field.getForeignKey() != null) {
                var foreignKeyEntity = field.getForeignKey();
                var foreignKeyValue = (int) entityDecoder.integerValue(field.getName());
                var key = entityDataRepository.getKey(foreignKeyEntity, foreignKeyValue);
                if (key == null) {
                    log.warn().append("unknown foreign key reference: message=").append(messageName)
                            .append(", foreignKeyEntity=").append(foreignKeyEntity)
                            .append(", foreignKeyValue=").append(foreignKeyValue)
                            .commit();
                    return;
                }
                fields.put(field, String.join("/", key));
            } else if (!field.isHeader()) {
                var value = entityDecoder.get(field.getName());
                if (value != null) {
                    if (field.getType() == DirectBuffer.class) {
                        value = BufferUtils.toAsciiString((DirectBuffer) value);
                    }
                    fields.put(field, value);
                }
            }
        }

        if (log.isDebug()) {
            log.debug().append(messageName).append(" accepted: fields=").append(fields)
                    .append(", primaryKey=").append(primaryKey)
                    .commit();
        }

        publisher.onEntityAccepted(messageName, entityDecoder.baseEntityName(), fields, primaryKey);
    }
}
