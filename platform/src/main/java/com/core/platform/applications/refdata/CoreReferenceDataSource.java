package com.core.platform.applications.refdata;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreMap;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.messages.Decoder;
import com.core.infrastructure.messages.Field;
import com.core.platform.applications.EntityDataRepository;
import com.core.platform.bus.BusClient;
import org.agrona.DirectBuffer;

class CoreReferenceDataSource {

    private final Log log;
    private final Publisher publisher;
    private final EntityDataRepository entityDataRepository;

    CoreReferenceDataSource(
            LogFactory logFactory,
            BusClient<?, ?> busClient,
            EntityDataRepository entityDataRepository, Publisher publisher, String... messageNames) {
        this.publisher = publisher;
        this.entityDataRepository = entityDataRepository;

        log = logFactory.create(getClass());

        var dispatcher = busClient.getDispatcher();
        for (var messageName : messageNames) {
            dispatcher.addListener(messageName, this::onEntityMessage);
        }
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
