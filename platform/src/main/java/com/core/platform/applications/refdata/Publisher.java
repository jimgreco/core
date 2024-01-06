package com.core.platform.applications.refdata;

import com.core.infrastructure.messages.Field;

import java.util.Map;

interface Publisher {

    void onEntityRequest(
            String messageName, String entityName, Map<Field, Object> entity);

    void onEntityAccepted(
            String messageName, String entityName, Map<Field, Object> entity, Object primaryKey);

    void onEntityRejected(
            String messageName, String entityName, Map<Field, Object> entity);
}
