package com.core.platform.bus;

import com.core.infrastructure.command.Property;
import com.core.infrastructure.messages.Dispatcher;
import com.core.infrastructure.messages.Provider;
import com.core.infrastructure.messages.Schema;

import java.util.Arrays;
import java.util.Objects;

/**
 * The abstract bus server provides an implementation of common features for bus server implementations including
 * management of application sequence numbers and getters for the dispatcher and schema.
 *
 * @param <DispatcherT> the dispatcher type
 */
public abstract class AbstractBusServer<DispatcherT extends Dispatcher, ProviderT extends Provider>
        implements BusServer<DispatcherT, ProviderT> {

    @Property
    private int[] appSeqNum;
    private final DispatcherT dispatcher;
    private short appId;
    private final Schema<DispatcherT, ProviderT> schema;

    /**
     * Creates a {@code AbstractBusServer} with the specified schema.
     *
     * @param schema the schema
     */
    protected AbstractBusServer(Schema<DispatcherT, ProviderT> schema) {
        this.schema = Objects.requireNonNull(schema, "schema is null");
        appSeqNum = new int[100];
        dispatcher = schema.createDispatcher();
    }

    @Override
    public short getApplicationId() {
        return appId;
    }

    @Override
    public Schema<DispatcherT, ProviderT> getSchema() {
        return schema;
    }

    @Override
    public DispatcherT getDispatcher() {
        return dispatcher;
    }

    @Override
    public void setApplicationSequenceNumber(int applicationId, int applicationSequenceNumber) {
        if (applicationId > appSeqNum.length) {
            appSeqNum = Arrays.copyOf(appSeqNum, 2 * appSeqNum.length);
        }
        if (appId == 0) {
            appId = (short) applicationId;
        }
        appSeqNum[applicationId - 1] = applicationSequenceNumber;
    }

    @Override
    public int incrementAndGetApplicationSequenceNumber(int applicationId) {
        if (applicationId <= 0 || applicationId > appSeqNum.length) {
            return -1;
        } else {
            return ++appSeqNum[applicationId - 1];
        }
    }

    @Override
    public int getApplicationSequenceNumber(int applicationId) {
        if (applicationId <= 0 || applicationId > appSeqNum.length) {
            return -1;
        } else {
            return appSeqNum[applicationId - 1];
        }
    }
}
