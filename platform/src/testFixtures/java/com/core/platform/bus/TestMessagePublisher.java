package com.core.platform.bus;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.messages.Decoder;
import com.core.infrastructure.messages.Dispatcher;
import com.core.infrastructure.messages.Encoder;
import com.core.infrastructure.messages.MessagePublisher;
import com.core.infrastructure.messages.Schema;
import com.core.platform.activation.Activatable;
import com.core.platform.activation.Activator;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.bus.mold.MoldConstants;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.List;
import java.util.function.Supplier;

public class TestMessagePublisher implements MessagePublisher, Activatable {

    protected final Activator activator;
    private final List<DirectBuffer> committedBuffers;
    private final List<DirectBuffer> sentBuffers;
    private final Schema<?, ?> schema;
    private final MutableIntObjectMap<Supplier<Decoder>> msgTypeToDecoders;
    private final boolean setContributor;
    private final FastList<Runnable> sendListeners;
    private final Encoder appDefEncoder;
    private final String appDefNameField;
    private final String applicationName;
    private short appId;
    private int appSeqNum;
    private int lastAppSeqNum;
    private MutableDirectBuffer lastBuffer;
    private boolean sentAppDef;

    // TODO: refactor these constructors
    public TestMessagePublisher(
            ActivatorFactory activatorFactory,
            Schema<?, ?> schema,
            Dispatcher dispatcher,
            String applicationName) {
        this.schema = schema;
        this.applicationName = applicationName;
        this.appId = (short) 0;
        setContributor = true;
        committedBuffers = new CoreList<>();
        sentBuffers = new CoreList<>();
        sendListeners = new CoreList<>();
        msgTypeToDecoders = new IntObjectHashMap<>();

        for (var messageName : schema.getMessageNames()) {
            msgTypeToDecoders.put(schema.getMessageType(messageName), () -> schema.createDecoder(messageName));
        }

        activator = activatorFactory.createActivator("Publisher:" + applicationName, this);

        var appDefMsgName = schema.getProperty("applicationDefinitionMessageName");
        appDefNameField = schema.getProperty("applicationDefinitionNameField");
        appDefEncoder = schema.createEncoder(appDefMsgName);

        dispatcher.addListenerBeforeDispatch(decoder -> {
            if (appDefMsgName.equals(decoder.messageName())
                    && applicationName.equals(BufferUtils.toAsciiString((DirectBuffer) decoder.get(appDefNameField)))) {
                appId = decoder.getApplicationId();
                activator.ready();
            }
            if (decoder.getApplicationId() == appId) {
                lastAppSeqNum = decoder.getApplicationSequenceNumber();
            }
        });
    }

    public TestMessagePublisher(
            ActivatorFactory activatorFactory, Schema<?, ?> schema, int applicationId, boolean setContributor) {
        this.schema = schema;
        this.setContributor = setContributor;
        this.appId = (short) applicationId;
        committedBuffers = new CoreList<>();
        sentBuffers = new CoreList<>();
        sendListeners = new CoreList<>();
        msgTypeToDecoders = new IntObjectHashMap<>();

        applicationName = null;
        appDefEncoder = null;
        appDefNameField = null;

        for (var messageName : schema.getMessageNames()) {
            msgTypeToDecoders.put(schema.getMessageType(messageName), () -> schema.createDecoder(messageName));
        }

        activator = activatorFactory.createActivator("Publisher:" + appId, this);
    }

    @Override
    public void activate() {
        if (applicationName == null) {
            activator.ready();
        } else {
            if (sentAppDef) {
                activator.ready();
            } else {
                sentAppDef = true;
                commit(appDefEncoder.wrap(acquire())
                        .set(appDefNameField, BufferUtils.fromAsciiString(applicationName))
                        .length());
                send();
            }
        }
    }

    @Override
    public void deactivate() {
        activator.notReady();
    }

    @Override
    public MutableDirectBuffer acquire() {
        return lastBuffer = BufferUtils.allocate(MoldConstants.MTU_SIZE - MoldConstants.HEADER_SIZE - Short.BYTES);
    }

    @Override
    public void commit(int commandLength) {
        if (setContributor) {
            lastBuffer.putLong(schema.getApplicationIdOffset(), appId);
            lastBuffer.putInt(schema.getApplicationSequenceNumberOffset(), ++appSeqNum);
        }
        committedBuffers.add(BufferUtils.copy(lastBuffer, 0, commandLength));
        lastBuffer = null;
    }

    @Override
    public void send() {
        sentBuffers.addAll(committedBuffers);
        committedBuffers.clear();

        for (var sendListener : sendListeners) {
            sendListener.run();
        }
    }

    @Override
    public String getApplicationName() {
        return applicationName;
    }

    @Override
    public short getApplicationId() {
        return appId;
    }

    @Override
    public boolean isCurrent() {
        return lastAppSeqNum == appSeqNum;
    }

    public boolean isEmpty() {
        return sentBuffers.isEmpty();
    }

    public DirectBuffer removeBuffer() {
        return sentBuffers.remove(0);
    }

    public void removeAll() {
        sentBuffers.clear();
    }

    @SuppressWarnings("unchecked")
    public <T extends Decoder> T remove() {
        var buffer = removeBuffer();
        var msgType = buffer.getByte(schema.getMessageTypeOffset());
        var decoder = msgTypeToDecoders.get(msgType).get();
        decoder.wrap(buffer);
        return (T) decoder;
    }

    public int size() {
        return sentBuffers.size();
    }

    public void addSendListener(Runnable listener) {
        sendListeners.add(listener);
    }
}
