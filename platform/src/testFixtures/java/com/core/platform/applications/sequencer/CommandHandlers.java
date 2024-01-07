package com.core.platform.applications.sequencer;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.platform.bus.BusServer;
import com.core.platform.schema.ApplicationDefinitionDecoder;
import com.core.platform.schema.ApplicationDefinitionEncoder;
import com.core.platform.schema.CurrencyDecoder;
import com.core.platform.schema.CurrencyEncoder;
import com.core.platform.schema.HeartbeatDecoder;
import com.core.platform.schema.HeartbeatEncoder;
import com.core.platform.schema.SpotDecoder;
import com.core.platform.schema.SpotEncoder;
import com.core.platform.schema.TestDispatcher;
import com.core.platform.schema.TestProvider;
import org.agrona.DirectBuffer;
import org.eclipse.collections.api.map.primitive.MutableLongIntMap;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.api.map.primitive.MutableObjectShortMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectShortHashMap;

import java.util.Objects;

/**
 * Sequencer command handlers for test schema.
 */
public class CommandHandlers {

    private final BusServer<TestDispatcher, TestProvider> busServer;
    private final ApplicationDefinitionEncoder appEncoder;
    private final HeartbeatEncoder heartbeatEncoder;
    private final CurrencyEncoder currencyEncoder;
    private final SpotEncoder spotEncoder;
    private final MutableObjectIntMap<DirectBuffer> currencyToId;
    private final MutableLongIntMap spotToId;
    private final MutableObjectShortMap<DirectBuffer> appToId;
    private final Log log;

    /**
     * Constructs a {@code CommandHandlers} object which subscribes to the following messages on the dispatcher.
     * <ul>
     *     <li>{@code ApplicationDefinition}
     *     <li>{@code Heartbeat}
     *     <li>{@code SequencerReject}
     *     <li>{@code Currency}
     *     <li>{@code Spot}
     * </ul>
     *
     * @param logFactory a factory to craete logs
     * @param busServer the bus server
     */
    public CommandHandlers(LogFactory logFactory, BusServer<TestDispatcher, TestProvider> busServer) {
        Objects.requireNonNull(logFactory, "logFactory is null");
        this.busServer = Objects.requireNonNull(busServer, "busServer is null");

        log = logFactory.create(getClass());

        heartbeatEncoder = new HeartbeatEncoder();
        appEncoder = new ApplicationDefinitionEncoder();
        currencyEncoder = new CurrencyEncoder();
        spotEncoder = new SpotEncoder();

        appToId = new ObjectShortHashMap<>();
        currencyToId = new ObjectIntHashMap<>();
        spotToId = new LongIntHashMap();

        var dispatcher = busServer.getDispatcher();
        dispatcher.addApplicationDefinitionListener(this::onApp);
        dispatcher.addHeartbeatListener(this::onHeartbeat);
        dispatcher.addCurrencyListener(this::onCurrency);
        dispatcher.addSpotListener(this::onSpot);
    }

    private void onApp(ApplicationDefinitionDecoder decoder) {
        var name = decoder.getName();
        if (name.capacity() == 0) {
            log.warn().append("invalid name: ").append(name).commit();
            return;
        }

        var appId = appToId.get(name);
        if (appId == 0) {
            appId = (short) (appToId.size() + 1);
            appToId.put(BufferUtils.copy(name), appId);
            busServer.setApplicationSequenceNumber(appId, decoder.getApplicationSequenceNumber());
        }

        BusServer.commit(busServer, appEncoder.copy(decoder, busServer.acquire())
                .setApplicationId(appId));
    }

    private void onHeartbeat(HeartbeatDecoder heartbeatDecoder) {
        BusServer.commit(busServer, heartbeatEncoder.copy(heartbeatDecoder,  busServer.acquire()));
    }

    private void onCurrency(CurrencyDecoder currencyDecoder) {
        var name = currencyDecoder.getName();
        if (name.capacity() == 0) {
            log.warn().append("invalid name: ").append(name).commit();
            return;
        }

        var currencyId = currencyToId.get(name);
        if (currencyId == 0) {
            currencyId = currencyToId.size() + 1;
            currencyToId.put(BufferUtils.copy(name), currencyId);
        }

        BusServer.commit(busServer, currencyEncoder.copy(currencyDecoder, busServer.acquire())
                .setCurrencyId(currencyId));
    }

    private void onSpot(SpotDecoder spotDecoder) {
        var baseCurrencyId = spotDecoder.getBaseCurrencyId();
        if (baseCurrencyId < 1 || baseCurrencyId > currencyToId.size()) {
            log.warn().append("invalid baseCurrencyId: ").append(baseCurrencyId).commit();
            return;
        }

        var quoteCurrencyId = spotDecoder.getQuoteCurrencyId();
        if (quoteCurrencyId < 1 || quoteCurrencyId > currencyToId.size()) {
            log.warn().append("invalid quoteCurrencyId:").append(baseCurrencyId).commit();
            return;
        }

        var spotKey = (((long) baseCurrencyId) << 32) | ((long) quoteCurrencyId);
        var spotId = spotToId.getIfAbsentPut(spotKey, spotToId.size() + 1);

        BusServer.commit(busServer, spotEncoder.copy(spotDecoder, busServer.acquire())
                .setInstrumentId(spotId));
    }

    @Command(path = "status", readOnly = true)
    @Override
    public String toString() {
        return "CommandHandlers{"
                + "currencies=" + currencyToId.size()
                + ", spotToId=" + spotToId.size()
                + '}';
    }
}
