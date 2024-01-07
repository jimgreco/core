package com.core.clob.applications.sequencer;

import com.core.clob.ClobConstants;
import com.core.clob.schema.AddOrderDecoder;
import com.core.clob.schema.AddOrderEncoder;
import com.core.clob.schema.ApplicationDefinitionDecoder;
import com.core.clob.schema.ApplicationDefinitionEncoder;
import com.core.clob.schema.ApplicationDiscoveryDecoder;
import com.core.clob.schema.CancelOrderDecoder;
import com.core.clob.schema.CancelOrderEncoder;
import com.core.clob.schema.ClobDispatcher;
import com.core.clob.schema.ClobProvider;
import com.core.clob.schema.EquityDefinitionDecoder;
import com.core.clob.schema.EquityDefinitionEncoder;
import com.core.clob.schema.FillOrderEncoder;
import com.core.clob.schema.HeartbeatDecoder;
import com.core.clob.schema.RejectCancelDecoder;
import com.core.clob.schema.RejectCancelEncoder;
import com.core.clob.schema.RejectOrderDecoder;
import com.core.clob.schema.RejectOrderEncoder;
import com.core.clob.schema.Side;
import com.core.infrastructure.Allocation;
import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.IntrusiveLinkedList;
import com.core.infrastructure.collections.ObjectPool;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.platform.bus.BusServer;
import org.agrona.DirectBuffer;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectShortHashMap;

import java.util.Objects;

/**
 * Command handlers for the CLOB messages.
 *
 * <p>This class validates most messages in the schema and contains a limit order book.
 * New orders that are added to the book in price-time priority through the {@code AddOrder} message.
 * Any orders that cross (i.e., a buy price of the incoming order is equal to order greater than the best ask or the
 * sell price of the incoming order is equal to or less than the best bid) will be matched an generate an
 * {@code FillOrder} message.
 * Orders can be removed from the book using the {@code CancelOrder} message.
 *
 * <ol>
 *    <li>{@link HeartbeatDecoder} has no validation and is copied.
 *    <li>{@link com.core.clob.schema.SequencerRejectDecoder} is ignored and not used by the CLOB.
 *    <li>{@link ApplicationDefinitionDecoder} checks that the {@code name} field is not empty and assigns a
 *        monotonically increasing number to the {@code applicationId} field.
 *    <li>{@link ApplicationDiscoveryDecoder} has no validation and is copied.
 *    <li>{@link EquityDefinitionDecoder} checks that the {@code ticker} field is not empty and assigns a
 *        monotonically increasing number to the {@code instrumentId} field.
 *    <li>{@link AddOrderDecoder} validates that the {@code side} field is a valid enumeration set value, the
 *        {@code qty} field is positive, the {@code instrumentId} field references a valid {@code EquityDefinition} and
 *        the {@code price} field is positive.
 *        For valid orders, a monotonically increasing number is then assigned to the {@code orderId} field and the
 *        order is matched against and the remainder is added to the limit order book.
 *        For invalid orders, a {@code RejectOrder} message is sent.
 *    <li>{@link CancelOrderDecoder} validates that the {@code orderId} field references a live order.
 *        For valid cancels, the referenced order is removed from the limit order book.
 *        For invalid cancels, a {@code RejectCancel} message is sent.
 *    <li>{@link com.core.clob.schema.FillOrderDecoder} is ignored as it is only sent by the Sequencer.
 *    <li>{@link RejectOrderEncoder} has no validation and is copied when not sent by the Sequencer.
 *    <li>{@link RejectCancelDecoder} has no validation and is copied when not sent by the Sequencer.
 * </ol>
 *
 * <p>Note: Keep in mind this is just an example and you would not want to actually use a CLOB with this data structure
 * in production.
 */
public class ClobCommandHandlers implements Encodable {

    private final BusServer<ClobDispatcher, ClobProvider> busServer;
    private final Log log;

    private final ApplicationDefinitionEncoder appDefEncoder;
    private final EquityDefinitionEncoder equityDefEncoder;
    private final AddOrderEncoder addOrderEncoder;
    private final CancelOrderEncoder cancelOrderEncoder;
    private final FillOrderEncoder fillOrderEncoder;
    private final RejectOrderEncoder rejectOrderEncoder;
    private final RejectCancelEncoder rejectCancelEncoder;

    private final ObjectShortHashMap<DirectBuffer> appNameToId;
    private final ObjectIntHashMap<DirectBuffer> symbolToId;
    private final IntObjectHashMap<DirectBuffer> idToSymbol;

    private final IntObjectHashMap<Order> idToOrderMap;
    private final ObjectPool<Order> orderPool;
    private final FastList<IntrusiveLinkedList<Order>> bids;
    private final FastList<IntrusiveLinkedList<Order>> asks;
    private int lastOrderId;

    /**
     * Creates a {@code ClobCommandHandlers} and subscribes to the following messages:
     * {@code Heartbeat}, {@code ApplicationDefinition}, {@code ApplicationDiscovery}, {@code EquityDefinition},
     * {@code AddOrder}, {@code CancelOrder}, {@code RejectOrder}, and {@code RejectCancel}.
     *
     * @param logFactory a factory to create logs
     * @param busServer the sequencer bus
     */
    public ClobCommandHandlers(LogFactory logFactory, BusServer<ClobDispatcher, ClobProvider> busServer) {
        Objects.requireNonNull(logFactory, "logFactory is null");
        this.busServer = Objects.requireNonNull(busServer, "busServer is null");

        log = logFactory.create(getClass());

        appDefEncoder = new ApplicationDefinitionEncoder();
        equityDefEncoder = new EquityDefinitionEncoder();
        addOrderEncoder = new AddOrderEncoder();
        cancelOrderEncoder = new CancelOrderEncoder();
        fillOrderEncoder = new FillOrderEncoder();
        rejectOrderEncoder = new RejectOrderEncoder();
        rejectCancelEncoder = new RejectCancelEncoder();

        appNameToId = new ObjectShortHashMap<>();
        symbolToId = new ObjectIntHashMap<>();
        idToSymbol = new IntObjectHashMap<>();

        idToOrderMap = new IntObjectHashMap<>();
        orderPool = new ObjectPool<>(Order::new);
        bids = new FastList<>();
        asks = new FastList<>();

        var dispatcher = busServer.getDispatcher();
        dispatcher.addHeartbeatListener(this::onHeartbeat);
        dispatcher.addApplicationDefinitionListener(this::onAppDef);
        dispatcher.addApplicationDiscoveryListener(this::onAppDisc);
        dispatcher.addEquityDefinitionListener(this::onEquityDef);
        dispatcher.addAddOrderListener(this::onAddOrder);
        dispatcher.addCancelOrderListener(this::onCancelOrder);
        dispatcher.addRejectOrderListener(this::onRejectOrder);
        dispatcher.addRejectCancelListener(this::onRejectCancel);
    }

    private void onHeartbeat(HeartbeatDecoder decoder) {
        BusServer.copy(busServer, decoder);
    }

    private void onAppDef(ApplicationDefinitionDecoder decoder) {
        var name = decoder.getName();
        if (name.capacity() == 0) {
            log.warn().append("empty app definition name").commit();
            return;
        }

        var appId = appNameToId.get(name);
        if (appId == 0) {
            appId = (short) (appNameToId.size() + 1);
            appNameToId.put(BufferUtils.copy(name), appId);
            busServer.setApplicationSequenceNumber(appId, decoder.getApplicationSequenceNumber());
        }

        BusServer.commit(busServer, appDefEncoder.copy(decoder, busServer.acquire())
                .setApplicationId(appId));
    }

    private void onAppDisc(ApplicationDiscoveryDecoder decoder) {
        BusServer.copy(busServer, decoder);
    }

    private void onEquityDef(EquityDefinitionDecoder decoder) {
        var ticker = decoder.getTicker();
        if (ticker.capacity() == 0) {
            log.warn().append("empty equity ticker").commit();
            return;
        }

        var instrumentId = symbolToId.get(ticker);
        if (instrumentId == 0) {
            instrumentId = (short) (symbolToId.size() + 1);
            symbolToId.put(BufferUtils.copy(ticker), instrumentId);
            idToSymbol.put(instrumentId, BufferUtils.copy(ticker));
            bids.add(new IntrusiveLinkedList<>());
            asks.add(new IntrusiveLinkedList<>());
        }

        BusServer.commit(busServer, equityDefEncoder.copy(decoder, busServer.acquire())
                .setInstrumentId(instrumentId));
    }

    private void onAddOrder(AddOrderDecoder decoder) {
        var side = decoder.getSide();
        if (side == null) {
            sendRejectOrder(decoder, "invalid side");
            return;
        }

        var qty = decoder.getQty();
        if (qty <= 0) {
            sendRejectOrder(decoder, "invalid qty");
            return;
        }

        var instrumentId = decoder.getInstrumentId();
        if (instrumentId <= 0 || instrumentId > symbolToId.size()) {
            sendRejectOrder(decoder, "invalid instrumentId");
            return;
        }

        var price = decoder.getPrice();
        if (price <= 0) {
            sendRejectOrder(decoder, "invalid price");
            return;
        }

        var orderId = ++lastOrderId;
        BusServer.commit(busServer, addOrderEncoder.copy(decoder, busServer.acquire())
                .setOrderId(orderId));

        var sell = side == Side.SELL;
        var book = sell ? bids.get(instrumentId - 1) : asks.get(instrumentId - 1);
        Order order;
        while (qty > 0 && (order = book.getFirst()) != null
                && (!sell && price >= order.price || sell && price <= order.price)) {
            var fillQty = Math.min(qty, order.qty);

            sendFill(decoder, orderId, fillQty, order.price);
            sendFill(decoder, order.orderId, fillQty, order.price);

            qty -= fillQty;
            order.qty -= fillQty;
            if (order.qty == 0) {
                book.removeFirst();
                idToOrderMap.remove(order.orderId);
                orderPool.returnObject(order);
            }
        }

        if (qty > 0) {
            var newOrder = orderPool.borrowObject();
            newOrder.orderId = orderId;
            newOrder.sell = sell;
            newOrder.qty = qty;
            newOrder.instrumentId = instrumentId;
            newOrder.price = price;

            var newBook = sell ? asks.get(instrumentId - 1) : bids.get(instrumentId - 1);
            newBook.insert(newOrder);
            idToOrderMap.put(orderId, newOrder);
        }
    }

    private void onCancelOrder(CancelOrderDecoder decoder) {
        var orderId = decoder.getOrderId();
        var order = idToOrderMap.get(orderId);
        if (order == null) {
            if (orderId > 0 && orderId <= lastOrderId) {
                sendRejectCancel(decoder, "too late to cancel");
            } else {
                sendRejectCancel(decoder, "unknown order");
            }
            return;
        }

        var book = order.sell ? asks.get(order.instrumentId - 1) : bids.get(order.instrumentId - 1);
        book.remove(order);
        idToOrderMap.remove(order.orderId);
        orderPool.returnObject(order);

        BusServer.commit(busServer, cancelOrderEncoder.copy(decoder,  busServer.acquire()));
    }

    private void sendFill(AddOrderDecoder decoder, int orderId, long fillQty, long price) {
        BusServer.commit(busServer, fillOrderEncoder.wrap(busServer.acquire())
                .setApplicationId(busServer.getApplicationId())
                .setApplicationSequenceNumber(
                        busServer.getApplicationSequenceNumber(decoder.getApplicationId()))
                .setOrderId(orderId)
                .setQty(fillQty)
                .setPrice(price));
    }

    private void sendRejectOrder(AddOrderDecoder decoder, String reason) {
        BusServer.commit(busServer, rejectOrderEncoder.wrap(busServer.acquire())
                .setApplicationId(decoder.getApplicationId())
                .setApplicationSequenceNumber(decoder.getApplicationSequenceNumber())
                .setSide(decoder.sideAsByte())
                .setQty(decoder.getQty())
                .setInstrumentId(decoder.getInstrumentId())
                .setPrice(decoder.getPrice())
                .setReason(reason));
    }

    private void sendRejectCancel(CancelOrderDecoder decoder, String reason) {
        BusServer.commit(busServer, rejectCancelEncoder.wrap(busServer.acquire())
                .setApplicationId(decoder.getApplicationId())
                .setApplicationSequenceNumber(decoder.getApplicationSequenceNumber())
                .setOrderId(decoder.getOrderId())
                .setReason(reason));
    }

    private void onRejectOrder(RejectOrderDecoder decoder) {
        // do not copy events created by the Sequencer, addOrder handles it
        if (decoder.getApplicationId() != busServer.getApplicationId()) {
            BusServer.copy(busServer, decoder);
        }
    }

    private void onRejectCancel(RejectCancelDecoder decoder) {
        // do not copy events created by the Sequencer, cancelOrder handles it
        if (decoder.getApplicationId() != busServer.getApplicationId()) {
            BusServer.copy(busServer, decoder);
        }
    }

    IntrusiveLinkedList<Order> getBids(DirectBuffer symbol) {
        return bids.get(symbolToId.get(symbol) - 1);
    }

    IntrusiveLinkedList<Order> getAsks(DirectBuffer symbol) {
        return asks.get(symbolToId.get(symbol) - 1);
    }

    short getAppId(DirectBuffer name) {
        return appNameToId.get(name);
    }

    int getInstrumentId(DirectBuffer symbol) {
        return symbolToId.get(symbol);
    }

    DirectBuffer getSymbol(int instrumentId) {
        return idToSymbol.get(instrumentId);
    }

    /**
     * Prints all the bids and offers in the limit order book to the console.
     *
     * @param symbol the symbol of the instrument to print
     * @return a string representing the best bids and offers
     */
    @Command(readOnly = true)
    @Allocation
    public String printBook(DirectBuffer symbol) {
        var string = new StringBuilder();
        var bid = getBids(symbol).getFirst();
        var ask = getAsks(symbol).getFirst();

        while (bid != null || ask != null) {
            var offset = string.length();
            if (bid != null) {
                string.append("[").append(bid.orderId).append("] ")
                        .append(bid.qty)
                        .append(" @ ").append(ClobConstants.PRICE_ENCODER.toDouble(bid.price));
                bid = (Order) bid.next;
            }
            if (ask != null) {
                for (var i = string.length() - offset; i < 20; i++) {
                    string.append(' ');
                }
                string.append(" [").append(ask.orderId).append("] ")
                        .append(ask.qty)
                        .append(" @ ").append(ClobConstants.PRICE_ENCODER.toDouble(ask.price));
                ask = (Order) ask.next;
            }
            string.append("\n");
        }

        return string.toString();
    }

    @Command(path = "status")
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("totalOrders").number(lastOrderId)
                .string("liveOrders").number(idToOrderMap.size())
                .string("books").number(symbolToId.size())
                .string("apps").number(appNameToId.size())
                .closeMap();
    }

    /**
     * The Sequencer order state.
     */
    static class Order implements IntrusiveLinkedList.IntrusiveLinkedListItem<Order>, Encodable {

        IntrusiveLinkedList.IntrusiveLinkedListItem<Order> prev;
        IntrusiveLinkedList.IntrusiveLinkedListItem<Order> next;
        boolean sell;
        int orderId;
        int instrumentId;
        long qty;
        long price;

        @Override
        public IntrusiveLinkedList.IntrusiveLinkedListItem<Order> getPrevious() {
            return prev;
        }

        @Override
        public IntrusiveLinkedList.IntrusiveLinkedListItem<Order> getNext() {
            return next;
        }

        @Override
        public void setPrevious(IntrusiveLinkedList.IntrusiveLinkedListItem<Order> prev) {
            this.prev = prev;
        }

        @Override
        public void setNext(IntrusiveLinkedList.IntrusiveLinkedListItem<Order> next) {
            this.next = next;
        }

        @Override
        public Order getItem() {
            return this;
        }

        @Override
        public int compareTo(Order o) {
            return (sell ? 1 : -1) * Long.compare(price, o.price);
        }

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.openMap()
                    .string("orderId").number(orderId)
                    .string("side").string(sell ? "sell" : "buy")
                    .string("qty").number(qty, ClobConstants.QTY_ENCODER)
                    .string("instrumentId").number(instrumentId)
                    .string("qty").number(price, ClobConstants.PRICE_ENCODER)
                    .closeMap();
        }
    }
}
