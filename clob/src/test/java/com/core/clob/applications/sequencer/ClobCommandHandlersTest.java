package com.core.clob.applications.sequencer;

import com.core.clob.applications.sequencer.ClobCommandHandlers.Order;
import com.core.clob.schema.AddOrderDecoder;
import com.core.clob.schema.AddOrderEncoder;
import com.core.clob.schema.ApplicationDefinitionEncoder;
import com.core.clob.schema.ApplicationDiscoveryDecoder;
import com.core.clob.schema.ApplicationDiscoveryEncoder;
import com.core.clob.schema.CancelOrderEncoder;
import com.core.clob.schema.ClobDispatcher;
import com.core.clob.schema.ClobProvider;
import com.core.clob.schema.ClobSchema;
import com.core.clob.schema.EquityDefinitionEncoder;
import com.core.clob.schema.FillOrderDecoder;
import com.core.clob.schema.HeartbeatDecoder;
import com.core.clob.schema.HeartbeatEncoder;
import com.core.clob.schema.RejectCancelDecoder;
import com.core.clob.schema.RejectOrderDecoder;
import com.core.clob.schema.Side;
import com.core.clob.schema.Status;
import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.log.TestLogFactory;
import com.core.infrastructure.metrics.MetricFactory;
import com.core.infrastructure.time.ManualTime;
import com.core.infrastructure.time.Scheduler;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.applications.sequencer.Sequencer;
import com.core.platform.bus.TestBusServer;
import com.core.platform.bus.TestMessagePublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.LocalTime;

import static com.core.clob.schema.Side.BUY;
import static com.core.clob.schema.Side.SELL;
import static org.assertj.core.api.BDDAssertions.then;

public class ClobCommandHandlersTest {

    private TestBusServer<ClobDispatcher, ClobProvider> busServer;
    private TestMessagePublisher eventPublisher;
    private ClobCommandHandlers handler;

    @BeforeEach
    void before_each() {
        var time = new ManualTime(LocalTime.of(9, 30));
        var scheduler = new Scheduler(time);
        var logFactory = new TestLogFactory();
        var activatorFactory = new ActivatorFactory(logFactory, new MetricFactory(logFactory));

        busServer = new TestBusServer<>(time, new ClobSchema(), activatorFactory);
        eventPublisher = busServer.getEventPublisher();
        var sequencer = new Sequencer(
                null,
                time,
                scheduler,
                activatorFactory,
                logFactory,
                busServer,
                "SEQ01");
        var activator = activatorFactory.getActivator(sequencer);
        handler = new ClobCommandHandlers(logFactory, busServer);

        activator.start();
        eventPublisher.removeAll();
        publishAppDefCommand("REFDATA01");
        publishEquityDefCommand("AAPL");
        publishEquityDefCommand("GOOG");
        publishAppDefCommand("LEHM01");
        publishAppDefCommand("BEAR01");
    }

    @Nested
    class AddOrderTests {

        @Test
        void addOrder_sends_addOrder_event() {
            sendOrder("LEHM01", BUY, 1000, "AAPL", 99);

            AddOrderDecoder decoder = busServer.getEventPublisher().remove();
            then(decoder.getOrderId()).isEqualTo(1);
            then(decoder.getSide()).isEqualTo(BUY);
            then(decoder.getQty()).isEqualTo(1000);
            then(decoder.getInstrumentId()).isEqualTo(1);
            then(decoder.getPrice()).isEqualTo(99);
        }

        @Test
        void addOrder_adds_bid_to_book() {
            sendOrder("BEAR01", BUY, 1000, "GOOG", 99);

            then(handler.getAsks(BufferUtils.fromAsciiString("GOOG")).isEmpty()).isTrue();
            var bids = handler.getBids(BufferUtils.fromAsciiString("GOOG"));
            then(bids.size()).isEqualTo(1);
            validate(bids.getFirst(), 1, BUY, 1000, "GOOG", 99);
        }

        @Test
        void addOrder_adds_ask_to_book() {
            sendOrder("LEHM01", SELL, 1000, "GOOG", 100);

            then(handler.getBids(BufferUtils.fromAsciiString("GOOG")).isEmpty()).isTrue();
            var asks = handler.getAsks(BufferUtils.fromAsciiString("GOOG"));
            then(asks.size()).isEqualTo(1);
            validate(asks.getFirst(), 1, SELL, 1000, "GOOG", 100);
        }

        @Test
        void addOrder_layers_bids() {
            sendOrder("LEHM01", BUY, 100, "AAPL", 100);
            sendOrder("BEAR01", BUY, 200, "AAPL", 99);
            sendOrder("LEHM01", BUY, 300, "AAPL", 101);
            sendOrder("LEHM01", BUY, 400, "AAPL", 101);

            then(handler.getAsks(BufferUtils.fromAsciiString("AAPL")).isEmpty()).isTrue();
            var asks = handler.getBids(BufferUtils.fromAsciiString("AAPL"));
            then(asks.size()).isEqualTo(4);
            var ask = asks.getFirst();
            validate(ask, 3, BUY, 300, "AAPL", 101);
            validate(ask = (Order) ask.next, 4, BUY, 400, "AAPL", 101);
            validate(ask = (Order) ask.next, 1, BUY, 100, "AAPL", 100);
            validate((Order) ask.next, 2, BUY, 200, "AAPL", 99);
        }

        @Test
        void addOrder_layers_asks() {
            sendOrder("LEHM01", SELL, 100, "AAPL", 100);
            sendOrder("LEHM01", SELL, 200, "AAPL", 99);
            sendOrder("BEAR01", SELL, 300, "AAPL", 101);
            sendOrder("BEAR01", SELL, 400, "AAPL", 99);

            then(handler.getBids(BufferUtils.fromAsciiString("AAPL")).isEmpty()).isTrue();
            var asks = handler.getAsks(BufferUtils.fromAsciiString("AAPL"));
            then(asks.size()).isEqualTo(4);
            var bid = asks.getFirst();
            validate(bid, 2, SELL, 200, "AAPL", 99);
            validate(bid = (Order) bid.next, 4, SELL, 400, "AAPL", 99);
            validate(bid = (Order) bid.next, 1, SELL, 100, "AAPL", 100);
            validate((Order) bid.next, 3, SELL, 300, "AAPL", 101);
        }

        @Test
        void addOrder_across_instruments_and_sides() {
            sendOrder("BEAR01", BUY, 100, "AAPL", 100);
            sendOrder("LEHM01", BUY, 200, "AAPL", 99);
            sendOrder("LEHM01", SELL, 300, "AAPL", 102);
            sendOrder("LEHM01", SELL, 400, "AAPL", 101);
            sendOrder("LEHM01", BUY, 500, "GOOG", 99);
            sendOrder("BEAR01", SELL, 600, "GOOG", 101);

            var bids = handler.getBids(BufferUtils.fromAsciiString("AAPL"));
            then(bids.size()).isEqualTo(2);
            validate(bids.getFirst(), 1, BUY, 100, "AAPL", 100);
            validate((Order) bids.getFirst().next, 2, BUY, 200, "AAPL", 99);
            var asks = handler.getAsks(BufferUtils.fromAsciiString("AAPL"));
            then(asks.size()).isEqualTo(2);
            validate(asks.getFirst(), 4, SELL, 400, "AAPL", 101);
            validate((Order) asks.getFirst().next, 3, SELL, 300, "AAPL", 102);
            bids = handler.getBids(BufferUtils.fromAsciiString("GOOG"));
            then(bids.size()).isEqualTo(1);
            validate(bids.getFirst(), 5, BUY, 500, "GOOG", 99);
            asks = handler.getAsks(BufferUtils.fromAsciiString("GOOG"));
            then(asks.size()).isEqualTo(1);
            validate(asks.getFirst(), 6, SELL, 600, "GOOG", 101);
        }

        @Test
        void addOrder_with_zero_qty_is_rejected() {
            sendOrder("BEAR01", BUY, 0, "AAPL", 100);

            RejectOrderDecoder decoder = eventPublisher.remove();
            then(decoder.reasonAsString()).isEqualTo("invalid qty");
        }

        @Test
        void addOrder_with_negative_qty_is_rejected() {
            sendOrder("BEAR01", BUY, -100, "AAPL", 100);

            RejectOrderDecoder decoder = eventPublisher.remove();
            then(decoder.reasonAsString()).isEqualTo("invalid qty");
        }

        @Test
        void addOrder_with_zero_price_is_rejected() {
            sendOrder("BEAR01", BUY, 100, "AAPL", 0);

            RejectOrderDecoder decoder = eventPublisher.remove();
            then(decoder.reasonAsString()).isEqualTo("invalid price");
        }

        @Test
        void addOrder_with_negative_price_is_rejected() {
            sendOrder("BEAR01", BUY, 100, "AAPL", -100);

            RejectOrderDecoder decoder = eventPublisher.remove();
            then(decoder.reasonAsString()).isEqualTo("invalid price");
        }

        @Test
        void addOrder_with_invalid_instrumentId_is_rejected() {
            sendOrder("BEAR01", BUY, 100, "FOO", -100);

            RejectOrderDecoder decoder = eventPublisher.remove();
            then(decoder.reasonAsString()).isEqualTo("invalid instrumentId");
        }

        @Test
        void addOrder_with_invalid_side_is_rejected() {
            var appId = handler.getAppId(BufferUtils.fromAsciiString("BEAR01"));
            var seqNum = busServer.getApplicationSequenceNumber(appId) + 1;

            busServer.publishCommand(new AddOrderEncoder()
                    .setApplicationId(appId)
                    .setApplicationSequenceNumber(seqNum)
                    .setSide((byte) 0)
                    .setQty(1000)
                    .setInstrumentId(handler.getInstrumentId(BufferUtils.fromAsciiString("GOOG")))
                    .setPrice(100));

            RejectOrderDecoder decoder = eventPublisher.remove();
            then(decoder.getApplicationId()).isEqualTo(appId);
            then(decoder.getApplicationSequenceNumber()).isEqualTo(seqNum);
            then(decoder.reasonAsString()).isEqualTo("invalid side");
        }

        @Test
        void fill_buy_completely() {
            sendOrder("BEAR01", SELL, 100, "AAPL", 100);
            sendOrder("BEAR01", SELL, 200, "AAPL", 99);
            sendOrder("BEAR01", SELL, 300, "AAPL", 101);
            sendOrder("BEAR01", SELL, 400, "AAPL", 99);
            eventPublisher.removeAll();

            sendOrder("LEHM01", BUY, 100, "AAPL", 99);

            eventPublisher.remove();
            FillOrderDecoder fillAgg = eventPublisher.remove();
            then(fillAgg.getOrderId()).isEqualTo(5);
            then(fillAgg.getPrice()).isEqualTo(99);
            then(fillAgg.getQty()).isEqualTo(100);
            FillOrderDecoder fillPass = eventPublisher.remove();
            then(fillPass.getOrderId()).isEqualTo(2);
            then(fillPass.getPrice()).isEqualTo(99);
            then(fillPass.getQty()).isEqualTo(100);
            then(handler.getBids(BufferUtils.fromAsciiString("AAPL")).size()).isEqualTo(0);
            var asks = handler.getAsks(BufferUtils.fromAsciiString("AAPL"));
            then(asks.size()).isEqualTo(4);
            var ask = asks.getFirst();
            then(ask.price).isEqualTo(99);
            then(ask.qty).isEqualTo(100);
            ask = ask.next.getItem();
            then(ask.price).isEqualTo(99);
            then(ask.qty).isEqualTo(400);
            ask = ask.next.getItem();
            then(ask.price).isEqualTo(100);
            then(ask.qty).isEqualTo(100);
            ask = ask.next.getItem();
            then(ask.price).isEqualTo(101);
            then(ask.qty).isEqualTo(300);
        }

        @Test
        void partially_fill_buy_and_rest() {
            sendOrder("BEAR01", SELL, 100, "AAPL", 100);
            sendOrder("BEAR01", SELL, 200, "AAPL", 99);
            sendOrder("BEAR01", SELL, 300, "AAPL", 101);
            sendOrder("BEAR01", SELL, 400, "AAPL", 99);
            eventPublisher.removeAll();

            sendOrder("LEHM01", BUY, 1000, "AAPL", 100);

            eventPublisher.remove();
            FillOrderDecoder fillAgg = eventPublisher.remove();
            then(fillAgg.getOrderId()).isEqualTo(5);
            then(fillAgg.getPrice()).isEqualTo(99);
            then(fillAgg.getQty()).isEqualTo(200);
            FillOrderDecoder fillPass = eventPublisher.remove();
            then(fillPass.getOrderId()).isEqualTo(2);
            then(fillPass.getPrice()).isEqualTo(99);
            then(fillPass.getQty()).isEqualTo(200);
            fillAgg = eventPublisher.remove();
            then(fillAgg.getOrderId()).isEqualTo(5);
            then(fillAgg.getPrice()).isEqualTo(99);
            then(fillAgg.getQty()).isEqualTo(400);
            fillPass = eventPublisher.remove();
            then(fillPass.getOrderId()).isEqualTo(4);
            then(fillPass.getPrice()).isEqualTo(99);
            then(fillPass.getQty()).isEqualTo(400);
            fillAgg = eventPublisher.remove();
            then(fillAgg.getOrderId()).isEqualTo(5);
            then(fillAgg.getPrice()).isEqualTo(100);
            then(fillAgg.getQty()).isEqualTo(100);
            fillPass = eventPublisher.remove();
            then(fillPass.getOrderId()).isEqualTo(1);
            then(fillPass.getPrice()).isEqualTo(100);
            then(fillPass.getQty()).isEqualTo(100);
            var bids = handler.getBids(BufferUtils.fromAsciiString("AAPL"));
            then(bids.size()).isEqualTo(1);
            var bid = bids.getFirst();
            then(bid.price).isEqualTo(100);
            then(bid.qty).isEqualTo(300);
            var asks = handler.getAsks(BufferUtils.fromAsciiString("AAPL"));
            var ask = asks.getFirst();
            then(ask.price).isEqualTo(101);
            then(ask.qty).isEqualTo(300);
        }

        @Test
        void buy_sweep_book() {
            sendOrder("BEAR01", SELL, 100, "AAPL", 100);
            sendOrder("BEAR01", SELL, 200, "AAPL", 99);
            sendOrder("BEAR01", SELL, 300, "AAPL", 101);
            sendOrder("BEAR01", SELL, 400, "AAPL", 99);
            eventPublisher.removeAll();

            sendOrder("LEHM01", BUY, 1500, "AAPL", 102);

            eventPublisher.remove();
            FillOrderDecoder fillAgg = eventPublisher.remove();
            then(fillAgg.getOrderId()).isEqualTo(5);
            then(fillAgg.getPrice()).isEqualTo(99);
            then(fillAgg.getQty()).isEqualTo(200);
            FillOrderDecoder fillPass = eventPublisher.remove();
            then(fillPass.getOrderId()).isEqualTo(2);
            then(fillPass.getPrice()).isEqualTo(99);
            then(fillPass.getQty()).isEqualTo(200);
            fillAgg = eventPublisher.remove();
            then(fillAgg.getOrderId()).isEqualTo(5);
            then(fillAgg.getPrice()).isEqualTo(99);
            then(fillAgg.getQty()).isEqualTo(400);
            fillPass = eventPublisher.remove();
            then(fillPass.getOrderId()).isEqualTo(4);
            then(fillPass.getPrice()).isEqualTo(99);
            then(fillPass.getQty()).isEqualTo(400);
            fillAgg = eventPublisher.remove();
            then(fillAgg.getOrderId()).isEqualTo(5);
            then(fillAgg.getPrice()).isEqualTo(100);
            then(fillAgg.getQty()).isEqualTo(100);
            fillPass = eventPublisher.remove();
            then(fillPass.getOrderId()).isEqualTo(1);
            then(fillPass.getPrice()).isEqualTo(100);
            then(fillPass.getQty()).isEqualTo(100);
            fillAgg = eventPublisher.remove();
            then(fillAgg.getOrderId()).isEqualTo(5);
            then(fillAgg.getPrice()).isEqualTo(101);
            then(fillAgg.getQty()).isEqualTo(300);
            fillPass = eventPublisher.remove();
            then(fillPass.getOrderId()).isEqualTo(3);
            then(fillPass.getPrice()).isEqualTo(101);
            then(fillPass.getQty()).isEqualTo(300);
            var bids = handler.getBids(BufferUtils.fromAsciiString("AAPL"));
            then(bids.size()).isEqualTo(1);
            var bid = bids.getFirst();
            then(bid.price).isEqualTo(102);
            then(bid.qty).isEqualTo(500);
            var asks = handler.getAsks(BufferUtils.fromAsciiString("AAPL"));
            then(asks.size()).isEqualTo(0);
        }


        @Test
        void fill_sell_completely() {
            sendOrder("BEAR01", BUY, 100, "AAPL", 100);
            sendOrder("BEAR01", BUY, 200, "AAPL", 99);
            sendOrder("BEAR01", BUY, 300, "AAPL", 101);
            sendOrder("BEAR01", BUY, 400, "AAPL", 99);
            eventPublisher.removeAll();

            sendOrder("LEHM01", SELL, 100, "AAPL", 101);

            eventPublisher.remove();
            FillOrderDecoder fillAgg = eventPublisher.remove();
            then(fillAgg.getOrderId()).isEqualTo(5);
            then(fillAgg.getPrice()).isEqualTo(101);
            then(fillAgg.getQty()).isEqualTo(100);
            FillOrderDecoder fillPass = eventPublisher.remove();
            then(fillPass.getOrderId()).isEqualTo(3);
            then(fillPass.getPrice()).isEqualTo(101);
            then(fillPass.getQty()).isEqualTo(100);
            var asks = handler.getAsks(BufferUtils.fromAsciiString("AAPL"));
            then(asks.size()).isEqualTo(0);
            var bids = handler.getBids(BufferUtils.fromAsciiString("AAPL"));
            then(bids.size()).isEqualTo(4);
            var bid = bids.getFirst();
            then(bid.price).isEqualTo(101);
            then(bid.qty).isEqualTo(200);
            bid = bid.next.getItem();
            then(bid.price).isEqualTo(100);
            then(bid.qty).isEqualTo(100);
            bid = bid.next.getItem();
            then(bid.price).isEqualTo(99);
            then(bid.qty).isEqualTo(200);
            bid = bid.next.getItem();
            then(bid.price).isEqualTo(99);
            then(bid.qty).isEqualTo(400);
        }

        @Test
        void fill_ask_and_rest() {
            sendOrder("BEAR01", BUY, 100, "AAPL", 100);
            sendOrder("BEAR01", BUY, 200, "AAPL", 99);
            sendOrder("BEAR01", BUY, 300, "AAPL", 101);
            sendOrder("BEAR01", BUY, 400, "AAPL", 99);
            eventPublisher.removeAll();

            sendOrder("LEHM01", SELL, 1000, "AAPL", 100);

            eventPublisher.remove();
            FillOrderDecoder fillAgg = eventPublisher.remove();
            then(fillAgg.getOrderId()).isEqualTo(5);
            then(fillAgg.getPrice()).isEqualTo(101);
            then(fillAgg.getQty()).isEqualTo(300);
            FillOrderDecoder fillPass = eventPublisher.remove();
            then(fillPass.getOrderId()).isEqualTo(3);
            then(fillPass.getPrice()).isEqualTo(101);
            then(fillPass.getQty()).isEqualTo(300);
            fillAgg = eventPublisher.remove();
            then(fillAgg.getOrderId()).isEqualTo(5);
            then(fillAgg.getPrice()).isEqualTo(100);
            then(fillAgg.getQty()).isEqualTo(100);
            fillPass = eventPublisher.remove();
            then(fillPass.getOrderId()).isEqualTo(1);
            then(fillPass.getPrice()).isEqualTo(100);
            then(fillPass.getQty()).isEqualTo(100);
            var asks = handler.getAsks(BufferUtils.fromAsciiString("AAPL"));
            then(asks.size()).isEqualTo(1);
            var ask = asks.getFirst();
            then(ask.price).isEqualTo(100);
            then(ask.qty).isEqualTo(600);
            var bids = handler.getBids(BufferUtils.fromAsciiString("AAPL"));
            then(bids.size()).isEqualTo(2);
            var bid = bids.getFirst();
            then(bid.price).isEqualTo(99);
            then(bid.qty).isEqualTo(200);
            bid = (Order) bid.next;
            then(bid.price).isEqualTo(99);
            then(bid.qty).isEqualTo(400);
        }

        @Test
        void sweep_book() {
            sendOrder("BEAR01", BUY, 100, "AAPL", 100);
            sendOrder("BEAR01", BUY, 200, "AAPL", 99);
            sendOrder("BEAR01", BUY, 300, "AAPL", 101);
            sendOrder("BEAR01", BUY, 400, "AAPL", 99);
            eventPublisher.removeAll();

            sendOrder("LEHM01", SELL, 1500, "AAPL", 95);

            eventPublisher.remove();
            FillOrderDecoder fillAgg = eventPublisher.remove();
            then(fillAgg.getOrderId()).isEqualTo(5);
            then(fillAgg.getPrice()).isEqualTo(101);
            then(fillAgg.getQty()).isEqualTo(300);
            FillOrderDecoder fillPass = eventPublisher.remove();
            then(fillPass.getOrderId()).isEqualTo(3);
            then(fillPass.getPrice()).isEqualTo(101);
            then(fillPass.getQty()).isEqualTo(300);
            fillAgg = eventPublisher.remove();
            then(fillAgg.getOrderId()).isEqualTo(5);
            then(fillAgg.getPrice()).isEqualTo(100);
            then(fillAgg.getQty()).isEqualTo(100);
            fillPass = eventPublisher.remove();
            then(fillPass.getOrderId()).isEqualTo(1);
            then(fillPass.getPrice()).isEqualTo(100);
            then(fillPass.getQty()).isEqualTo(100);
            fillAgg = eventPublisher.remove();
            then(fillAgg.getOrderId()).isEqualTo(5);
            then(fillAgg.getPrice()).isEqualTo(99);
            then(fillAgg.getQty()).isEqualTo(200);
            fillPass = eventPublisher.remove();
            then(fillPass.getOrderId()).isEqualTo(2);
            then(fillPass.getPrice()).isEqualTo(99);
            then(fillPass.getQty()).isEqualTo(200);
            fillAgg = eventPublisher.remove();
            then(fillAgg.getOrderId()).isEqualTo(5);
            then(fillAgg.getPrice()).isEqualTo(99);
            then(fillAgg.getQty()).isEqualTo(400);
            fillPass = eventPublisher.remove();
            then(fillPass.getOrderId()).isEqualTo(4);
            then(fillPass.getPrice()).isEqualTo(99);
            then(fillPass.getQty()).isEqualTo(400);
            var bids = handler.getBids(BufferUtils.fromAsciiString("AAPL"));
            then(bids.size()).isEqualTo(0);
            var asks = handler.getAsks(BufferUtils.fromAsciiString("AAPL"));
            then(asks.size()).isEqualTo(1);
            var ask = asks.getFirst();
            then(ask.price).isEqualTo(95);
            then(ask.qty).isEqualTo(500);
        }
    }

    @Nested
    class CancelOrderTests {

        @BeforeEach
        void before_each() {
            sendOrder("LEHM01", SELL, 100, "AAPL", 100);
            sendOrder("LEHM01", SELL, 200, "AAPL", 99);
            sendOrder("BEAR01", SELL, 300, "AAPL", 101);
            sendOrder("LEHM01", BUY, 400, "AAPL", 97);
            sendOrder("BEAR01", BUY, 500, "AAPL", 98);
            sendOrder("LEHM01", BUY, 600, "AAPL", 96);
        }

        @Test
        void cancelOrder_new_best_bid() {
            sendCancel("BEAR01", 5);

            var bids = handler.getBids(BufferUtils.fromAsciiString("AAPL"));
            then(bids.size()).isEqualTo(2);
            validate(bids.getFirst(), 4, BUY, 400, "AAPL", 97);
            validate((Order) bids.getFirst().next, 6, BUY, 600, "AAPL", 96);
        }

        @Test
        void cancelOrder_middle_bid() {
            sendCancel("BEAR01", 4);

            var bids = handler.getBids(BufferUtils.fromAsciiString("AAPL"));
            then(bids.size()).isEqualTo(2);
            validate(bids.getFirst(), 5, BUY, 500, "AAPL", 98);
            validate((Order) bids.getFirst().next, 6, BUY, 600, "AAPL", 96);
        }

        @Test
        void cancelOrder_worst_bid() {
            sendCancel("BEAR01", 6);

            var bids = handler.getBids(BufferUtils.fromAsciiString("AAPL"));
            then(bids.size()).isEqualTo(2);
            validate(bids.getFirst(), 5, BUY, 500, "AAPL", 98);
            validate((Order) bids.getFirst().next, 4, BUY, 400, "AAPL", 97);
        }

        @Test
        void cancelOrder_new_best_ask() {
            sendCancel("BEAR01", 2);

            var asks = handler.getAsks(BufferUtils.fromAsciiString("AAPL"));
            then(asks.size()).isEqualTo(2);
            validate(asks.getFirst(), 1, SELL, 100, "AAPL", 100);
            validate((Order) asks.getFirst().next, 3, SELL, 300, "AAPL", 101);
        }

        @Test
        void cancelOrder_middle_ask() {
            sendCancel("BEAR01", 1);

            var asks = handler.getAsks(BufferUtils.fromAsciiString("AAPL"));
            then(asks.size()).isEqualTo(2);
            validate(asks.getFirst(), 2, SELL, 200, "AAPL", 99);
            validate((Order) asks.getFirst().next, 3, SELL, 300, "AAPL", 101);
        }

        @Test
        void cancelOrder_worst_ask() {
            sendCancel("BEAR01", 3);

            var asks = handler.getAsks(BufferUtils.fromAsciiString("AAPL"));
            then(asks.size()).isEqualTo(2);
            validate(asks.getFirst(), 2, SELL, 200, "AAPL", 99);
            validate((Order) asks.getFirst().next, 1, SELL, 100, "AAPL", 100);
        }

        @Test
        void cancelOrder_unknown_orderId_is_rejected() {
            eventPublisher.removeAll();

            sendCancel("BEAR01", 7);

            RejectCancelDecoder decoder = eventPublisher.remove();
            then(decoder.getOrderId()).isEqualTo(7);
            then(decoder.reasonAsString()).isEqualTo("unknown order");
        }

        @Test
        void cancelOrder_already_removed_orderId_is_rejected() {
            sendCancel("BEAR01", 3);
            eventPublisher.removeAll();

            sendCancel("BEAR01", 3);

            RejectCancelDecoder decoder = eventPublisher.remove();
            then(decoder.getOrderId()).isEqualTo(3);
            then(decoder.reasonAsString()).isEqualTo("too late to cancel");
        }

        @Test
        void cancelOrder_already_filled_orderId_is_rejected() {
            sendOrder("BEAR01", BUY, 700, "AAPL", 99);
            eventPublisher.removeAll();

            sendCancel("LEHM01", 2);

            RejectCancelDecoder decoder = eventPublisher.remove();
            then(decoder.getOrderId()).isEqualTo(2);
            then(decoder.reasonAsString()).isEqualTo("too late to cancel");
        }
    }

    @Nested
    class MiscEventTests {

        @Test
        void heartbeat_is_copied() {
            var appId = handler.getAppId(BufferUtils.fromAsciiString("SEQ01"));
            var seqNum = busServer.getApplicationSequenceNumber(appId) + 1;

            busServer.publishCommand(new HeartbeatEncoder()
                    .setApplicationId(appId)
                    .setApplicationSequenceNumber(seqNum));

            HeartbeatDecoder hb = eventPublisher.remove();
            then(hb.getApplicationId()).isEqualTo(appId);
            then(hb.getApplicationSequenceNumber()).isEqualTo(seqNum);
        }

        @Test
        void appDisc_is_copied() {
            var appId = handler.getAppId(BufferUtils.fromAsciiString("LEHM01"));
            var seqNum = busServer.getApplicationSequenceNumber(appId) + 1;

            busServer.publishCommand(new ApplicationDiscoveryEncoder()
                    .setApplicationId(appId)
                    .setApplicationSequenceNumber(seqNum)
                    .setCommandPath("/foo/bar")
                    .setVmName("vm01")
                    .setActivationStatus(Status.UP));

            ApplicationDiscoveryDecoder disc = eventPublisher.remove();
            then(disc.commandPathAsString()).isEqualTo("/foo/bar");
            then(disc.vmNameAsString()).isEqualTo("vm01");
            then(disc.getActivationStatus()).isEqualTo(Status.UP);
        }
    }

    private void sendOrder(String sender, Side side, int qty, String symbol, int price) {
        var appId = handler.getAppId(BufferUtils.fromAsciiString(sender));
        var seqNum = busServer.getApplicationSequenceNumber(appId) + 1;
        busServer.publishCommand(new AddOrderEncoder()
                .setApplicationId(appId)
                .setApplicationSequenceNumber(seqNum)
                .setSide(side)
                .setQty(qty)
                .setInstrumentId(handler.getInstrumentId(BufferUtils.fromAsciiString(symbol)))
                .setPrice(price));
    }

    private void sendCancel(String sender, int orderId) {
        var appId = handler.getAppId(BufferUtils.fromAsciiString(sender));
        var seqNum = busServer.getApplicationSequenceNumber(appId) + 1;
        busServer.publishCommand(new CancelOrderEncoder()
                .setApplicationId(appId)
                .setApplicationSequenceNumber(seqNum)
                .setOrderId(orderId));
    }

    private void publishAppDefCommand(String appName) {
        busServer.publishCommand(new ApplicationDefinitionEncoder()
                .setApplicationSequenceNumber(1)
                .setName(appName));
        busServer.getEventPublisher().remove();
    }

    private void publishEquityDefCommand(String equity) {
        var appId = handler.getAppId(BufferUtils.fromAsciiString("REFDATA01"));
        busServer.publishCommand(new EquityDefinitionEncoder()
                .setApplicationId(appId)
                .setApplicationSequenceNumber(busServer.getApplicationSequenceNumber(appId) + 1)
                .setTicker(equity));
        busServer.getEventPublisher().remove();
    }

    private void validate(Order order,
                          int orderId, Side side, long qty, String symbol, long price) {
        then(order.orderId).isEqualTo(orderId);
        then(order.sell).isEqualTo(side == SELL);
        then(order.qty).isEqualTo(qty);
        then(order.instrumentId).isEqualTo(handler.getInstrumentId(BufferUtils.fromAsciiString(symbol)));
        then(order.price).isEqualTo(price);
    }
}
