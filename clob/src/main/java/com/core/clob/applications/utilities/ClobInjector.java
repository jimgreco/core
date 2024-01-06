package com.core.clob.applications.utilities;

import com.core.clob.ClobConstants;
import com.core.clob.schema.Side;
import com.core.infrastructure.Allocation;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.log.LogFactory;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.applications.utilities.Injector;
import com.core.platform.bus.BusClient;

/**
 * The {@code ClobInjector} extends {@code Injector} to provide methods to add and remove orders to/from the limit order
 * book.
 *
 * @see Injector for activation information
 */
public class ClobInjector extends Injector {

    /**
     * Creates an {@code ClobInjector} with the specified parameters.
     *
     * @param logFactory       a factory to create logs
     * @param activatorFactory a factory of activators
     * @param busClient        the bus client
     * @param applicationName  the name of this application
     */
    public ClobInjector(LogFactory logFactory,
                        ActivatorFactory activatorFactory,
                        BusClient<?, ?> busClient,
                        String applicationName) {
        super(logFactory, activatorFactory, busClient, applicationName);
    }

    /**
     * Adds a new order to the limit order book with the specified parameters.
     *
     * @param side the side of the order
     * @param qty the quantity of the order
     * @param symbol the symbol of the order's instrument
     * @param price the limit price of the order
     */
    @Allocation
    @Command
    public void addOrder(Side side, double qty, String symbol, double price) {
        send("addOrder",
                "side=" + side,
                "qty=" + ClobConstants.QTY_ENCODER.toLong(qty),
                "instrumentId=" + symbol,
                "price=" + ClobConstants.PRICE_ENCODER.toLong(price));
    }

    /**
     * Cancels an order in the limit order book with the specified order identifier.
     *
     * @param orderId the identifier of the order to cancel.
     */
    @Allocation
    @Command
    public void cancelOrder(int orderId) {
        send("cancelOrder", "orderId=" + orderId);
    }
}
