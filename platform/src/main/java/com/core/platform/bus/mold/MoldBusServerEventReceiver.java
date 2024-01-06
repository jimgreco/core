package com.core.platform.bus.mold;

import com.core.infrastructure.io.Selector;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.metrics.MetricFactory;
import com.core.infrastructure.time.Scheduler;
import com.core.platform.activation.ActivatorFactory;

class MoldBusServerEventReceiver extends MoldEventReceiver {

    MoldBusServerEventReceiver(
            String busName,
            String name,
            Selector selector,
            Scheduler scheduler,
            LogFactory logFactory,
            MetricFactory metricFactory,
            ActivatorFactory activatorFactory,
            MoldSession moldSession,
            String eventChannelAddress,
            String discoveryChannelAddress) {
        super(busName, name, selector, scheduler, logFactory, metricFactory, activatorFactory,
                moldSession, eventChannelAddress, discoveryChannelAddress);
    }

    @Override
    public void activate() {
        super.deactivate();
    }

    @Override
    public void deactivate() {
        super.activate();
    }
}
