package com.core.platform.bus.mold;

import com.core.infrastructure.io.Selector;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.metrics.MetricFactory;
import com.core.infrastructure.time.Scheduler;
import com.core.infrastructure.time.Time;
import com.core.platform.activation.ActivatorFactory;

class TcpBusServerMessageReceiver extends TcpMessageReceiver {

    TcpBusServerMessageReceiver(
            Selector selector,
            Time time,
            Scheduler scheduler,
            LogFactory logFactory,
            MetricFactory metricFactory,
            ActivatorFactory activatorFactory,
            MoldSession moldSession,
            String name,
            String address) {
        super(selector, time, scheduler, logFactory, metricFactory, activatorFactory, moldSession, name, address);
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
