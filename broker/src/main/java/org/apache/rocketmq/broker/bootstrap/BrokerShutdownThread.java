package org.apache.rocketmq.broker.bootstrap;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class BrokerShutdownThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private volatile boolean hasShutdown = false;
    private final AtomicInteger shutdownTimes = new AtomicInteger(0);
    private final BrokerController brokerController;

    public BrokerShutdownThread(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public void run() {
        synchronized (this) {
            LOG.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
            if (this.hasShutdown) {
                return;
            }

            this.hasShutdown = true;
            long beginTime = System.currentTimeMillis();
            brokerController.shutdown();
            long consumingTimeTotal = System.currentTimeMillis() - beginTime;
            LOG.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
        }
    }
}
