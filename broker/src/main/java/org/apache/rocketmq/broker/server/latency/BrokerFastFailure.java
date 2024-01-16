/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.server.latency;

import org.apache.rocketmq.broker.server.BrokerController;
import org.apache.rocketmq.broker.server.daemon.BrokerNettyServer;
import org.apache.rocketmq.common.app.AbstractBrokerRunnable;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.lang.thread.ThreadFactoryImpl;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.lang.future.FutureTaskExt;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * BrokerFastFailure will cover {@link BrokerController#getBrokerNettyServer()#getSendThreadPoolQueue()} and
 * {@link BrokerController#getBrokerNettyServer()#getPullThreadPoolQueue()}
 */
public class BrokerFastFailure {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final ScheduledExecutorService scheduledExecutorService;
    private final BrokerController brokerController;

    private volatile long jstackTime = System.currentTimeMillis();

    public BrokerFastFailure(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.scheduledExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new ThreadFactoryImpl("BrokerFastFailureScheduledThread", true,
                brokerController == null ? null : brokerController.getBrokerConfig()));
    }

    public static RequestTask castRunnable(final Runnable runnable) {
        try {
            if (runnable instanceof FutureTaskExt) {
                FutureTaskExt object = (FutureTaskExt) runnable;
                return (RequestTask) object.getRunnable();
            }
        } catch (Throwable e) {
            LOGGER.error(String.format("castRunnable exception, %s", runnable.getClass().getName()), e);
        }

        return null;
    }

    public void start() {
        this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.brokerController.getBrokerConfig()) {
            @Override
            public void run0() {
                if (brokerController.getBrokerConfig().isBrokerFastFailureEnable()) {
                    cleanExpiredRequest();
                }
            }
        }, 1000, 10, TimeUnit.MILLISECONDS);
    }

    private void cleanExpiredRequest() {
        responseSystemBusy();

        BrokerNettyServer nettyServer = this.brokerController.getBrokerNettyServer();
        BrokerConfig brokerConfig = this.brokerController.getBrokerConfig();

        cleanExpiredRequestInQueue(nettyServer.getSendThreadPoolQueue(), brokerConfig.getWaitTimeMillsInSendQueue());
        cleanExpiredRequestInQueue(nettyServer.getPullThreadPoolQueue(), brokerConfig.getWaitTimeMillsInPullQueue());
        cleanExpiredRequestInQueue(nettyServer.getLitePullThreadPoolQueue(), brokerConfig.getWaitTimeMillsInLitePullQueue());
        cleanExpiredRequestInQueue(nettyServer.getHeartbeatThreadPoolQueue(), brokerConfig.getWaitTimeMillsInHeartbeatQueue());
        cleanExpiredRequestInQueue(nettyServer.getEndTransactionThreadPoolQueue(), brokerConfig.getWaitTimeMillsInTransactionQueue());
        cleanExpiredRequestInQueue(nettyServer.getAckThreadPoolQueue(), brokerConfig.getWaitTimeMillsInAckQueue());
    }

    private void responseSystemBusy() {
        while (this.brokerController.getMessageStore().isOSPageCacheBusy()) {
            try {
                if (this.brokerController.getBrokerNettyServer().getSendThreadPoolQueue().isEmpty()) {
                    break;
                }

                final Runnable runnable = this.brokerController.getBrokerNettyServer().getSendThreadPoolQueue().poll(0, TimeUnit.SECONDS);
                if (null == runnable) {
                    break;
                }

                final RequestTask rt = castRunnable(runnable);
                if (rt == null) {
                    continue;
                }

                rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, String.format(
                    "[PCBUSY_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms, "
                        + "size of queue: %d", System.currentTimeMillis() - rt.getCreateTimestamp(),
                    this.brokerController.getBrokerNettyServer().getSendThreadPoolQueue().size()));
            } catch (Throwable ignored) {
            }
        }
    }

    void cleanExpiredRequestInQueue(final BlockingQueue<Runnable> blockingQueue, final long maxWaitTimeMillsInQueue) {
        while (true) {
            try {
                if (blockingQueue.isEmpty()) {
                    break;
                }

                final Runnable runnable = blockingQueue.peek();
                if (null == runnable) {
                    break;
                }

                final RequestTask rt = castRunnable(runnable);
                if (rt == null || rt.isStopRun()) {
                    break;
                }

                final long behind = System.currentTimeMillis() - rt.getCreateTimestamp();
                if (behind < maxWaitTimeMillsInQueue) {
                    break;
                }

                if (!blockingQueue.remove(runnable)) {
                    continue;
                }

                rt.setStopRun(true);
                rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, String.format("[TIMEOUT_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms, size of queue: %d", behind, blockingQueue.size()));
                if (System.currentTimeMillis() - jstackTime > 15000) {
                    jstackTime = System.currentTimeMillis();
                    LOGGER.warn("broker jstack \n " + IOUtils.jstack());
                }
            } catch (Throwable ignored) {
            }
        }
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }
}
