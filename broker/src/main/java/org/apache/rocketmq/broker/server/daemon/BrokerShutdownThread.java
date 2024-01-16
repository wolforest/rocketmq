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
package org.apache.rocketmq.broker.server.daemon;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.server.BrokerController;
import org.apache.rocketmq.common.domain.constant.LoggerName;
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
