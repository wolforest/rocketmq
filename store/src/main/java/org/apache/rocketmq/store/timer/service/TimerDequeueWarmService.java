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
package org.apache.rocketmq.store.timer.service;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.timer.TimerMessageStore;

public class TimerDequeueWarmService extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private TimerMessageStore timerMessageStore;

    public TimerDequeueWarmService(TimerMessageStore timerMessageStore) {
        this.timerMessageStore = timerMessageStore;
    }

    @Override
    public String getServiceName() {
        String brokerIdentifier = "";
        if (timerMessageStore.getMessageStore() instanceof DefaultMessageStore && ((DefaultMessageStore) timerMessageStore.getMessageStore()).getBrokerConfig().isInBrokerContainer()) {
            brokerIdentifier = ((DefaultMessageStore) timerMessageStore.getMessageStore()).getBrokerConfig().getIdentifier();
        }
        return brokerIdentifier + this.getClass().getSimpleName();
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service start");
        while (!this.isStopped()) {
            try {
                //if (!storeConfig.isTimerWarmEnable() || -1 == TimerMessageStore.this.warmDequeue()) {
                waitForRunning(50);
                //}
            } catch (Throwable e) {
                LOGGER.error("Error occurred in " + getServiceName(), e);
            }
        }
        LOGGER.info(this.getServiceName() + " service end");
    }
}

