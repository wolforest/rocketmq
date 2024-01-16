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
package org.apache.rocketmq.broker.server.daemon.pop;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.server.BrokerController;
import org.apache.rocketmq.broker.api.controller.PopMessageProcessor;
import org.apache.rocketmq.common.domain.constant.PopConstants;
import org.apache.rocketmq.common.lang.thread.ServiceThread;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class QueueLockManager extends ServiceThread {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);

    private final ConcurrentHashMap<String, TimedLock> expiredLocalCache = new ConcurrentHashMap<>(100000);
    private final BrokerController brokerController;

    public QueueLockManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public String buildLockKey(String topic, String consumerGroup, int queueId) {
        return topic + PopConstants.SPLIT + consumerGroup + PopConstants.SPLIT + queueId;
    }

    public boolean tryLock(String topic, String consumerGroup, int queueId) {
        return tryLock(buildLockKey(topic, consumerGroup, queueId));
    }

    public boolean tryLock(String key) {
        TimedLock timedLock = expiredLocalCache.get(key);

        if (timedLock == null) {
            TimedLock old = expiredLocalCache.putIfAbsent(key, new TimedLock());
            if (old != null) {
                return false;
            } else {
                timedLock = expiredLocalCache.get(key);
            }
        }

        if (timedLock == null) {
            return false;
        }

        return timedLock.tryLock();
    }

    /**
     * is not thread safe, may cause duplicate lock
     *
     * @param usedExpireMillis the expired time in millisecond
     * @return total numbers of TimedLock
     */
    public int cleanUnusedLock(final long usedExpireMillis) {
        Iterator<Map.Entry<String, TimedLock>> iterator = expiredLocalCache.entrySet().iterator();

        int total = 0;
        while (iterator.hasNext()) {
            Map.Entry<String, TimedLock> entry = iterator.next();

            if (System.currentTimeMillis() - entry.getValue().getLockTime() > usedExpireMillis) {
                iterator.remove();
                POP_LOGGER.info("Remove unused queue lock: {}, {}, {}", entry.getKey(),
                    entry.getValue().getLockTime(),
                    entry.getValue().isLock());
            }

            total++;
        }

        return total;
    }

    public void unLock(String topic, String consumerGroup, int queueId) {
        unLock(buildLockKey(topic, consumerGroup, queueId));
    }

    public void unLock(String key) {
        TimedLock timedLock = expiredLocalCache.get(key);
        if (timedLock != null) {
            timedLock.unLock();
        }
    }

    @Override
    public String getServiceName() {
        PopMessageProcessor popMessageProcessor = brokerController.getBrokerNettyServer().getPopMessageProcessor();
        if (popMessageProcessor.getBrokerController().getBrokerConfig().isInBrokerContainer()) {
            return popMessageProcessor.getBrokerController().getBrokerIdentity().getIdentifier() + QueueLockManager.class.getSimpleName();
        }
        return QueueLockManager.class.getSimpleName();
    }

    @Override
    public void run() {
        while (!isStopped()) {
            try {
                this.waitForRunning(60000);
                int count = cleanUnusedLock(60000);
                POP_LOGGER.info("QueueLockSize={}", count);
            } catch (Exception e) {
                POP_LOGGER.error("QueueLockManager run error", e);
            }
        }
    }
}

