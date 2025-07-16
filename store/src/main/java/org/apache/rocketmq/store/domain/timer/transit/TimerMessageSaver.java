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
package org.apache.rocketmq.store.domain.timer.transit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.lang.thread.ServiceThread;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerLog;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerWheel;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerWheelPersistence;
import org.apache.rocketmq.store.server.metrics.DefaultStoreMetricsManager;
import org.apache.rocketmq.store.domain.timer.persistence.Persistence;
import org.apache.rocketmq.store.domain.timer.metrics.TimerMetricManager;
import org.apache.rocketmq.store.domain.timer.model.TimerEvent;
import org.apache.rocketmq.store.domain.timer.model.TimerState;
import org.apache.rocketmq.store.server.metrics.PerfCounter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.rocketmq.store.domain.timer.TimerMessageStore.ENQUEUE_PUT;

/**
 * @renamed from TimerEnqueuePutService to TimerMessageSaver
 * poll message from fetchedTimerMessageQueue
 *      put message to timerWheel persistent storage
 *      or enqueue TimerMessageStore.timerMessageDeliverQueue
 *
 */
public class TimerMessageSaver extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final int BATCH_SIZE = 10;

    private final TimerState timerState;
    private final MessageStoreConfig storeConfig;
    private final MessageOperator messageOperator;

    private final BlockingQueue<TimerEvent> fetchedTimerMessageQueue;
    private final BlockingQueue<TimerEvent> timerMessageDeliverQueue;
    private final TimerMessageProducer[] timerMessageProducers;
    private final TimerMessageQuerier[] timerMessageQueries;
    private final PerfCounter.Ticks perfCounterTicks;

    private final Persistence persistence;
    public TimerMessageSaver(TimerState timerState,
                             MessageStoreConfig storeConfig,
                             TimerWheel timerWheel,
                             TimerLog timerLog,
                             MessageOperator messageOperator,
                             BlockingQueue<TimerEvent> fetchedTimerMessageQueue,
                             BlockingQueue<TimerEvent> timerMessageDeliverQueue,
                             TimerMessageProducer[] timerMessageProducers,
                             TimerMessageQuerier[] timerMessageQueries,
                             TimerMetricManager metricManager,
                             PerfCounter.Ticks perfCounterTicks) {
        this.timerState = timerState;
        this.storeConfig = storeConfig;
        this.messageOperator = messageOperator;

        this.fetchedTimerMessageQueue = fetchedTimerMessageQueue;
        this.timerMessageDeliverQueue = timerMessageDeliverQueue;
        this.timerMessageProducers = timerMessageProducers;
        this.timerMessageQueries = timerMessageQueries;
        this.perfCounterTicks = perfCounterTicks;

        this.persistence = new TimerWheelPersistence(timerState,timerWheel,timerLog,storeConfig,metricManager,perfCounterTicks);
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service start");
        while (!this.isStopped() || fetchedTimerMessageQueue.size() != 0) {
            try {
                fetchAndPutTimerRequest();
            } catch (Throwable e) {
                LOGGER.error("Unknown error", e);
            }
        }
        LOGGER.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return timerState.getServiceThreadName() + this.getClass().getSimpleName();
    }

    private void fetchAndPutTimerRequest() throws Exception {
        long tmpCommitQueueOffset = timerState.currQueueOffset;
        List<TimerEvent> timerEvents = this.fetchTimerRequests();
        if (CollectionUtils.isEmpty(timerEvents)) {
            timerState.commitQueueOffset = tmpCommitQueueOffset;
            timerState.maybeMoveWriteTime();
            return;
        }

        while (!isStopped()) {
            CountDownLatch latch = new CountDownLatch(timerEvents.size());
            for (TimerEvent req : timerEvents) {
                req.setLatch(latch);
                this.putToTimerWheelOrEnqueueDeliverQueue(req);
            }

            timerState.checkDeliverQueueLatch(latch, fetchedTimerMessageQueue, timerMessageProducers, timerMessageQueries, -1);

            boolean allSuccess = timerEvents.stream().allMatch(TimerEvent::isSuccess);
            if (allSuccess) {
                break;
            }

            ThreadUtils.sleep(50);
        }

        timerState.commitQueueOffset = timerEvents.get(timerEvents.size() - 1).getMsg().getQueueOffset();
        timerState.maybeMoveWriteTime();
    }

    /**
     * collect the requests
     */
    private List<TimerEvent> fetchTimerRequests() throws InterruptedException {
        List<TimerEvent> timerEventList = null;
        TimerEvent firstReq = fetchedTimerMessageQueue.poll(10, TimeUnit.MILLISECONDS);
        if (null == firstReq) {
            return null;
        }

        timerEventList = new ArrayList<>(16);
        timerEventList.add(firstReq);
        fetchMoreTimerRequests(timerEventList);

        return timerEventList;
    }

    private void fetchMoreTimerRequests(List<TimerEvent> timerEventList) throws InterruptedException {
        while (true) {
            TimerEvent tmpReq = fetchedTimerMessageQueue.poll(3, TimeUnit.MILLISECONDS);
            if (null == tmpReq) {
                break;
            }

            timerEventList.add(tmpReq);

            if (timerEventList.size() > BATCH_SIZE) {
                break;
            }
        }
    }

    private void putToTimerWheelOrEnqueueDeliverQueue(TimerEvent timerEvent) {
        try {
            perfCounterTicks.startTick(ENQUEUE_PUT);

            DefaultStoreMetricsManager.incTimerEnqueueCount(messageOperator.getRealTopic(timerEvent.getMsg()));
            boolean shouldFire = timerEvent.getDelayTime() < timerState.currWriteTimeMs;
            if (timerState.isShouldRunningDequeue() && shouldFire) {
                timerEvent.setEnqueueTime(Long.MAX_VALUE);
                timerMessageDeliverQueue.put(timerEvent);
            } else {
                boolean success = persistence.save(timerEvent);
                timerEvent.idempotentRelease(success || storeConfig.isTimerSkipUnknownError());
            }

            perfCounterTicks.endTick(ENQUEUE_PUT);
        } catch (Throwable t) {
            handleTimerWheelAddingException(t, timerEvent);
        }
    }

    private void handleTimerWheelAddingException(Throwable t, TimerEvent timerEvent) {
        LOGGER.error("Unknown error", t);
        if (storeConfig.isTimerSkipUnknownError()) {
            timerEvent.idempotentRelease(true);
        } else {
            ThreadUtils.sleep(50);
        }
    }
}

