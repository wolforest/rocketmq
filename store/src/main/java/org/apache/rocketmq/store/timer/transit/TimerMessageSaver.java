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
package org.apache.rocketmq.store.timer.transit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.metrics.DefaultStoreMetricsManager;
import org.apache.rocketmq.store.timer.MessageOperator;
import org.apache.rocketmq.store.timer.Persistence;
import org.apache.rocketmq.store.timer.TimerMetricManager;
import org.apache.rocketmq.store.timer.TimerRequest;
import org.apache.rocketmq.store.timer.TimerState;
import org.apache.rocketmq.store.timer.persistence.wheel.TimerLog;
import org.apache.rocketmq.store.timer.persistence.wheel.TimerWheel;
import org.apache.rocketmq.store.timer.persistence.wheel.TimerWheelPersistence;
import org.apache.rocketmq.store.util.PerfCounter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.rocketmq.store.timer.TimerMessageStore.ENQUEUE_PUT;

public class TimerMessageSaver extends ServiceThread {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final TimerState timerState;
    private final MessageStoreConfig storeConfig;
    private final MessageOperator messageOperator;

    private final BlockingQueue<TimerRequest> fetchedTimerMessageQueue;
    private final BlockingQueue<TimerRequest> timerMessageDeliverQueue;
    private final TimerMessageDeliver[] timerMessageDelivers;
    private final TimerMessageQuery[] timerMessageQueries;
    private final PerfCounter.Ticks perfCounterTicks;

    private final Persistence persistence;
    public TimerMessageSaver(TimerState timerState,
                             MessageStoreConfig storeConfig,
                             TimerWheel timerWheel,
                             TimerLog timerLog,
                             MessageOperator messageOperator,
                             BlockingQueue<TimerRequest> fetchedTimerMessageQueue,
                             BlockingQueue<TimerRequest> timerMessageDeliverQueue,
                             TimerMessageDeliver[] timerMessageDelivers,
                             TimerMessageQuery[] timerMessageQueries,
                             TimerMetricManager metricManager,
                             PerfCounter.Ticks perfCounterTicks) {
        this.timerState = timerState;
        this.storeConfig = storeConfig;
        this.messageOperator = messageOperator;

        this.fetchedTimerMessageQueue = fetchedTimerMessageQueue;
        this.timerMessageDeliverQueue = timerMessageDeliverQueue;
        this.timerMessageDelivers = timerMessageDelivers;
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

    /**
     * collect the requests
     */
    private List<TimerRequest> fetchTimerRequests() throws InterruptedException {
        List<TimerRequest> timerRequestList = null;
        TimerRequest firstReq = fetchedTimerMessageQueue.poll(10, TimeUnit.MILLISECONDS);
        if (null != firstReq) {
            timerRequestList = new ArrayList<>(16);
            timerRequestList.add(firstReq);
            while (true) {
                TimerRequest tmpReq = fetchedTimerMessageQueue.poll(3, TimeUnit.MILLISECONDS);
                if (null == tmpReq) {
                    break;
                }
                timerRequestList.add(tmpReq);
                if (timerRequestList.size() > 10) {
                    break;
                }
            }
        }
        return timerRequestList;
    }

    private void putMessageToTimerWheel(TimerRequest timerRequest) {
        try {
            perfCounterTicks.startTick(ENQUEUE_PUT);
            DefaultStoreMetricsManager.incTimerEnqueueCount(messageOperator.getRealTopic(timerRequest.getMsg()));
            boolean shouldFire = timerRequest.getDelayTime() < timerState.currWriteTimeMs;
            if (timerState.isShouldRunningDequeue() && shouldFire) {
                timerRequest.setEnqueueTime(Long.MAX_VALUE);
                timerMessageDeliverQueue.put(timerRequest);
            } else {
                boolean success = persistence.save(timerRequest);
                timerRequest.idempotentRelease(success || storeConfig.isTimerSkipUnknownError());
            }
            perfCounterTicks.endTick(ENQUEUE_PUT);
        } catch (Throwable t) {
            LOGGER.error("Unknown error", t);
            if (storeConfig.isTimerSkipUnknownError()) {
                timerRequest.idempotentRelease(true);
            } else {
                ThreadUtils.sleep(50);
            }
        }
    }



    private void fetchAndPutTimerRequest() throws Exception {
        long tmpCommitQueueOffset = timerState.currQueueOffset;
        List<TimerRequest> timerRequests = this.fetchTimerRequests();
        if (CollectionUtils.isEmpty(timerRequests)) {

            timerState.commitQueueOffset = tmpCommitQueueOffset;
            timerState.maybeMoveWriteTime();
            return;
        }

        while (!isStopped()) {
            CountDownLatch latch = new CountDownLatch(timerRequests.size());
            for (TimerRequest req : timerRequests) {
                req.setLatch(latch);
                this.putMessageToTimerWheel(req);
            }
            timerState.checkDeliverQueueLatch(latch, fetchedTimerMessageQueue, timerMessageDelivers, timerMessageQueries, -1);
            boolean allSuccess = timerRequests.stream().allMatch(TimerRequest::isSuccess);
            if (allSuccess) {
                break;
            } else {
                ThreadUtils.sleep(50);
            }
        }
        timerState.commitQueueOffset = timerRequests.get(timerRequests.size() - 1).getMsg().getQueueOffset();
        timerState.maybeMoveWriteTime();
    }


}

