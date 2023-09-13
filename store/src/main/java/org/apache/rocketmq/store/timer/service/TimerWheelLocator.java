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


import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.metrics.DefaultStoreMetricsManager;
import org.apache.rocketmq.store.timer.TimerState;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.timer.TimerRequest;
import org.apache.rocketmq.store.util.PerfCounter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.rocketmq.store.timer.TimerMessageStore.ENQUEUE_PUT;

public class TimerWheelLocator extends ServiceThread {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private TimerMessageStore timerMessageStore;
    private BlockingQueue<TimerRequest> enqueuePutQueue;
    private BlockingQueue<List<TimerRequest>> dequeueGetQueue;
    private BlockingQueue<TimerRequest> dequeuePutQueue;
    private PerfCounter.Ticks perfCounterTicks;
    private TimerState pointer;
    private MessageStoreConfig storeConfig;
    public TimerWheelLocator(TimerMessageStore timerMessageStore) {
        this.timerMessageStore = timerMessageStore;
        enqueuePutQueue = timerMessageStore.getFetchedTimerMessageQueue();
        dequeueGetQueue = timerMessageStore.getDequeueGetQueue();
        dequeuePutQueue = timerMessageStore.getDequeuePutQueue();
        perfCounterTicks = timerMessageStore.getPerfCounterTicks();
        pointer = timerMessageStore.getPointer();
        storeConfig = timerMessageStore.getMessageStore().getMessageStoreConfig();
    }

    @Override
    public String getServiceName() {
        return timerMessageStore.getServiceThreadName() + this.getClass().getSimpleName();
    }

    /**
     * collect the requests
     */
    protected List<TimerRequest> fetchTimerRequests() throws InterruptedException {
        List<TimerRequest> trs = null;
        TimerRequest firstReq = enqueuePutQueue.poll(10, TimeUnit.MILLISECONDS);
        if (null != firstReq) {
            trs = new ArrayList<>(16);
            trs.add(firstReq);
            while (true) {
                TimerRequest tmpReq = enqueuePutQueue.poll(3, TimeUnit.MILLISECONDS);
                if (null == tmpReq) {
                    break;
                }
                trs.add(tmpReq);
                if (trs.size() > 10) {
                    break;
                }
            }
        }
        return trs;
    }

    protected void putMessageToTimerWheel(TimerRequest req) {
        try {
            perfCounterTicks.startTick(ENQUEUE_PUT);
            DefaultStoreMetricsManager.incTimerEnqueueCount(timerMessageStore.getRealTopic(req.getMsg()));
            if (pointer.shouldRunningDequeue && req.getDelayTime() < pointer.currWriteTimeMs) {
                dequeuePutQueue.put(req);
            } else {
                boolean doEnqueueRes = timerMessageStore.doEnqueue(
                        req.getOffsetPy(), req.getSizePy(), req.getDelayTime(), req.getMsg());
                req.idempotentRelease(doEnqueueRes || storeConfig.isTimerSkipUnknownError());
            }
            perfCounterTicks.endTick(ENQUEUE_PUT);
        } catch (Throwable t) {
            LOGGER.error("Unknown error", t);
            if (storeConfig.isTimerSkipUnknownError()) {
                req.idempotentRelease(true);
            } else {
                ThreadUtils.sleep(50);
            }
        }
    }

    protected void fetchAndPutTimerRequest() throws Exception {
        long tmpCommitQueueOffset = pointer.currQueueOffset;
        List<TimerRequest> trs = this.fetchTimerRequests();
        if (CollectionUtils.isEmpty(trs)) {
            pointer.commitQueueOffset = tmpCommitQueueOffset;
            timerMessageStore.maybeMoveWriteTime();
            return;
        }

        while (!isStopped()) {
            CountDownLatch latch = new CountDownLatch(trs.size());
            for (TimerRequest req : trs) {
                req.setLatch(latch);
                this.putMessageToTimerWheel(req);
            }
            timerMessageStore.checkDequeueLatch(latch, -1);
            boolean allSuccess = trs.stream().allMatch(TimerRequest::isSucc);
            if (allSuccess) {
                break;
            } else {
                ThreadUtils.sleep(50);
            }
        }
        pointer.commitQueueOffset = trs.get(trs.size() - 1).getMsg().getQueueOffset();
        timerMessageStore.maybeMoveWriteTime();
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service start");
        while (!this.isStopped() || enqueuePutQueue.size() != 0) {
            try {
                fetchAndPutTimerRequest();
            } catch (Throwable e) {
                LOGGER.error("Unknown error", e);
            }
        }
        LOGGER.info(this.getServiceName() + " service end");
    }
}

