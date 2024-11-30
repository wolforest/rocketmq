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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.lang.thread.ServiceThread;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.domain.timer.metrics.TimerMetricManager;
import org.apache.rocketmq.store.domain.timer.model.TimerRequest;
import org.apache.rocketmq.store.domain.timer.model.TimerState;
import org.apache.rocketmq.store.domain.timer.persistence.Persistence;
import org.apache.rocketmq.store.domain.timer.persistence.ScanResult;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerLog;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerWheel;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerWheelPersistence;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.server.metrics.PerfCounter;

/**
 * @renamed from TimerDequeueGetService to TimerMessageScanner
 * scan persistence and put message to:
 *  1. timerMessageQueries
 *  2. timerMessageDeliverQueue -> then timerState.checkDeliverQueueLatch()
 */
public class TimerMessageScanner extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final MessageStoreConfig storeConfig;
    private final TimerState timerState;
    private final int commitLogFileSize;

    private final BlockingQueue<List<TimerRequest>> timerMessageQueryQueue;
    private final BlockingQueue<TimerRequest> timerMessageDeliverQueue;
    private final TimerMessageDeliver[] timerMessageDelivers;
    private final TimerMessageQuerier[] timerMessageQueries;

    private final Persistence persistence;
    private long shouldStartTime;
    private final int precisionMs;

    public TimerMessageScanner(TimerState timerState,
                               MessageStoreConfig storeConfig,
                               TimerWheel timerWheel,
                               TimerLog timerLog,
                               BlockingQueue<List<TimerRequest>> timerMessageQueryQueue,
                               BlockingQueue<TimerRequest> timerMessageDeliverQueue,
                               TimerMessageDeliver[] timerMessageDelivers,
                               TimerMessageQuerier[] timerMessageQueries,
                               TimerMetricManager metricManager,
                               PerfCounter.Ticks perfCounterTicks) {
        this.timerState = timerState;
        this.storeConfig = storeConfig;

        this.timerMessageQueryQueue = timerMessageQueryQueue;
        this.timerMessageDeliverQueue = timerMessageDeliverQueue;
        this.timerMessageDelivers = timerMessageDelivers;
        this.timerMessageQueries = timerMessageQueries;

        this.persistence = new TimerWheelPersistence(timerState,timerWheel,timerLog,storeConfig,metricManager,perfCounterTicks);
        precisionMs = storeConfig.getTimerPrecisionMs();
        commitLogFileSize = storeConfig.getMappedFileSizeCommitLog();
    }

    public void start(long shouldStartTime) {
        this.shouldStartTime = shouldStartTime;
        super.start();
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service start");
        while (!this.isStopped()) {
            try {
                if (System.currentTimeMillis() < shouldStartTime) {
                    LOGGER.info("TimerDequeueGetService ready to run after {}.", shouldStartTime);
                    waitForRunning(1000);
                    continue;
                }
                if (-1 == dequeue()) {
                    waitForRunning(100L * storeConfig.getTimerPrecisionMs() / 1000);
                }
            } catch (Throwable e) {
                LOGGER.error("Error occurred in " + getServiceName(), e);
            }
        }
        LOGGER.info(this.getServiceName() + " service end");
    }


    @Override
    public String getServiceName() {
        return timerState.getServiceThreadName() + this.getClass().getSimpleName();
    }

    private int dequeue() throws Exception {
        if (storeConfig.isTimerStopDequeue()) {
            return -1;
        }
        if (!timerState.isRunningDequeue()) {
            return -1;
        }
        if (timerState.currReadTimeMs >= timerState.currWriteTimeMs) {
            return -1;
        }

        ScanResult result = persistence.scan();
        if (result.getCode() == 0) {
            return result.getCode();
        }

        if (!timerState.isRunningDequeue()) {
            return -1;
        }

        putToQuery(result.getDeleteMsgStack());
        putToQuery(result.getNormalMsgStack());

        // if master -> slave -> master, then the read time move forward, and messages will be lossed
        if (timerState.dequeueStatusChangeFlag) {
            return -1;
        }
        if (!timerState.isRunningDequeue()) {
            return -1;
        }

        timerState.moveReadTime(precisionMs);
        return 1;
    }

    /**
     * Put timerRequests to TimerQueryQueue
     *
     * @param msgStack msg scan from persistence
     * @throws Exception exception
     */
    private void putToQuery(LinkedList<TimerRequest> msgStack) throws Exception {
        List<List<TimerRequest>> timerRequestListGroup = splitIntoLists(msgStack);
        CountDownLatch countDownLatch = new CountDownLatch(msgStack.size());
        //read the deleted msg: the msg used to mark another msg is deleted
        for (List<TimerRequest> timerRequests : timerRequestListGroup) {
            for (TimerRequest timerRequest : timerRequests) {
                timerRequest.setLatch(countDownLatch);
            }
            timerMessageQueryQueue.put(timerRequests);
        }
        //do we need to use loop with tryAcquire
        timerState.checkDeliverQueueLatch(countDownLatch, this.timerMessageDeliverQueue, this.timerMessageDelivers, this.timerMessageQueries, this.timerState.currReadTimeMs);
    }

    private List<List<TimerRequest>> splitIntoLists(List<TimerRequest> origin) {
        //this method assume that the origin is not null;
        List<List<TimerRequest>> lists = new LinkedList<>();
        if (origin.size() < 100) {
            lists.add(origin);
            return lists;
        }
        List<TimerRequest> currList = null;
        int fileIndexPy = -1;
        int msgIndex = 0;
        for (TimerRequest tr : origin) {
            if (fileIndexPy != tr.getOffsetPy() / commitLogFileSize) {
                msgIndex = 0;
                if (null != currList && currList.size() > 0) {
                    lists.add(currList);
                }
                currList = new LinkedList<>();
                currList.add(tr);
                fileIndexPy = (int) (tr.getOffsetPy() / commitLogFileSize);
            } else {
                currList.add(tr);
                if (++msgIndex % 2000 == 0) {
                    lists.add(currList);
                    currList = new ArrayList<>();
                }
            }
        }
        if (null != currList && currList.size() > 0) {
            lists.add(currList);
        }
        return lists;
    }
}

