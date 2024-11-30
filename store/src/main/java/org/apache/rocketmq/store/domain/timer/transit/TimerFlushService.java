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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.apache.rocketmq.common.lang.thread.ServiceThread;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerLog;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerWheel;
import org.apache.rocketmq.store.domain.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.domain.timer.metrics.TimerMetrics;
import org.apache.rocketmq.store.domain.timer.model.TimerRequest;
import org.apache.rocketmq.store.domain.timer.model.TimerState;

import static org.apache.rocketmq.store.domain.timer.model.TimerState.TIMER_TOPIC;

/**
 * periodically flush timerWheel thread
 */
public class TimerFlushService extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final SimpleDateFormat sdf = new SimpleDateFormat("MM-dd HH:mm:ss");
    private final MessageStore messageStore;
    private final BlockingQueue<TimerRequest> fetchedTimerMessageQueue;
    private final BlockingQueue<List<TimerRequest>> timerMessageQueryQueue;
    private final BlockingQueue<TimerRequest> timerMessageDeliverQueue;
    private final MessageStoreConfig storeConfig;
    private final TimerState timerState;
    private final TimerMetrics timerMetrics;
    private final TimerLog timerLog;
    private final TimerWheel timerWheel;

    public TimerFlushService(TimerState timerState,
                             MessageStoreConfig storeConfig,
                             MessageStore messageStore,
                             TimerWheel timerWheel,
                             TimerLog timerLog,
                             BlockingQueue<TimerRequest> fetchedTimerMessageQueue,
                             BlockingQueue<List<TimerRequest>> timerMessageQueryQueue,
                             BlockingQueue<TimerRequest> timerMessageDeliverQueue,
                             TimerMetrics timerMetrics

    ) {
        this.messageStore = messageStore;
        this.fetchedTimerMessageQueue = fetchedTimerMessageQueue;
        this.timerMessageQueryQueue = timerMessageQueryQueue;
        this.timerMessageDeliverQueue = timerMessageDeliverQueue;
        this.storeConfig = storeConfig;
        this.timerState = timerState;
        this.timerMetrics = timerMetrics;
        this.timerLog = timerLog;
        this.timerWheel = timerWheel;
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service start");
        long start = System.currentTimeMillis();

        while (!this.isStopped()) {
            start = flushTimerWheel(start);
        }
        LOGGER.info(this.getServiceName() + " service end");
    }

    private long flushTimerWheel(long start) {
        try {
            timerState.prepareTimerCheckPoint();
            timerLog.getMappedFileQueue().flush(0);
            timerWheel.flush();
            timerState.flushCheckpoint();
            if (System.currentTimeMillis() - start > storeConfig.getTimerProgressLogIntervalMs()) {
                start = System.currentTimeMillis();
                long tmpQueueOffset = timerState.currQueueOffset;
                ConsumeQueueInterface cq = messageStore.getConsumeQueue(TIMER_TOPIC, 0);
                long maxOffsetInQueue = cq == null ? 0 : cq.getMaxOffsetInQueue();
                LOGGER.info("[{}]Timer progress-check commitRead:[{}] currRead:[{}] currWrite:[{}] readBehind:{} currReadOffset:{} offsetBehind:{} behindMaster:{} " +
                        "enqPutQueue:{} deqGetQueue:{} deqPutQueue:{} allCongestNum:{} enqExpiredStoreTime:{}",
                    storeConfig.getBrokerRole(),
                    format(timerState.commitReadTimeMs), format(timerState.currReadTimeMs), format(timerState.currWriteTimeMs), timerState.getDequeueBehind(),
                    tmpQueueOffset, maxOffsetInQueue - tmpQueueOffset, timerState.getMasterTimerQueueOffset() - tmpQueueOffset,
                    fetchedTimerMessageQueue.size(), timerMessageQueryQueue.size(), timerMessageDeliverQueue.size(), timerState.getAllCongestNum(), format(timerState.lastEnqueueButExpiredStoreTime));
            }
            timerMetrics.persist();
            waitForRunning(storeConfig.getTimerFlushIntervalMs());
        } catch (Throwable e) {
            LOGGER.error("Error occurred in " + getServiceName(), e);
        }

        return start;
    }

    private String format(long time) {
        return sdf.format(new Date(time));
    }

    @Override
    public String getServiceName() {
        return timerState.getServiceThreadName() + this.getClass().getSimpleName();
    }

}


