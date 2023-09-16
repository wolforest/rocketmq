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

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.metrics.DefaultStoreMetricsManager;
import org.apache.rocketmq.store.timer.Slot;
import org.apache.rocketmq.store.timer.TimerLog;
import org.apache.rocketmq.store.timer.TimerRequest;
import org.apache.rocketmq.store.timer.TimerState;
import org.apache.rocketmq.store.timer.TimerWheel;
import org.apache.rocketmq.store.util.PerfCounter;

import static org.apache.rocketmq.store.timer.TimerMessageStore.ENQUEUE_PUT;

public class TimerWheelLocator extends ServiceThread {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private MessageStoreConfig storeConfig;
    private TimerState timerState;
    private TimerLog timerLog;
    private TimerWheel timerWheel;
    private TimerMetricManager metricManager;

    public TimerWheelLocator(MessageStoreConfig storeConfig,
                             TimerWheel timerWheel,
                             TimerLog timerLog,
                             TimerState timerState,
                             TimerMetricManager metricManager,
                             BlockingQueue<TimerRequest> fetchedTimerMessageQueue, BlockingQueue<TimerRequest> timerMessageDeliverQueue,
                             TimerMessageDeliver[] timerMessageDelivers, TimerMessageQuery[] timerMessageQueries,
                             TimerState pointer, PerfCounter.Ticks perfCounterTicks) {
        this.storeConfig = storeConfig;
        this.timerWheel = timerWheel;
        this.timerLog = timerLog;
        this.timerState = timerState;
        this.metricManager = metricManager;
        this.fetchedTimerMessageQueue = fetchedTimerMessageQueue;
        this.timerMessageDeliverQueue = timerMessageDeliverQueue;
        this.timerMessageDelivers = timerMessageDelivers;
        this.timerMessageQueries = timerMessageQueries;
        this.pointer = pointer;
        this.perfCounterTicks = perfCounterTicks;
    }

    private BlockingQueue<TimerRequest> fetchedTimerMessageQueue;
    private BlockingQueue<TimerRequest> timerMessageDeliverQueue;
    private TimerMessageDeliver[] timerMessageDelivers;
    private TimerMessageQuery[] timerMessageQueries;
    private TimerState pointer;
    private PerfCounter.Ticks perfCounterTicks;


    @Override
    public String getServiceName() {
        return timerState.getServiceThreadName() + this.getClass().getSimpleName();
    }

    /**
     * collect the requests
     */
    protected List<TimerRequest> fetchTimerRequests() throws InterruptedException {
        List<TimerRequest> trs = null;
        TimerRequest firstReq = fetchedTimerMessageQueue.poll(10, TimeUnit.MILLISECONDS);
        if (null != firstReq) {
            trs = new ArrayList<>(16);
            trs.add(firstReq);
            while (true) {
                TimerRequest tmpReq = fetchedTimerMessageQueue.poll(3, TimeUnit.MILLISECONDS);
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

    public String getRealTopic(MessageExt msgExt) {
        if (msgExt == null) {
            return null;
        }
        return msgExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC);
    }

    protected void putMessageToTimerWheel(TimerRequest req) {
        try {
            perfCounterTicks.startTick(ENQUEUE_PUT);
            DefaultStoreMetricsManager.incTimerEnqueueCount(getRealTopic(req.getMsg()));
            if (pointer.shouldRunningDequeue && req.getDelayTime() < pointer.currWriteTimeMs) {
                timerMessageDeliverQueue.put(req);
            } else {
                boolean doEnqueueRes = doEnqueue(
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
    private final ByteBuffer timerLogBuffer = ByteBuffer.allocate(4 * 1024);

    public boolean doEnqueue(long offsetPy, int sizePy, long delayedTime, MessageExt messageExt) {
        LOGGER.debug("Do enqueue [{}] [{}]", new Timestamp(delayedTime), messageExt);
        //copy the value first, avoid concurrent problem
        long tmpWriteTimeMs = pointer.currWriteTimeMs;
        boolean needRoll = delayedTime - tmpWriteTimeMs >= (long) pointer.timerRollWindowSlots * pointer.precisionMs;
        int magic = TimerState.MAGIC_DEFAULT;
        if (needRoll) {
            magic = magic | TimerState.MAGIC_ROLL;
            if (delayedTime - tmpWriteTimeMs - (long) pointer.timerRollWindowSlots * pointer.precisionMs < (long) pointer.timerRollWindowSlots / 3 * pointer.precisionMs) {
                //give enough time to next roll
                delayedTime = tmpWriteTimeMs + (long) (pointer.timerRollWindowSlots / 2) * pointer.precisionMs;
            } else {
                delayedTime = tmpWriteTimeMs + (long) pointer.timerRollWindowSlots * pointer.precisionMs;
            }
        }
        boolean isDelete = messageExt.getProperty(TimerState.TIMER_DELETE_UNIQUE_KEY) != null;
        if (isDelete) {
            magic = magic | TimerState.MAGIC_DELETE;
        }
        String realTopic = messageExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC);
        Slot slot = timerWheel.getSlot(delayedTime);
        ByteBuffer tmpBuffer = timerLogBuffer;
        tmpBuffer.clear();
        tmpBuffer.putInt(TimerLog.UNIT_SIZE); //size
        tmpBuffer.putLong(slot.lastPos); //prev pos
        tmpBuffer.putInt(magic); //magic
        tmpBuffer.putLong(tmpWriteTimeMs); //currWriteTime
        tmpBuffer.putInt((int) (delayedTime - tmpWriteTimeMs)); //delayTime
        tmpBuffer.putLong(offsetPy); //offset
        tmpBuffer.putInt(sizePy); //size
        tmpBuffer.putInt(metricManager.hashTopicForMetrics(realTopic)); //hashcode of real topic
        tmpBuffer.putLong(0); //reserved value, just set to 0 now
        long ret = timerLog.append(tmpBuffer.array(), 0, TimerLog.UNIT_SIZE);
        if (-1 != ret) {
            // If it's a delete message, then slot's total num -1
            // TODO: check if the delete msg is in the same slot with "the msg to be deleted".
            timerWheel.putSlot(delayedTime, slot.firstPos == -1 ? ret : slot.firstPos, ret,
                    isDelete ? slot.num - 1 : slot.num + 1, slot.magic);
            metricManager.addMetric(messageExt, isDelete ? -1 : 1);
        }
        return -1 != ret;
    }



    protected void fetchAndPutTimerRequest() throws Exception {
        long tmpCommitQueueOffset = pointer.currQueueOffset;
        List<TimerRequest> trs = this.fetchTimerRequests();
        if (CollectionUtils.isEmpty(trs)) {
            pointer.commitQueueOffset = tmpCommitQueueOffset;
            pointer.maybeMoveWriteTime();
            return;
        }

        while (!isStopped()) {
            CountDownLatch latch = new CountDownLatch(trs.size());
            for (TimerRequest req : trs) {
                req.setLatch(latch);
                this.putMessageToTimerWheel(req);
            }
            pointer.checkDeliverQueueLatch(latch, fetchedTimerMessageQueue,timerMessageDelivers,timerMessageQueries, -1);
            boolean allSuccess = trs.stream().allMatch(TimerRequest::isSucc);
            if (allSuccess) {
                break;
            } else {
                ThreadUtils.sleep(50);
            }
        }
        pointer.commitQueueOffset = trs.get(trs.size() - 1).getMsg().getQueueOffset();
        pointer.maybeMoveWriteTime();
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
}

