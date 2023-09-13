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
import org.apache.rocketmq.store.logfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.timer.Slot;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.timer.TimerRequest;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;

public class TimerWheelFetcher extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private TimerMessageStore timerMessageStore;
    private long shouldStartTime;

    public TimerWheelFetcher(TimerMessageStore timerMessageStore) {
        this.timerMessageStore = timerMessageStore;
    }

    @Override
    public String getServiceName() {
        return timerMessageStore.getServiceThreadName() + this.getClass().getSimpleName();
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
                    waitForRunning(100L * timerMessageStore.getPrecisionMs() / 1000);
                }
            } catch (Throwable e) {
                LOGGER.error("Error occurred in " + getServiceName(), e);
            }
        }
        LOGGER.info(this.getServiceName() + " service end");
    }

    public int dequeue() throws Exception {
        if (storeConfig.isTimerStopDequeue()) {
            return -1;
        }
        if (!isRunningDequeue()) {
            return -1;
        }
        if (pointer.currReadTimeMs >= pointer.currWriteTimeMs) {
            return -1;
        }

        Slot slot = timerWheel.getSlot(pointer.currReadTimeMs);
        if (-1 == slot.timeMs) {
            moveReadTime();
            return 0;
        }
        try {
            //clear the flag
            dequeueStatusChangeFlag = false;

            long currOffsetPy = slot.lastPos;
            Set<String> deleteUniqKeys = new ConcurrentSkipListSet<>();
            LinkedList<TimerRequest> normalMsgStack = new LinkedList<>();
            LinkedList<TimerRequest> deleteMsgStack = new LinkedList<>();
            LinkedList<SelectMappedBufferResult> sbrs = new LinkedList<>();
            SelectMappedBufferResult timeSbr = null;
            //read the timer log one by one
            while (currOffsetPy != -1) {
                perfCounterTicks.startTick("dequeue_read_timerlog");
                if (null == timeSbr || timeSbr.getStartOffset() > currOffsetPy) {
                    timeSbr = timerLog.getWholeBuffer(currOffsetPy);
                    if (null != timeSbr) {
                        sbrs.add(timeSbr);
                    }
                }
                if (null == timeSbr) {
                    break;
                }
                long prevPos = -1;
                try {
                    int position = (int) (currOffsetPy % timerLogFileSize);
                    timeSbr.getByteBuffer().position(position);
                    timeSbr.getByteBuffer().getInt(); //size
                    prevPos = timeSbr.getByteBuffer().getLong();
                    int magic = timeSbr.getByteBuffer().getInt();
                    long enqueueTime = timeSbr.getByteBuffer().getLong();
                    long delayedTime = timeSbr.getByteBuffer().getInt() + enqueueTime;
                    long offsetPy = timeSbr.getByteBuffer().getLong();
                    int sizePy = timeSbr.getByteBuffer().getInt();
                    TimerRequest timerRequest = new TimerRequest(offsetPy, sizePy, delayedTime, enqueueTime, magic);
                    timerRequest.setDeleteList(deleteUniqKeys);
                    if (needDelete(magic) && !needRoll(magic)) {
                        deleteMsgStack.add(timerRequest);
                    } else {
                        normalMsgStack.addFirst(timerRequest);
                    }
                } catch (Exception e) {
                    LOGGER.error("Error in dequeue_read_timerlog", e);
                } finally {
                    currOffsetPy = prevPos;
                    perfCounterTicks.endTick("dequeue_read_timerlog");
                }
            }
            if (deleteMsgStack.size() == 0 && normalMsgStack.size() == 0) {
                LOGGER.warn("dequeue time:{} but read nothing from timerLog", pointer.currReadTimeMs);
            }
            for (SelectMappedBufferResult sbr : sbrs) {
                if (null != sbr) {
                    sbr.release();
                }
            }
            if (!isRunningDequeue()) {
                return -1;
            }
            CountDownLatch deleteLatch = new CountDownLatch(deleteMsgStack.size());
            //read the delete msg: the msg used to mark another msg is deleted
            for (List<TimerRequest> deleteList : splitIntoLists(deleteMsgStack)) {
                for (TimerRequest tr : deleteList) {
                    tr.setLatch(deleteLatch);
                }
                dequeueGetQueue.put(deleteList);
            }
            //do we need to use loop with tryAcquire
            checkDequeueLatch(deleteLatch, pointer.currReadTimeMs);

            CountDownLatch normalLatch = new CountDownLatch(normalMsgStack.size());
            //read the normal msg
            for (List<TimerRequest> normalList : splitIntoLists(normalMsgStack)) {
                for (TimerRequest tr : normalList) {
                    tr.setLatch(normalLatch);
                }
                dequeueGetQueue.put(normalList);
            }
            checkDequeueLatch(normalLatch, pointer.currReadTimeMs);
            // if master -> slave -> master, then the read time move forward, and messages will be lossed
            if (dequeueStatusChangeFlag) {
                return -1;
            }
            if (!isRunningDequeue()) {
                return -1;
            }
            moveReadTime();
        } catch (Throwable t) {
            LOGGER.error("Unknown error in dequeue process", t);
            if (storeConfig.isTimerSkipUnknownError()) {
                moveReadTime();
            }
        }
        return 1;
    }
}

