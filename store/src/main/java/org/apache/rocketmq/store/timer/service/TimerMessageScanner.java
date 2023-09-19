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
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.timer.Slot;
import org.apache.rocketmq.store.timer.TimerLog;
import org.apache.rocketmq.store.timer.TimerRequest;
import org.apache.rocketmq.store.timer.TimerState;
import org.apache.rocketmq.store.timer.TimerWheel;
import org.apache.rocketmq.store.util.PerfCounter;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;

public class TimerMessageScanner extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private MessageStoreConfig storeConfig;
    private TimerState timerState;
    private TimerWheel timerWheel;
    private TimerLog timerLog;
    private PerfCounter.Ticks perfCounterTicks;

    private int commitLogFileSize;

    private BlockingQueue<List<TimerRequest>> timerMessageQueryQueue;
    private BlockingQueue<TimerRequest> timerMessageDeliverQueue;
    private TimerMessageDeliver[] timerMessageDelivers;
    private TimerMessageQuery[] timerMessageQueries;

    private Scanner scanner;
    private long shouldStartTime;
    private final int timerLogFileSize;
    private final int precisionMs;

    public TimerMessageScanner(TimerState timerState,
                               MessageStoreConfig storeConfig,
                               TimerWheel timerWheel,
                               TimerLog timerLog,
                               BlockingQueue<List<TimerRequest>> timerMessageQueryQueue,
                               BlockingQueue<TimerRequest> timerMessageDeliverQueue,
                               TimerMessageDeliver[] timerMessageDelivers,
                               TimerMessageQuery[] timerMessageQueries,
                               PerfCounter.Ticks perfCounterTicks) {
        this.timerState = timerState;
        this.storeConfig = storeConfig;
        this.timerWheel = timerWheel;
        this.timerLog = timerLog;
        this.timerMessageQueryQueue = timerMessageQueryQueue;
        this.timerMessageDeliverQueue = timerMessageDeliverQueue;
        this.timerMessageDelivers = timerMessageDelivers;
        this.timerMessageQueries = timerMessageQueries;
        this.perfCounterTicks = perfCounterTicks;
        this.scanner = new TimerWheelScanner(timerState, timerWheel, timerLog, storeConfig, perfCounterTicks);
        timerLogFileSize = storeConfig.getMappedFileSizeTimerLog();
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
        LinkedList<TimerRequest> normalMsgStack = new LinkedList<>();
        LinkedList<TimerRequest> deleteMsgStack = new LinkedList<>();


        Scanner.ScannResult result = scanner.scan();
        if (result.getCode() == 0) return result.getCode();



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

    private Scanner.ScannResult doScan(LinkedList<TimerRequest> normalMsgStack, LinkedList<TimerRequest> deleteMsgStack) {
        Scanner.ScannResult result = new Scanner.ScannResult();
        Slot slot = timerWheel.getSlot(timerState.currReadTimeMs);
        if (-1 == slot.timeMs) {
            timerState.moveReadTime(precisionMs);
            return result;
        }
        result.code = 1;
        try {
            //clear the flag
            timerState.dequeueStatusChangeFlag = false;

            long currOffsetPy = slot.lastPos;
            Set<String> deleteUniqKeys = new ConcurrentSkipListSet<>();
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
                    if (timerState.needDelete(magic) && !timerState.needRoll(magic)) {
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
                LOGGER.warn("dequeue time:{} but read nothing from timerLog", timerState.currReadTimeMs);
            }
            for (SelectMappedBufferResult sbr : sbrs) {
                if (null != sbr) {
                    sbr.release();
                }
            }
        } catch (Throwable t) {
            LOGGER.error("Unknown error in dequeue process", t);
            if (storeConfig.isTimerSkipUnknownError()) {
                timerState.moveReadTime(precisionMs);
            }
        }
        return result;
    }

    /**
     * Put timerRequests to TimerQueryQueue
     *
     * @param msgStack
     * @throws Exception
     */
    private void putToQuery(LinkedList<TimerRequest> msgStack) throws Exception {
        List<List<TimerRequest>> timerRequestListGroup = splitIntoLists(msgStack);
        CountDownLatch countDownLatch = new CountDownLatch(msgStack.size());
        //read the delete msg: the msg used to mark another msg is deleted
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

