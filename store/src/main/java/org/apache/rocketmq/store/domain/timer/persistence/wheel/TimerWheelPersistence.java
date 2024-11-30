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
package org.apache.rocketmq.store.domain.timer.persistence.wheel;

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.domain.timer.metrics.TimerMetricManager;
import org.apache.rocketmq.store.domain.timer.model.TimerRequest;
import org.apache.rocketmq.store.domain.timer.model.TimerState;
import org.apache.rocketmq.store.domain.timer.persistence.Persistence;
import org.apache.rocketmq.store.domain.timer.persistence.ScanResult;
import org.apache.rocketmq.store.infra.mappedfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.server.metrics.PerfCounter;

public class TimerWheelPersistence implements Persistence {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final TimerState timerState;
    private final TimerWheel timerWheel;
    private final TimerLog timerLog;
    private final TimerMetricManager metricManager;
    private final MessageStoreConfig storeConfig;
    private final PerfCounter.Ticks perfCounterTicks;

    private final int precisionMs;
    private final int timerLogFileSize;

    public TimerWheelPersistence(TimerState timerState, TimerWheel timerWheel, TimerLog timerLog, MessageStoreConfig storeConfig, TimerMetricManager metricManager, PerfCounter.Ticks perfCounterTicks) {
        this.timerState = timerState;
        this.timerWheel = timerWheel;
        this.timerLog = timerLog;
        this.metricManager = metricManager;
        this.storeConfig = storeConfig;
        this.perfCounterTicks = perfCounterTicks;

        this.precisionMs = storeConfig.getTimerPrecisionMs();
        this.timerLogFileSize = storeConfig.getMappedFileSizeTimerLog();
    }

    @Override
    public boolean save(TimerRequest timerRequest) {
        long delayedTime = timerRequest.getDelayTime();
        MessageExt messageExt = timerRequest.getMsg();
        LOGGER.debug("Do enqueue [{}] [{}]", new Timestamp(delayedTime), messageExt);

        //copy the value first, avoid concurrent problem
        long tmpWriteTimeMs = timerState.currWriteTimeMs;
        boolean needRoll = delayedTime - tmpWriteTimeMs >= (long) timerState.timerRollWindowSlots * timerState.precisionMs;
        int magic = TimerState.MAGIC_DEFAULT;

        if (needRoll) {
            magic = magic | TimerState.MAGIC_ROLL;
            if (delayedTime - tmpWriteTimeMs - (long) timerState.timerRollWindowSlots * timerState.precisionMs < (long) timerState.timerRollWindowSlots / 3 * timerState.precisionMs) {
                //give enough time to next roll
                delayedTime = tmpWriteTimeMs + (long) (timerState.timerRollWindowSlots / 2) * timerState.precisionMs;
            } else {
                delayedTime = tmpWriteTimeMs + (long) timerState.timerRollWindowSlots * timerState.precisionMs;
            }
        }

        boolean isDelete = messageExt.getProperty(TimerState.TIMER_DELETE_UNIQUE_KEY) != null;
        if (isDelete) {
            magic = magic | TimerState.MAGIC_DELETE;
        }

        String realTopic = messageExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC);
        Slot slot = timerWheel.getSlot(delayedTime);
        long ret = appendTimerLog(timerRequest.getCommitLogOffset(), timerRequest.getMessageSize(), delayedTime, tmpWriteTimeMs, magic, realTopic, slot.lastPos);
        if (-1 != ret) {
            // If it's a delete message, then slot's total num -1
            // TODO: check if the delete msg is in the same slot with "the msg to be deleted".
            timerWheel.putSlot(delayedTime, slot.firstPos == -1 ? ret : slot.firstPos, ret,
                    isDelete ? slot.num - 1 : slot.num + 1, slot.magic);
            metricManager.addMetric(messageExt, isDelete ? -1 : 1);
        }
        return -1 != ret;
    }


    @Override
    public ScanResult scan() {
        ScanResult result = new ScanResult();
        Slot slot = timerWheel.getSlot(timerState.currReadTimeMs);
        if (-1 == slot.timeMs) {
            timerState.moveReadTime(precisionMs);
            return result;
        }
        result.setCode(1);
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
                        result.addDeleteMsgStack(timerRequest);
                    } else {
                        result.addNormalMsgStack(timerRequest);
                    }
                } catch (Exception e) {
                    LOGGER.error("Error in dequeue_read_timerlog", e);
                } finally {
                    currOffsetPy = prevPos;
                    perfCounterTicks.endTick("dequeue_read_timerlog");
                }
            }
            if (result.sizeOfDeleteMsgStack() == 0 && result.sizeOfNormalMsgStack() == 0) {
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

    private long appendTimerLog(long commitLogOffset, int messageSize, long delayedTime, long tmpWriteTimeMs, int magic, String realTopic, long lastPos) {
        Block block = new Block(
                Block.SIZE,
                lastPos,
                magic,
                tmpWriteTimeMs,
                (int) (delayedTime - tmpWriteTimeMs),
                commitLogOffset,
                messageSize,
                metricManager.hashTopicForMetrics(realTopic),
                0);

        return timerLog.append(block, 0, Block.SIZE);
    }

}
