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


import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.timer.Block;
import org.apache.rocketmq.store.timer.Slot;
import org.apache.rocketmq.store.timer.TimerLog;
import org.apache.rocketmq.store.timer.TimerRequest;
import org.apache.rocketmq.store.timer.TimerState;
import org.apache.rocketmq.store.timer.TimerWheel;

import java.sql.Timestamp;

public class TimerWheelLocator implements Locator {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private TimerState timerState;
    private TimerWheel timerWheel;
    private TimerLog timerLog;
    private TimerMetricManager metricManager;

    public TimerWheelLocator(TimerState timerState, TimerWheel timerWheel, TimerLog timerLog, TimerMetricManager metricManager) {
        this.timerState = timerState;
        this.timerWheel = timerWheel;
        this.timerLog = timerLog;
        this.metricManager = metricManager;
    }

    @Override
    public boolean save(TimerRequest timerRequest) {
        long offsetPy = timerRequest.getOffsetPy();
        int sizePy = timerRequest.getSizePy();
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
        long ret = appendTimerLog(offsetPy, sizePy, delayedTime, tmpWriteTimeMs, magic, realTopic, slot.lastPos);
        if (-1 != ret) {
            // If it's a delete message, then slot's total num -1
            // TODO: check if the delete msg is in the same slot with "the msg to be deleted".
            timerWheel.putSlot(delayedTime, slot.firstPos == -1 ? ret : slot.firstPos, ret,
                    isDelete ? slot.num - 1 : slot.num + 1, slot.magic);
            metricManager.addMetric(messageExt, isDelete ? -1 : 1);
        }
        return -1 != ret;
    }

    private long appendTimerLog(long offsetPy, int sizePy, long delayedTime, long tmpWriteTimeMs, int magic, String realTopic, long lastPos) {
        Block block = new Block(
                Block.SIZE,
                lastPos,
                magic,
                tmpWriteTimeMs,
                (int) (delayedTime - tmpWriteTimeMs),
                offsetPy,
                sizePy,
                metricManager.hashTopicForMetrics(realTopic),
                0);

        long ret = timerLog.append(block, 0, Block.SIZE);
        return ret;
    }


}
