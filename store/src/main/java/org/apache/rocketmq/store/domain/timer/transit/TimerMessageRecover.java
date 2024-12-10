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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.domain.timer.persistence.TimerCheckpoint;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerLog;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerWheel;
import org.apache.rocketmq.store.infra.mappedfile.MappedFile;
import org.apache.rocketmq.store.infra.mappedfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.domain.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.domain.queue.CqUnit;
import org.apache.rocketmq.store.domain.queue.ReferredIterator;
import org.apache.rocketmq.store.domain.timer.model.TimerState;

import static org.apache.rocketmq.store.domain.timer.model.TimerState.TIMER_TOPIC;

/**
 *
 */
public class TimerMessageRecover {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final TimerState timerState;
    private final TimerWheel timerWheel;
    private final TimerLog timerLog;
    private final MessageOperator messageOperator;
    private final TimerCheckpoint timerCheckpoint;

    private final boolean debug = false;
    private final int precisionMs;

    public TimerMessageRecover(TimerState timerState,
                               TimerWheel timerWheel,
                               TimerLog timerLog, MessageOperator messageOperator,
                               TimerCheckpoint timerCheckpoint) {
        this.timerState = timerState;
        this.timerWheel = timerWheel;
        this.timerLog = timerLog;
        this.messageOperator = messageOperator;
        this.timerCheckpoint = timerCheckpoint;
        precisionMs = timerState.precisionMs;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public void recover() {
        //recover timerLog
        long lastFlushPos = timerCheckpoint.getLastTimerLogFlushPos();
        MappedFile lastFile = timerLog.getMappedFileQueue().getLastMappedFile();
        if (null != lastFile) {
            lastFlushPos = lastFlushPos - lastFile.getFileSize();
        }
        if (lastFlushPos < 0) {
            lastFlushPos = 0;
        }
        long processOffset = recoverAndRevise(lastFlushPos, true);

        timerLog.getMappedFileQueue().setFlushedPosition(processOffset);
        //revise queue offset
        long queueOffset = reviseQueueOffset(processOffset);
        if (-1 == queueOffset) {
            timerState.currQueueOffset = timerCheckpoint.getLastTimerQueueOffset();
        } else {
            timerState.currQueueOffset = queueOffset + 1;
        }
        timerState.currQueueOffset = Math.min(timerState.currQueueOffset, timerCheckpoint.getMasterTimerQueueOffset());

        //check timer wheel
        timerState.currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
        long nextReadTimeMs = formatTimeMs(
                System.currentTimeMillis()) - (long) timerState.totalSlots * precisionMs + (long) TimerState.TIMER_BLANK_SLOTS * precisionMs;
        if (timerState.currReadTimeMs < nextReadTimeMs) {
            timerState.currReadTimeMs = nextReadTimeMs;
        }
        //the timer wheel may contain physical offset bigger than timerLog
        //This will only happen when the timerLog is damaged
        //hard to test
        long minFirst = timerWheel.checkPhyPos(timerState.currReadTimeMs, processOffset);
        if (debug) {
            minFirst = 0;
        }
        if (minFirst < processOffset) {
            LOGGER.warn("Timer recheck because of minFirst:{} processOffset:{}", minFirst, processOffset);
            recoverAndRevise(minFirst, false);
        }
        LOGGER.info("Timer recover ok currReadTimerMs:{} currQueueOffset:{} checkQueueOffset:{} processOffset:{}",
                timerState.currReadTimeMs, timerState.currQueueOffset, timerCheckpoint.getLastTimerQueueOffset(), processOffset);

        timerState.commitReadTimeMs = timerState.currReadTimeMs;
        timerState.commitQueueOffset = timerState.currQueueOffset;

        timerState.prepareTimerCheckPoint();
    }

    /**
     * recover timerLog and revise timerWheel
     *
     * @param beginOffset lastFlushPosition - lastMappedFile.fileSize
     * @param checkTimerLog true | false
     * @return process offset
     */
    private long recoverAndRevise(long beginOffset, boolean checkTimerLog) {
        LOGGER.info("Begin to recover timerLog offset:{} check:{}", beginOffset, checkTimerLog);
        MappedFile lastFile = timerLog.getMappedFileQueue().getLastMappedFile();
        if (null == lastFile) {
            return 0;
        }

        List<MappedFile> mappedFiles = timerLog.getMappedFileQueue().getMappedFiles();
        int index = mappedFiles.size() - 1;
        for (; index >= 0; index--) {
            MappedFile mappedFile = mappedFiles.get(index);
            if (beginOffset >= mappedFile.getOffsetInFileName()) {
                break;
            }
        }
        if (index < 0) {
            index = 0;
        }

        return recoverTimerLogFiles(mappedFiles, checkTimerLog, index);
    }

    private long recoverTimerLogFiles(List<MappedFile> mappedFiles, boolean checkTimerLog, int index) {
        long checkOffset = mappedFiles.get(index).getOffsetInFileName();
        for (; index < mappedFiles.size(); index++) {
            MappedFile mappedFile = mappedFiles.get(index);
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0, checkTimerLog ? mappedFiles.get(index).getFileSize() : mappedFile.getWroteOrCommitPosition());
            ByteBuffer bf = sbr.getByteBuffer();
            int position = 0;
            boolean stopCheck = false;
            for (; position < sbr.getSize(); position += TimerLog.UNIT_SIZE) {
                try {
                    bf.position(position);
                    int size = bf.getInt();//size
                    bf.getLong();//prev pos
                    int magic = bf.getInt();
                    if (magic == TimerLog.BLANK_MAGIC_CODE) {
                        break;
                    }
                    if (checkTimerLog && (!TimerState.isMagicOK(magic) || TimerLog.UNIT_SIZE != size)) {
                        stopCheck = true;
                        break;
                    }
                    long delayTime = bf.getLong() + bf.getInt();
                    if (TimerLog.UNIT_SIZE == size && TimerState.isMagicOK(magic)) {
                        timerWheel.reviseSlot(delayTime, TimerWheel.IGNORE, sbr.getStartOffset() + position, true);
                    }
                } catch (Exception e) {
                    LOGGER.error("Recover timerLog error", e);
                    stopCheck = true;
                    break;
                }
            }
            sbr.release();
            checkOffset = mappedFiles.get(index).getOffsetInFileName() + position;
            if (stopCheck) {
                break;
            }
        }
        if (checkTimerLog) {
            timerLog.getMappedFileQueue().truncateDirtyFiles(checkOffset);
        }
        return checkOffset;
    }

    private long reviseQueueOffset(long processOffset) {
        SelectMappedBufferResult selectRes = timerLog.getTimerMessage(processOffset - (TimerLog.UNIT_SIZE - TimerLog.UNIT_PRE_SIZE_FOR_MSG));
        if (null == selectRes) {
            return -1;
        }
        try {
            // message offset and message size
            long offsetPy = selectRes.getByteBuffer().getLong();
            int sizePy = selectRes.getByteBuffer().getInt();
            MessageExt messageExt = messageOperator.readMessageByCommitOffset(offsetPy, sizePy);
            if (null == messageExt) {
                return -1;
            }

            // check offset in msg is equal to offset of cq.
            // if not, use cq offset.
            long msgQueueOffset = messageExt.getQueueOffset();
            int queueId = messageExt.getQueueId();
            ConsumeQueueInterface cq = messageOperator.getConsumeQueue(TIMER_TOPIC, queueId);
            if (null == cq) {
                return msgQueueOffset;
            }

            return reviseByConsumeQueue(cq, msgQueueOffset, offsetPy, sizePy);
        } finally {
            selectRes.release();
        }
    }

    private long reviseByConsumeQueue(ConsumeQueueInterface cq, long msgQueueOffset, long offsetPy, int sizePy) {
        long cqOffset = msgQueueOffset;
        long tmpOffset = msgQueueOffset;
        int maxCount = 20000;
        while (maxCount-- > 0) {
            if (tmpOffset < 0) {
                LOGGER.warn("reviseQueueOffset check cq offset fail, msg in cq is not found.{}, {}",
                    offsetPy, sizePy);
                break;
            }
            ReferredIterator<CqUnit> iterator = null;
            try {
                iterator = cq.iterateFrom(tmpOffset);
                CqUnit cqUnit = null;
                if (null == iterator || (cqUnit = iterator.next()) == null) {
                    // offset in msg may be greater than offset of cq.
                    tmpOffset -= 1;
                    continue;
                }

                long offsetPyTemp = cqUnit.getPos();
                int sizePyTemp = cqUnit.getSize();
                if (offsetPyTemp == offsetPy && sizePyTemp == sizePy) {
                    LOGGER.info("reviseQueueOffset check cq offset ok. {}, {}, {}",
                        tmpOffset, offsetPyTemp, sizePyTemp);
                    cqOffset = tmpOffset;
                    break;
                }
                tmpOffset -= 1;
            } catch (Throwable e) {
                LOGGER.error("reviseQueueOffset check cq offset error.", e);
            } finally {
                if (iterator != null) {
                    iterator.release();
                }
            }
        }

        return cqOffset;
    }

    private long formatTimeMs(long timeMs) {
        return timeMs / precisionMs * precisionMs;
    }


}
