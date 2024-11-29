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
package org.apache.rocketmq.store.domain.timer;

import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerLog;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerWheel;
import org.apache.rocketmq.store.domain.timer.transit.TimerMessageQuery;
import org.apache.rocketmq.store.domain.timer.transit.TimerMessageDeliver;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TimerState {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public static final String TIMER_TOPIC = TopicValidator.SYSTEM_TOPIC_PREFIX + "wheel_timer";
    public static final int INITIAL = 0, RUNNING = 1, HAULT = 2, SHUTDOWN = 3;
    public static final int MAGIC_DEFAULT = 1;
    public static final int MAGIC_ROLL = 1 << 1;
    public static final int MAGIC_DELETE = 1 << 2;
    public static final int PUT_OK = 0, PUT_NEED_RETRY = 1, PUT_NO_RETRY = 2, PUT_FAILED = -1;

    public static final String TIMER_ENQUEUE_MS = MessageConst.PROPERTY_TIMER_ENQUEUE_MS;
    public static final String TIMER_DEQUEUE_MS = MessageConst.PROPERTY_TIMER_DEQUEUE_MS;
    public static final String TIMER_ROLL_TIMES = MessageConst.PROPERTY_TIMER_ROLL_TIMES;
    public static final String TIMER_DELETE_UNIQUE_KEY = MessageConst.PROPERTY_TIMER_DEL_UNIQKEY;

    public static final int TIMER_BLANK_SLOTS = 60;
    /**
     * last read timestamp of persistence.scan()
     * may be updated by TimerMessageScanner
     */
    public volatile long currReadTimeMs;
    /**
     * last write timestamp of timer message
     * may be updated by TimerMessageDeliver
     */
    public volatile long currWriteTimeMs;
    public volatile long preReadTimeMs;
    public volatile long commitReadTimeMs;
    public volatile long currQueueOffset; //only one queue that is 0
    public volatile long commitQueueOffset;
    public volatile long lastCommitReadTimeMs;
    public volatile long lastCommitQueueOffset;
    public long lastEnqueueButExpiredTime;
    public long lastEnqueueButExpiredStoreTime;

    // True if current store is master or current brokerId is equal to the minimum brokerId of the replica group in slaveActingMaster mode.
    private volatile boolean shouldRunningDequeue;

    //the dequeue is an asynchronous process, use this flag to track if the status has changed
    public boolean dequeueStatusChangeFlag = false;

    private volatile int state = INITIAL;
    public TimerCheckpoint timerCheckpoint;
    private final TimerWheel timerWheel;
    private final TimerLog timerLog;
    private final MessageStore messageStore;
    private final MessageStoreConfig storeConfig;
    public final int precisionMs;
    public final int timerRollWindowSlots;
    public final int slotsTotal;

    public TimerState(TimerCheckpoint timerCheckpoint, MessageStoreConfig storeConfig, TimerLog timerLog, int slotsTotal, TimerWheel timerWheel, MessageStore messageStore) {
        this.timerCheckpoint = timerCheckpoint;
        this.storeConfig = storeConfig;
        this.timerLog = timerLog;
        this.timerWheel = timerWheel;
        this.messageStore = messageStore;
        this.precisionMs = storeConfig.getTimerPrecisionMs();
        // TimerWheel contains the fixed number of slots regardless of precision.
        this.slotsTotal = slotsTotal;
        // timerRollWindow contains the fixed number of slots regardless of precision.
        if (storeConfig.getTimerRollWindowSlot() > slotsTotal - TIMER_BLANK_SLOTS
                || storeConfig.getTimerRollWindowSlot() < 2) {
            this.timerRollWindowSlots = slotsTotal - TIMER_BLANK_SLOTS;
        } else {
            this.timerRollWindowSlots = storeConfig.getTimerRollWindowSlot();
        }
    }

    public void syncLastReadTimeMs() {
        currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
        commitReadTimeMs = currReadTimeMs;
    }

    public boolean isRunningDequeue() {
        if (!shouldRunningDequeue) {
            syncLastReadTimeMs();
            return false;
        }
        return isRunning();
    }

    public long getMasterTimerQueueOffset() {
        return timerCheckpoint.getMasterTimerQueueOffset();
    }

    public void flushCheckpoint() {
        timerCheckpoint.flush();
    }

    public void prepareTimerCheckPoint() {
        timerCheckpoint.setLastTimerLogFlushPos(timerLog.getMappedFileQueue().getFlushedWhere());
        timerCheckpoint.setLastReadTimeMs(commitReadTimeMs);
        if (shouldRunningDequeue) {
            timerCheckpoint.setMasterTimerQueueOffset(commitQueueOffset);
            if (commitReadTimeMs != lastCommitReadTimeMs || commitQueueOffset != lastCommitQueueOffset) {
                timerCheckpoint.updateDateVersion(messageStore.getStateMachineVersion());
                lastCommitReadTimeMs = commitReadTimeMs;
                lastCommitQueueOffset = commitQueueOffset;
            }
        }
        timerCheckpoint.setLastTimerQueueOffset(Math.min(commitQueueOffset, timerCheckpoint.getMasterTimerQueueOffset()));
    }

    public void moveReadTime(int precisionMs) {
        currReadTimeMs = currReadTimeMs + precisionMs;
        commitReadTimeMs = currReadTimeMs;
    }

    public void maybeMoveWriteTime() {
        if (currWriteTimeMs < formatTimeMs(System.currentTimeMillis())) {
            currWriteTimeMs = formatTimeMs(System.currentTimeMillis());
        }
    }

    public boolean checkStateForTimerMessageDelivers(TimerMessageDeliver[] timerMessageDelivers, int state) {
        for (AbstractStateService service : timerMessageDelivers) {
            if (!service.isState(state)) {
                return false;
            }
        }
        return true;
    }

    public boolean checkStateForTimerMessageQueries(TimerMessageQuery[] timerMessageQueries, int state) {
        for (AbstractStateService service : timerMessageQueries) {
            if (!service.isState(state)) {
                return false;
            }
        }
        return true;
    }

    public void checkDeliverQueueLatch(CountDownLatch latch, BlockingQueue<TimerRequest> timerMessageDeliverQueue, TimerMessageDeliver[] timerMessageDelivers, TimerMessageQuery[] timerMessageQueries, long delayedTime) throws Exception {
        if (latch.await(1, TimeUnit.SECONDS)) {
            return;
        }
        int checkNum = 0;
        while (true) {
            if (!timerMessageDeliverQueue.isEmpty()
                    || !checkStateForTimerMessageQueries(timerMessageQueries, AbstractStateService.WAITING)
                    || !checkStateForTimerMessageDelivers(timerMessageDelivers, AbstractStateService.WAITING)) {
                //let it go
            } else {
                checkNum++;
                if (checkNum >= 2) {
                    break;
                }
            }
            if (latch.await(1, TimeUnit.SECONDS)) {
                break;
            }
        }
        if (!latch.await(1, TimeUnit.SECONDS)) {
            LOGGER.warn("Check latch failed delayedTime:{}", delayedTime);
        }
    }

    public long getDequeueBehindMillis() {
        return System.currentTimeMillis() - currReadTimeMs;
    }

    public long getDequeueBehind() {
        return getDequeueBehindMillis() / 1000;
    }

    public long getAllCongestNum() {
        return timerWheel.getAllNum(currReadTimeMs);
    }

    public String getServiceThreadName() {
        String brokerIdentifier = "";
        if (this.messageStore instanceof DefaultMessageStore) {
            DefaultMessageStore messageStore = (DefaultMessageStore) this.messageStore;
            if (messageStore.getBrokerConfig().isInBrokerContainer()) {
                brokerIdentifier = messageStore.getBrokerConfig().getIdentifier();
            }
        }
        return brokerIdentifier;
    }

    private long formatTimeMs(long timeMs) {
        return timeMs / precisionMs * precisionMs;
    }

    public void flagShutdown() {
        state = SHUTDOWN;
    }

    public boolean isShutdown() {
        return SHUTDOWN == state;
    }

    public boolean isRunning() {
        return RUNNING == state;
    }

    public void flagRunning() {
        state = RUNNING;
    }

    public boolean needDelete(int magic) {
        return (magic & MAGIC_DELETE) != 0;
    }

    public boolean needRoll(int magic) {
        return (magic & MAGIC_ROLL) != 0;
    }

    public static boolean isMagicOK(int magic) {
        return (magic | 0xF) == 0xF;
    }

    public boolean isShouldRunningDequeue() {
        return shouldRunningDequeue;
    }

    public void setShouldRunningDequeue(boolean shouldRunningDequeue) {
        this.shouldRunningDequeue = shouldRunningDequeue;
    }
}
