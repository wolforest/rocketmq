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
package org.apache.rocketmq.store.domain.timer.model;

import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.domain.timer.persistence.TimerCheckpoint;
import org.apache.rocketmq.store.domain.timer.transit.AbstractStateThread;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerLog;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerWheel;
import org.apache.rocketmq.store.domain.timer.transit.TimerMessageQuerier;
import org.apache.rocketmq.store.domain.timer.transit.TimerMessageProducer;

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

    /**
     *
     */
    public static final int TIMER_BLANK_SLOTS = 60;
    /**
     * last read timestamp of persistence.scan()
     * updated by:
     *  - BrokerMessageService: when init
     *  - TimerMessageConsumer: when broker role changed
     *  - TimerMessageRecover: when recover
     *  - TimerMessagePersistence: when scan result is empty
     *  - TimerMessageScanner: after scan add 1 * precisionMs
     */
    public volatile long currReadTimeMs;
    /**
     * last write timestamp of timer message
     * updated by:
     * - TimerMessage.start()
     * - TimerMessageSaver.fetchAndPutTimerRequest()
     */
    public volatile long currWriteTimeMs;

    /**
     * preload timer tasks time
     * updated by:
     * - TimerMessageStore.warmDequeue
     */
    public volatile long preReadTimeMs;

    /**
     * equals currReadTimeMs in all code base,
     * maybe useful in not opened source.
     */
    public volatile long commitReadTimeMs;

    /**
     * consume queue offset already pulled to timer system
     * updated by:
     * - TimerMessageConsumer
     * - TimerMessageRecover
     */
    public volatile long currQueueOffset; //only one queue that is 0

    /**
     * consume queue offset already write to timer persistence(not flushed)
     * updated by :
     * - TimerMessageSaver
     * - TimerMessageRecover
     */
    public volatile long commitQueueOffset;

    /**
     * lastCommitReadTimeMs and lastCommitQueueOffset are same
     * updated by:
     * - TimerMessageConsumer: broker role change
     * - TimerFlushService: flush
     * - TimerMessageRecover: recover
     * - TimerMessageStore: shutdown
     */
    public volatile long lastCommitReadTimeMs;
    public volatile long lastCommitQueueOffset;

    /**
     * the latest time when pull messageExt from message queue
     * updated by TimerMessageConsumer
     */
    public long lastEnqueueButExpiredTime;
    /**
     * the time of the latest messageExt.storeTimeStamp
     * updated by TimerMessageConsumer
     */
    public long lastEnqueueButExpiredStoreTime;

    /**
     * True if current store is master
     *  or current brokerId is equal to the minimum brokerId
     *  of the replica group in slaveActingMaster mode.
     */
    private volatile boolean shouldRunningDequeue;

    /**
     * the dequeue is an asynchronous process, use this flag to track if the status has changed
     */
    public boolean dequeueStatusChangeFlag = false;

    private volatile int state = INITIAL;
    public TimerCheckpoint timerCheckpoint;
    private final TimerWheel timerWheel;
    private final TimerLog timerLog;
    private final MessageStore messageStore;
    public final int precisionMs;

    /**
     * timer wheel slots number
     * default: 2 * 24 * 3600
     */
    public final int timerRollWindowSlots;
    /**
     * @renamed from slotsTotal = totalSlots
     * slotsTotal = TIMER_WHEEL_TTL_DAY * DAY_SECS
     * default: 7 * 24 * 3600
     */
    public final int totalSlots;

    public TimerState(TimerCheckpoint timerCheckpoint, MessageStoreConfig storeConfig, TimerLog timerLog, int totalSlots, TimerWheel timerWheel, MessageStore messageStore) {
        this.timerCheckpoint = timerCheckpoint;
        this.timerLog = timerLog;
        this.timerWheel = timerWheel;
        this.messageStore = messageStore;
        this.precisionMs = storeConfig.getTimerPrecisionMs();
        // TimerWheel contains the fixed number of slots regardless of precision.
        this.totalSlots = totalSlots;

        // timerRollWindow contains the fixed number of slots regardless of precision.
        if (storeConfig.getTimerRollWindowSlot() > totalSlots - TIMER_BLANK_SLOTS
                || storeConfig.getTimerRollWindowSlot() < 2) {
            this.timerRollWindowSlots = totalSlots - TIMER_BLANK_SLOTS;
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
        timerCheckpoint.setLastTimerLogFlushPos(timerLog.getMappedFileQueue().getFlushedPosition());
        timerCheckpoint.setLastReadTimeMs(commitReadTimeMs);

        prepareCheckPointForDequeue();

        timerCheckpoint.setLastTimerQueueOffset(Math.min(commitQueueOffset, timerCheckpoint.getMasterTimerQueueOffset()));
    }

    private void prepareCheckPointForDequeue() {
        if (!shouldRunningDequeue) {
            return;
        }

        timerCheckpoint.setMasterTimerQueueOffset(commitQueueOffset);
        if (commitReadTimeMs == lastCommitReadTimeMs && commitQueueOffset == lastCommitQueueOffset) {
            return;
        }

        timerCheckpoint.updateDateVersion(messageStore.getStateMachineVersion());
        lastCommitReadTimeMs = commitReadTimeMs;
        lastCommitQueueOffset = commitQueueOffset;
    }

    /**
     * move readTime forward by 1s
     * called by TimerWheelPersistence.scan()
     * when slot.timeMs == -1; AKA slot is useless.
     *
     * @param precisionMs precision
     */
    public void moveReadTime(int precisionMs) {
        currReadTimeMs = currReadTimeMs + precisionMs;
        commitReadTimeMs = currReadTimeMs;
    }

    /**
     * called by:
     * - TimerMessage.start()
     * - TimerMessageSaver.fetchAndPutTimerRequest()
     */
    public void maybeMoveWriteTime() {
        if (currWriteTimeMs < formatTimeMs(System.currentTimeMillis())) {
            currWriteTimeMs = formatTimeMs(System.currentTimeMillis());
        }
    }

    public boolean checkStateForTimerMessageDelivers(TimerMessageProducer[] timerMessageProducers, int state) {
        for (AbstractStateThread service : timerMessageProducers) {
            if (!service.isState(state)) {
                return false;
            }
        }
        return true;
    }

    public boolean checkStateForTimerMessageQueries(TimerMessageQuerier[] timerMessageQueries, int state) {
        for (AbstractStateThread service : timerMessageQueries) {
            if (!service.isState(state)) {
                return false;
            }
        }
        return true;
    }

    public void checkDeliverQueueLatch(CountDownLatch latch, BlockingQueue<TimerRequest> timerMessageDeliverQueue, TimerMessageProducer[] timerMessageProducers, TimerMessageQuerier[] timerMessageQueries, long delayedTime) throws Exception {
        if (latch.await(1, TimeUnit.SECONDS)) {
            return;
        }
        int checkNum = 0;
        while (true) {
            if (!timerMessageDeliverQueue.isEmpty()
                    || !checkStateForTimerMessageQueries(timerMessageQueries, AbstractStateThread.WAITING)
                    || !checkStateForTimerMessageDelivers(timerMessageProducers, AbstractStateThread.WAITING)) {
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
        if (!(this.messageStore instanceof DefaultMessageStore)) {
            return "";
        }

        DefaultMessageStore messageStore = (DefaultMessageStore) this.messageStore;
        if (messageStore.getBrokerConfig().isInBrokerContainer()) {
            return messageStore.getBrokerConfig().getIdentifier();
        }

        return "";
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
