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
package org.apache.rocketmq.store.timer;

import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class TimerState {
    public static final int INITIAL = 0, RUNNING = 1, HAULT = 2, SHUTDOWN = 3;
    // The total days in the timer wheel when precision is 1000ms.
    // If the broker shutdown last more than the configured days, will cause message loss
    public static final int TIMER_WHEEL_TTL_DAY = 7;
    public static final int DAY_SECS = 24 * 3600;
    public static final int TIMER_BLANK_SLOTS = 60;
    public volatile long currReadTimeMs;
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
    public volatile boolean shouldRunningDequeue;

    //the dequeue is an asynchronous process, use this flag to track if the status has changed
    public boolean dequeueStatusChangeFlag = false;

    private volatile int state = INITIAL;
    private TimerCheckpoint timerCheckpoint;
    private TimerLog timerLog;
    private MessageStore messageStore;
    private MessageStoreConfig storeConfig;
    public final int precisionMs;
    public final int timerRollWindowSlots;
    public final int slotsTotal;

    public TimerState(TimerCheckpoint timerCheckpoint,MessageStoreConfig storeConfig, TimerLog timerLog, MessageStore messageStore) {
        this.timerCheckpoint = timerCheckpoint;
        this.storeConfig = storeConfig;
        this.timerLog = timerLog;
        this.messageStore = messageStore;
        this.precisionMs = storeConfig.getTimerPrecisionMs();
        // TimerWheel contains the fixed number of slots regardless of precision.
        this.slotsTotal = TIMER_WHEEL_TTL_DAY * DAY_SECS;
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
}