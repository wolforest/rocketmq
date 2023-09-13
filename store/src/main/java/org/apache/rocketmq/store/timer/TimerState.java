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

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.store.MessageStore;

public class Pointer {
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

    private TimerCheckpoint timerCheckpoint;
    private TimerLog timerLog;
    private MessageStore messageStore;

    public Pointer(TimerCheckpoint timerCheckpoint, TimerLog timerLog, MessageStore messageStore) {
        this.timerCheckpoint = timerCheckpoint;
        this.timerLog = timerLog;
        this.messageStore = messageStore;
    }

    public void syncLastReadTimeMs(Long lastReadTimeMs) {
        currReadTimeMs = lastReadTimeMs;// timerCheckpoint.getLastReadTimeMs();
        commitReadTimeMs = currReadTimeMs;
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


}