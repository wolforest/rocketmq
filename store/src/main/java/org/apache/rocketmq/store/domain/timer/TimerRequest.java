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

import org.apache.rocketmq.common.domain.message.MessageExt;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * timer task to be scheduled, build from ConsumeQueue and CommitLog
 */
public class TimerRequest {

    /**
     * commitLog offset
     */
    private final long offsetPy;
    /**
     * size of message in the commitLog
     */
    private final int sizePy;
    /**
     * delayTime of message, stored in message property map
     */
    private final long delayTime;
    /**
     * magic code, always equals TimerMessageAccepter.MAGIC_DEFAULT (1)
     */
    private final int magic;

    /**
     * enqueue timestamp (ms)
     */
    private long enqueueTime;
    /**
     * timer task related msg
     */
    private MessageExt msg;

    //optional would be a good choice, but it relies on JDK 8
    private CountDownLatch latch;

    private boolean released;

    //whether the operation is successful
    private boolean success;

    private Set<String> deleteList;

    public TimerRequest(long offsetPy, int sizePy, long delayTime, long enqueueTime, int magic) {
        this(offsetPy, sizePy, delayTime, enqueueTime, magic, null);
    }

    public TimerRequest(long offsetPy, int sizePy, long delayTime, long enqueueTime, int magic, MessageExt msg) {
        this.offsetPy = offsetPy;
        this.sizePy = sizePy;
        this.delayTime = delayTime;
        this.enqueueTime = enqueueTime;
        this.magic = magic;
        this.msg = msg;
    }

    public long getOffsetPy() {
        return offsetPy;
    }

    public int getSizePy() {
        return sizePy;
    }

    public long getDelayTime() {
        return delayTime;
    }

    public long getEnqueueTime() {
        return enqueueTime;
    }

    public MessageExt getMsg() {
        return msg;
    }

    public void setMsg(MessageExt msg) {
        this.msg = msg;
    }

    public int getMagic() {
        return magic;
    }

    public Set<String> getDeleteList() {
        return deleteList;
    }

    public void setDeleteList(Set<String> deleteList) {
        this.deleteList = deleteList;
    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }
    public void setEnqueueTime(long enqueueTime) {
        this.enqueueTime = enqueueTime;
    }
    public void idempotentRelease() {
        idempotentRelease(true);
    }

    public void idempotentRelease(boolean success) {
        this.success = success;
        if (!released && latch != null) {
            released = true;
            latch.countDown();
        }
    }

    public boolean isSuccess() {
        return success;
    }

    @Override
    public String toString() {
        return "TimerRequest{" +
            "offsetPy=" + offsetPy +
            ", sizePy=" + sizePy +
            ", delayTime=" + delayTime +
            ", enqueueTime=" + enqueueTime +
            ", magic=" + magic +
            ", msg=" + msg +
            ", latch=" + latch +
            ", released=" + released +
            ", succ=" + success +
            ", deleteList=" + deleteList +
            '}';
    }
}
