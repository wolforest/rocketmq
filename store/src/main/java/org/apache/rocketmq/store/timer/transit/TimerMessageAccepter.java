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
package org.apache.rocketmq.store.timer.transit;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.queue.ConsumeQueue;
import org.apache.rocketmq.store.timer.MessageOperator;
import org.apache.rocketmq.store.timer.TimerRequest;
import org.apache.rocketmq.store.timer.TimerState;
import org.apache.rocketmq.store.util.PerfCounter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Consume the original topic queue, convert message to TimerTask and put it into the in-memory pending queue
 *
 * pull message directly from consume queue with predefined queueId
 *
 */
public class TimerMessageAccepter extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    public static final String TIMER_TOPIC = TopicValidator.SYSTEM_TOPIC_PREFIX + "wheel_timer";
    public static final String TIMER_OUT_MS = MessageConst.PROPERTY_TIMER_OUT_MS;
    public static final int MAGIC_DEFAULT = 1;
    private final MessageStoreConfig storeConfig;
    private volatile BrokerRole lastBrokerRole = BrokerRole.SLAVE;
    private final TimerState timerState;
    private final PerfCounter.Ticks perfCounterTicks;
    private final MessageOperator messageOperator;
    private final BlockingQueue<TimerRequest> fetchedTimerMessageQueue;

    public TimerMessageAccepter(
            TimerState timerState,
            MessageStoreConfig storeConfig,
            MessageOperator messageOperator,
            BlockingQueue<TimerRequest> fetchedTimerMessageQueue,
            PerfCounter.Ticks perfCounterTicks) {
        this.timerState = timerState;
        this.storeConfig = storeConfig;
        this.messageOperator = messageOperator;
        this.fetchedTimerMessageQueue = fetchedTimerMessageQueue;
        this.perfCounterTicks = perfCounterTicks;
        this.lastBrokerRole = storeConfig.getBrokerRole();
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service start");
        while (!this.isStopped()) {
            try {
                if (!fetch(0)) {
                    waitForRunning(100L * storeConfig.getTimerPrecisionMs() / 1000);
                }
            } catch (Throwable e) {
                LOGGER.error("Error occurred in " + getServiceName(), e);
            }
        }
        LOGGER.info(this.getServiceName() + " service end");
    }

    private boolean fetch(int queueId) {
        if (storeConfig.isTimerStopEnqueue()) {
            return false;
        }
        if (!isRunningEnqueue()) {
            return false;
        }
        SelectMappedBufferResult queueItem = getIndexBuffer(queueId);
        if (null ==  queueItem) {
            return false;
        }

        long currQueueOffset = timerState.currQueueOffset;
        return fetch(queueItem, currQueueOffset);
    }

    private boolean fetch(SelectMappedBufferResult queueItem, long currQueueOffset) {
        try {
            int i = 0;
            for (; i <  queueItem.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                boolean status = enqueueTimerTask(queueItem, currQueueOffset, i);
                if (!status) {
                    return false;
                }

                // if broker role changes, ignore last enqueue
                if (!isRunningEnqueue()) {
                    return false;
                }
                timerState.currQueueOffset = forwardOffset(currQueueOffset, i);
            }
            timerState.currQueueOffset = forwardOffset(currQueueOffset, i);
            return i > 0;
        } catch (Exception e) {
            LOGGER.error("Unknown exception in enqueuing", e);
        } finally {
            queueItem.release();
        }
        return false;
    }

    private boolean enqueueTimerTask(SelectMappedBufferResult queueItem, long currQueueOffset, int i) throws Exception {
        perfCounterTicks.startTick("enqueue_get");
        try {
            long offsetPy =  queueItem.getByteBuffer().getLong();
            int sizePy =  queueItem.getByteBuffer().getInt();
            discard( queueItem);
            MessageExt msgExt = messageOperator.readMessageByCommitOffset(offsetPy, sizePy);
            if (null == msgExt) {
                perfCounterTicks.getCounter("enqueue_get_miss");
                return true;
            }
            timerState.lastEnqueueButExpiredTime = System.currentTimeMillis();
            timerState.lastEnqueueButExpiredStoreTime = msgExt.getStoreTimestamp();
            long delayedTime = Long.parseLong(msgExt.getProperty(TIMER_OUT_MS));
            // use CQ offset, not offset in Message
            msgExt.setQueueOffset(forwardOffset(currQueueOffset, i));
            TimerRequest timerRequest = new TimerRequest(offsetPy, sizePy, delayedTime, System.currentTimeMillis(), MAGIC_DEFAULT, msgExt);

            if (!loopOffer(timerRequest)) {
                return false;
            }

        } catch (Exception e) {
            deferThrow(e);
        } finally {
            perfCounterTicks.endTick("enqueue_get");
        }

        return true;
    }

    private long forwardOffset(long currQueueOffset, int i) {
        return currQueueOffset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
    }

    private void deferThrow(Exception e) throws Exception {
        // here may cause the message loss
        if (storeConfig.isTimerSkipUnknownError()) {
            LOGGER.warn("Unknown error in skipped in enqueuing", e);
        } else {
            ThreadUtils.sleep(50);
            throw e;
        }
    }

    private void discard(SelectMappedBufferResult bufferCQ) {
        bufferCQ.getByteBuffer().getLong(); //tags code,Just to move the cursor
    }

    private boolean loopOffer(TimerRequest timerRequest) throws InterruptedException {
        while (!fetchedTimerMessageQueue.offer(timerRequest, 3, TimeUnit.SECONDS)) {
            if (!isRunningEnqueue()) {
                return false;
            }
        }
        return true;
    }

    /**
     * get consume queue item ByteBuffer
     * facade of ConsumeQueue.getIndexBuffer
     *
     * @param queueId queueId
     * @return SelectMappedBufferResult
     */
    private SelectMappedBufferResult getIndexBuffer(int queueId) {
        ConsumeQueue cq = messageOperator.getConsumeQueue(TIMER_TOPIC, queueId);
        if (null == cq) {
            return null;
        }
        if (timerState.currQueueOffset < cq.getMinOffsetInQueue()) {
            LOGGER.warn("Timer currQueueOffset:{} is smaller than minOffsetInQueue:{}",
                    timerState.currQueueOffset, cq.getMinOffsetInQueue());
            timerState.currQueueOffset = cq.getMinOffsetInQueue();
        }
        return cq.getIndexBuffer(timerState.currQueueOffset);
    }

    @Override
    public String getServiceName() {
        return timerState.getServiceThreadName() + this.getClass().getSimpleName();
    }

    private void checkBrokerRole() {
        BrokerRole currRole = storeConfig.getBrokerRole();
        if (lastBrokerRole != currRole) {
            synchronized (lastBrokerRole) {
                LOGGER.info("Broker role change from {} to {}", lastBrokerRole, currRole);
                //if change to master, do something
                if (BrokerRole.SLAVE != currRole) {
                    timerState.currQueueOffset = Math.min(timerState.currQueueOffset, timerState.timerCheckpoint.getMasterTimerQueueOffset());
                    timerState.commitQueueOffset = timerState.currQueueOffset;
                    timerState.prepareTimerCheckPoint();
                    timerState.timerCheckpoint.flush();
                    timerState.currReadTimeMs = timerState.timerCheckpoint.getLastReadTimeMs();
                    timerState.commitReadTimeMs = timerState.currReadTimeMs;
                }
                //if change to slave, just let it go
                lastBrokerRole = currRole;
            }
        }
    }

    private boolean isMaster() {
        return BrokerRole.SLAVE != lastBrokerRole;
    }

    private boolean isRunningEnqueue() {
        checkBrokerRole();
        if (!timerState.isShouldRunningDequeue() && !isMaster() && timerState.currQueueOffset >= timerState.timerCheckpoint.getMasterTimerQueueOffset()) {
            return false;
        }

        return timerState.isRunning();
    }
}
