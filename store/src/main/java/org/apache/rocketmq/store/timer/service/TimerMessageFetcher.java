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
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.queue.ConsumeQueue;
import org.apache.rocketmq.store.timer.TimerState;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerRequest;
import org.apache.rocketmq.store.util.PerfCounter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Consume the original topic queue,convert message to TimerTask and put it into the in-memory pending queue
 */
public class TimerMessageFetcher extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    public static final String TIMER_TOPIC = TopicValidator.SYSTEM_TOPIC_PREFIX + "wheel_timer";
    public static final String TIMER_OUT_MS = MessageConst.PROPERTY_TIMER_OUT_MS;
    public static final int MAGIC_DEFAULT = 1;
    public static final int DEFAULT_CAPACITY = 1024;
    //private TimerMessageStore timerMessageStore;
    private MessageStoreConfig storeConfig;
    private String serviceThreadName;
    private volatile BrokerRole lastBrokerRole = BrokerRole.SLAVE;
    private TimerState pointer;
    private TimerCheckpoint timerCheckpoint;
    private MessageStore messageStore;
    private PerfCounter.Ticks perfCounterTicks;
    private MessageReader messageReader;
    private BlockingQueue<TimerRequest> fetchedTimerMessageQueue;


    public TimerMessageFetcher(BlockingQueue<TimerRequest> fetchedTimerMessageQueue, MessageStoreConfig storeConfig, PerfCounter.Ticks perfCounterTicks, MessageReader messageReader, MessageStore messageStore, TimerState pointer, TimerCheckpoint timerCheckpoint, String serviceThreadName) {
        this.fetchedTimerMessageQueue = fetchedTimerMessageQueue;
        this.storeConfig = storeConfig;
        this.serviceThreadName = serviceThreadName;
        this.lastBrokerRole = storeConfig.getBrokerRole();
        this.messageReader = messageReader;
        this.pointer = pointer;
        this.timerCheckpoint = timerCheckpoint;
        this.messageStore = messageStore;
        this.perfCounterTicks = perfCounterTicks;
    }


    @Override
    public String getServiceName() {
        return serviceThreadName + this.getClass().getSimpleName();
    }

    private void checkBrokerRole() {
        BrokerRole currRole = storeConfig.getBrokerRole();
        if (lastBrokerRole != currRole) {
            synchronized (lastBrokerRole) {
                LOGGER.info("Broker role change from {} to {}", lastBrokerRole, currRole);
                //if change to master, do something
                if (BrokerRole.SLAVE != currRole) {
                    pointer.currQueueOffset = Math.min(pointer.currQueueOffset, timerCheckpoint.getMasterTimerQueueOffset());
                    pointer.commitQueueOffset = pointer.currQueueOffset;
                    pointer.prepareTimerCheckPoint();
                    timerCheckpoint.flush();
                    pointer.currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
                    pointer.commitReadTimeMs = pointer.currReadTimeMs;
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
        if (!pointer.shouldRunningDequeue && !isMaster() && pointer.currQueueOffset >= timerCheckpoint.getMasterTimerQueueOffset()) {
            return false;
        }

        return pointer.isRunning();
    }

    public boolean enqueue(int queueId) {
        if (storeConfig.isTimerStopEnqueue()) {
            return false;
        }
        if (!isRunningEnqueue()) {
            return false;
        }
        ConsumeQueue cq = (ConsumeQueue) this.messageStore.getConsumeQueue(TIMER_TOPIC, queueId);
        if (null == cq) {
            return false;
        }
        if (pointer.currQueueOffset < cq.getMinOffsetInQueue()) {
            LOGGER.warn("Timer currQueueOffset:{} is smaller than minOffsetInQueue:{}",
                    pointer.currQueueOffset, cq.getMinOffsetInQueue());
            pointer.currQueueOffset = cq.getMinOffsetInQueue();
        }
        long offset = pointer.currQueueOffset;
        SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(offset);
        if (null == bufferCQ) {
            return false;
        }
        try {
            int i = 0;
            for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                perfCounterTicks.startTick("enqueue_get");
                try {
                    long offsetPy = bufferCQ.getByteBuffer().getLong();
                    int sizePy = bufferCQ.getByteBuffer().getInt();
                    bufferCQ.getByteBuffer().getLong(); //tags code
                    MessageExt msgExt = messageReader.getMessageByCommitOffset(offsetPy, sizePy);
                    if (null == msgExt) {
                        perfCounterTicks.getCounter("enqueue_get_miss");
                    } else {
                        pointer.lastEnqueueButExpiredTime = System.currentTimeMillis();
                        pointer.lastEnqueueButExpiredStoreTime = msgExt.getStoreTimestamp();
                        long delayedTime = Long.parseLong(msgExt.getProperty(TIMER_OUT_MS));
                        // use CQ offset, not offset in Message
                        msgExt.setQueueOffset(offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE));
                        TimerRequest timerRequest = new TimerRequest(offsetPy, sizePy, delayedTime, System.currentTimeMillis(), MAGIC_DEFAULT, msgExt);
                        // System.out.printf("build enqueue request, %s%n", timerRequest);
                        while (!fetchedTimerMessageQueue.offer(timerRequest, 3, TimeUnit.SECONDS)) {
                            if (!isRunningEnqueue()) {
                                return false;
                            }
                        }
                    }
                } catch (Exception e) {
                    // here may cause the message loss
                    if (storeConfig.isTimerSkipUnknownError()) {
                        LOGGER.warn("Unknown error in skipped in enqueuing", e);
                    } else {
                        ThreadUtils.sleep(50);
                        throw e;
                    }
                } finally {
                    perfCounterTicks.endTick("enqueue_get");
                }
                // if broker role changes, ignore last enqueue
                if (!isRunningEnqueue()) {
                    return false;
                }
                pointer.currQueueOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            }
            pointer.currQueueOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            return i > 0;
        } catch (Exception e) {
            LOGGER.error("Unknown exception in enqueuing", e);
        } finally {
            bufferCQ.release();
        }
        return false;
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service start");
        while (!this.isStopped()) {
            try {
                if (!enqueue(0)) {
                    waitForRunning(100L * storeConfig.getTimerPrecisionMs() / 1000);
                }
            } catch (Throwable e) {
                LOGGER.error("Error occurred in " + getServiceName(), e);
            }
        }
        LOGGER.info(this.getServiceName() + " service end");
    }

}
