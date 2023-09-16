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

import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.metrics.DefaultStoreMetricsManager;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerState;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.timer.TimerRequest;
import org.apache.rocketmq.store.util.PerfCounter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.rocketmq.store.timer.TimerMessageStore.DEQUEUE_PUT;
import static org.apache.rocketmq.store.timer.TimerState.PUT_NEED_RETRY;

public class TimerMessageDeliver extends AbstractStateService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    @Override
    public String getServiceName() {
        return serviceThreadName + this.getClass().getSimpleName();
    }

    private TimerMessageStore timerMessageStore;
    private BlockingQueue<TimerRequest> timerMessageDeliverQueue;
    private PerfCounter.Ticks perfCounterTicks;
    private TimerState pointer;
    private MessageStoreConfig storeConfig;
    private TimerMetricManager metricManager;
    private String serviceThreadName;
    private BrokerStatsManager brokerStatsManager;
    private Function<MessageExtBrokerInner, PutMessageResult> escapeBridgeHook;

    public TimerMessageDeliver(TimerMessageStore timerMessageStore, TimerMetricManager timerMetricManager,
                               BlockingQueue<TimerRequest> timerMessageDeliverQueue, PerfCounter.Ticks perfCounterTicks,
                               TimerState timerState, MessageStoreConfig storeConfig, String serviceThreadName, BrokerStatsManager brokerStatsManager,
                               Function<MessageExtBrokerInner, PutMessageResult> escapeBridgeHook
    ) {
        this.timerMessageStore = timerMessageStore;
        this.timerMessageDeliverQueue = timerMessageDeliverQueue;
        this.perfCounterTicks = perfCounterTicks;
        this.pointer = timerState;
        this.storeConfig = storeConfig;
        this.metricManager = timerMetricManager;
        this.serviceThreadName = serviceThreadName;
        this.brokerStatsManager = brokerStatsManager;
        this.escapeBridgeHook = escapeBridgeHook;
    }


    @Override
    public void run() {
        setState(AbstractStateService.START);
        LOGGER.info(this.getServiceName() + " service start");
        while (!this.isStopped() || timerMessageDeliverQueue.size() != 0) {
            try {
                setState(AbstractStateService.WAITING);
                TimerRequest tr = timerMessageDeliverQueue.poll(10, TimeUnit.MILLISECONDS);
                if (null == tr) {
                    continue;
                }
                setState(AbstractStateService.RUNNING);
                boolean doRes = false;
                boolean tmpDequeueChangeFlag = false;
                try {

                    while (!isStopped() && !doRes) {
                        if (!pointer.isRunningDequeue()) {
                            pointer.dequeueStatusChangeFlag = true;
                            tmpDequeueChangeFlag = true;
                            break;
                        }

                        try {
                            perfCounterTicks.startTick(DEQUEUE_PUT);
                            DefaultStoreMetricsManager.incTimerDequeueCount(getRealTopic(tr.getMsg()));
                            metricManager.addMetric(tr.getMsg(), -1);
                            MessageExtBrokerInner msg = convert(tr.getMsg(), tr.getEnqueueTime(), timerMessageStore.timerState.needRoll(tr.getMagic()));

                            doRes = PUT_NEED_RETRY != timerMessageStore.doPut(msg, timerMessageStore.timerState.needRoll(tr.getMagic()));

                            while (!doRes && !isStopped()) {
                                if (!pointer.isRunningDequeue()) {
                                    pointer.dequeueStatusChangeFlag = true;
                                    tmpDequeueChangeFlag = true;
                                    break;
                                }

                                doRes = PUT_NEED_RETRY != timerMessageStore.doPut(msg, timerMessageStore.timerState.needRoll(tr.getMagic()));
                                Thread.sleep(500L * timerMessageStore.getPrecisionMs() / 1000);
                            }
                            perfCounterTicks.endTick(DEQUEUE_PUT);
                        } catch (Throwable t) {
                            LOGGER.info("Unknown error", t);
                            if (storeConfig.isTimerSkipUnknownError()) {
                                doRes = true;
                            } else {
                                ThreadUtils.sleep(50);
                            }
                        }
                    }
                } finally {
                    tr.idempotentRelease(!tmpDequeueChangeFlag);
                }

            } catch (Throwable e) {
                LOGGER.error("Error occurred in " + getServiceName(), e);
            }
        }
        LOGGER.info(this.getServiceName() + " service end");
        setState(AbstractStateService.END);
    }

    public String getRealTopic(MessageExt msgExt) {
        if (msgExt == null) {
            return null;
        }
        return msgExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC);
    }

    public MessageExtBrokerInner convert(MessageExt messageExt, long enqueueTime, boolean needRoll) {
        if (enqueueTime != -1) {
            MessageAccessor.putProperty(messageExt, TimerState.TIMER_ENQUEUE_MS, enqueueTime + "");
        }
        if (needRoll) {
            if (messageExt.getProperty(TimerState.TIMER_ROLL_TIMES) != null) {
                MessageAccessor.putProperty(messageExt, TimerState.TIMER_ROLL_TIMES, Integer.parseInt(messageExt.getProperty(TimerState.TIMER_ROLL_TIMES)) + 1 + "");
            } else {
                MessageAccessor.putProperty(messageExt, TimerState.TIMER_ROLL_TIMES, 1 + "");
            }
        }
        MessageAccessor.putProperty(messageExt, TimerState.TIMER_DEQUEUE_MS, System.currentTimeMillis() + "");
        MessageExtBrokerInner message = convertMessage(messageExt, needRoll);
        return message;
    }

    public MessageExtBrokerInner convertMessage(MessageExt msgExt, boolean needRoll) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);

        if (needRoll) {
            msgInner.setTopic(msgExt.getTopic());
            msgInner.setQueueId(msgExt.getQueueId());
        } else {
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
            msgInner.setQueueId(Integer.parseInt(msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC);
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID);
        }
        return msgInner;
    }

}

