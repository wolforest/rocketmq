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

import org.apache.rocketmq.common.domain.topic.TopicFilterType;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageAccessor;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.domain.timer.metrics.TimerMetricManager;
import org.apache.rocketmq.store.domain.timer.model.TimerState;
import org.apache.rocketmq.store.server.metrics.DefaultStoreMetricsManager;
import org.apache.rocketmq.store.api.broker.stats.BrokerStatsManager;
import org.apache.rocketmq.store.domain.timer.model.TimerRequest;
import org.apache.rocketmq.store.server.metrics.PerfCounter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.rocketmq.store.domain.timer.TimerMessageStore.DEQUEUE_PUT;

/**
 * put timer message back to commitLog
 */
public class TimerMessageDeliver extends AbstractStateThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final BlockingQueue<TimerRequest> timerMessageDeliverQueue;
    private final PerfCounter.Ticks perfCounterTicks;
    private final TimerState timerState;
    private final MessageStoreConfig storeConfig;
    private final TimerMetricManager metricManager;
    private final BrokerStatsManager brokerStatsManager;
    private final Function<MessageExtBrokerInner, PutMessageResult> escapeBridgeHook;
    private final MessageOperator messageOperator;

    public TimerMessageDeliver(
            TimerState timerState,
            MessageStoreConfig storeConfig,
            MessageOperator messageOperator,
            BlockingQueue<TimerRequest> timerMessageDeliverQueue,
            BrokerStatsManager brokerStatsManager,
            TimerMetricManager timerMetricManager,
            Function<MessageExtBrokerInner, PutMessageResult> escapeBridgeHook,
            PerfCounter.Ticks perfCounterTicks
    ) {
        this.timerMessageDeliverQueue = timerMessageDeliverQueue;
        this.perfCounterTicks = perfCounterTicks;
        this.timerState = timerState;
        this.storeConfig = storeConfig;
        this.metricManager = timerMetricManager;
        this.brokerStatsManager = brokerStatsManager;
        this.escapeBridgeHook = escapeBridgeHook;
        this.messageOperator = messageOperator;
    }

    @Override
    public String getServiceName() {
        return timerState.getServiceThreadName() + this.getClass().getSimpleName();
    }

    @Override
    public void run() {
        setState(AbstractStateThread.START);
        LOGGER.info(this.getServiceName() + " service start");

        while (!this.isStopped() || timerMessageDeliverQueue.size() != 0) {
            try {
                setState(AbstractStateThread.WAITING);
                TimerRequest timerRequest = timerMessageDeliverQueue.poll(10, TimeUnit.MILLISECONDS);
                if (null == timerRequest) {
                    continue;
                }

                run(timerRequest);

            } catch (Throwable e) {
                LOGGER.error("Error occurred in " + getServiceName(), e);
            }
        }
        LOGGER.info(this.getServiceName() + " service end");
        setState(AbstractStateThread.END);
    }

    private void run(TimerRequest timerRequest) {
        setState(AbstractStateThread.RUNNING);
        boolean doRes = false;
        boolean tmpDequeueChangeFlag = false;
        try {

            while (!isStopped() && !doRes) {
                if (!timerState.isRunningDequeue()) {
                    timerState.dequeueStatusChangeFlag = true;
                    tmpDequeueChangeFlag = true;
                    break;
                }

                try {
                    perfCounterTicks.startTick(DEQUEUE_PUT);
                    MessageExt msgExt = timerRequest.getMsg();
                    DefaultStoreMetricsManager.incTimerDequeueCount(messageOperator.getRealTopic(msgExt));
                    if (timerRequest.getEnqueueTime() == Long.MAX_VALUE) {
                        // never enqueue, mark it.
                        MessageAccessor.putProperty(msgExt, TimerState.TIMER_ENQUEUE_MS, String.valueOf(Long.MAX_VALUE));
                    }

                    metricManager.addMetric(msgExt, -1);
                    MessageExtBrokerInner msg = convert(msgExt, timerRequest.getEnqueueTime(), timerState.needRoll(timerRequest.getMagic()));

                    doRes = TimerState.PUT_NEED_RETRY != doPut(msg, timerState.needRoll(timerRequest.getMagic()));

                    while (!doRes && !isStopped()) {
                        if (!timerState.isRunningDequeue()) {
                            timerState.dequeueStatusChangeFlag = true;
                            tmpDequeueChangeFlag = true;
                            break;
                        }

                        doRes = TimerState.PUT_NEED_RETRY != doPut(msg, timerState.needRoll(timerRequest.getMagic()));
                        Thread.sleep(500L * timerState.precisionMs / 1000);
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
            timerRequest.idempotentRelease(!tmpDequeueChangeFlag);
        }
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
        return convertMessage(messageExt, needRoll);
    }

    public MessageExtBrokerInner convertMessage(MessageExt msgExt, boolean needRoll) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, MessageAccessor.deepCopyProperties(msgExt.getProperties()));
        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
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

    //0 success; 1 fail, need retry; 2 fail, do not retry;
    public int doPut(MessageExtBrokerInner message, boolean roll) throws Exception {

        if (!roll && null != message.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY)) {
            LOGGER.warn("Trying do put delete timer msg:[{}] roll:[{}]", message, roll);
            return TimerState.PUT_NO_RETRY;
        }

        PutMessageResult putMessageResult = putMessage(message);

        int retryNum = 0;
        while (retryNum < 3) {
            int checkStatus = checkPutResult(putMessageResult, message);
            if (checkStatus != TimerState.PUT_FAILED) {
                return checkStatus;
            }

            retryNum++;
            Thread.sleep(50);

            putMessageResult = putMessage(message);
            LOGGER.warn("Retrying to do put timer msg retryNum:{} putRes:{} msg:{}", retryNum, putMessageResult, message);
        }
        return TimerState.PUT_NO_RETRY;
    }

    private int checkPutResult(PutMessageResult putMessageResult, MessageExtBrokerInner message) {
        if (null == putMessageResult || null == putMessageResult.getPutMessageStatus()) {
            return TimerState.PUT_FAILED;
        }

        switch (putMessageResult.getPutMessageStatus()) {
            case PUT_OK:
                incPutMatrix(putMessageResult, message);
                return TimerState.PUT_OK;
            case SERVICE_NOT_AVAILABLE:
                return TimerState.PUT_NEED_RETRY;
            case MESSAGE_ILLEGAL:
            case PROPERTIES_SIZE_EXCEEDED:
                return TimerState.PUT_NO_RETRY;
            case CREATE_MAPPED_FILE_FAILED:
            case FLUSH_DISK_TIMEOUT:
            case FLUSH_SLAVE_TIMEOUT:
            case OS_PAGE_CACHE_BUSY:
            case SLAVE_NOT_AVAILABLE:
            case UNKNOWN_ERROR:
            default:
                return TimerState.PUT_FAILED;
        }
    }

    private void incPutMatrix(PutMessageResult putMessageResult, MessageExtBrokerInner message) {
        if (brokerStatsManager == null) {
            return;
        }

        this.brokerStatsManager.incTopicPutNums(message.getTopic(), 1, 1);

        if (putMessageResult.getAppendMessageResult() != null) {
            this.brokerStatsManager.incTopicPutSize(message.getTopic(),
                putMessageResult.getAppendMessageResult().getWroteBytes());
        }

        this.brokerStatsManager.incBrokerPutNums(message.getTopic(), 1);
    }

    private PutMessageResult putMessage(MessageExtBrokerInner message) {
        if (escapeBridgeHook != null) {
            return escapeBridgeHook.apply(message);
        }

        return messageOperator.putMessage(message);
    }
}

