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
package org.apache.rocketmq.broker.schedule;

import io.opentelemetry.api.common.Attributes;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.common.lang.attribute.TopicMessageType;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.domain.topic.TopicValidator;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_MESSAGE_TYPE;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;

public class PutResultProcess {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private String topic;
    private long offset;
    private long physicOffset;
    private int physicSize;
    private int delayLevel;
    private String msgId;
    private boolean autoResend = false;
    private CompletableFuture<PutMessageResult> future;

    private volatile AtomicInteger resendCount = new AtomicInteger(0);
    private volatile ProcessStatus status = ProcessStatus.RUNNING;
    private final ScheduleMessageService scheduleMessageService;

    public PutResultProcess(ScheduleMessageService scheduleMessageService) {
        this.scheduleMessageService = scheduleMessageService;
    }

    public PutResultProcess setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public PutResultProcess setOffset(long offset) {
        this.offset = offset;
        return this;
    }

    public PutResultProcess setPhysicOffset(long physicOffset) {
        this.physicOffset = physicOffset;
        return this;
    }

    public PutResultProcess setPhysicSize(int physicSize) {
        this.physicSize = physicSize;
        return this;
    }

    public PutResultProcess setDelayLevel(int delayLevel) {
        this.delayLevel = delayLevel;
        return this;
    }

    public PutResultProcess setMsgId(String msgId) {
        this.msgId = msgId;
        return this;
    }

    public PutResultProcess setAutoResend(boolean autoResend) {
        this.autoResend = autoResend;
        return this;
    }

    public PutResultProcess setFuture(CompletableFuture<PutMessageResult> future) {
        this.future = future;
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public long getOffset() {
        return offset;
    }

    public long getNextOffset() {
        return offset + 1;
    }

    public long getPhysicOffset() {
        return physicOffset;
    }

    public int getPhysicSize() {
        return physicSize;
    }

    public Integer getDelayLevel() {
        return delayLevel;
    }

    public String getMsgId() {
        return msgId;
    }

    public boolean isAutoResend() {
        return autoResend;
    }

    public CompletableFuture<PutMessageResult> getFuture() {
        return future;
    }

    public AtomicInteger getResendCount() {
        return resendCount;
    }

    public PutResultProcess thenProcess() {
        this.future.thenAccept(this::handleResult);

        this.future.exceptionally(e -> {
            log.error("ScheduleMessageService put message exceptionally, info: {}",
                PutResultProcess.this.toString(), e);

            onException();
            return null;
        });
        return this;
    }

    private void handleResult(PutMessageResult result) {
        if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            onSuccess(result);
        } else {
            log.warn("ScheduleMessageService put message failed. info: {}.", result);
            onException();
        }
    }

    public void onSuccess(PutMessageResult result) {
        this.status = ProcessStatus.SUCCESS;
        MessageStoreConfig messageStoreConfig = scheduleMessageService.getBrokerController().getMessageStore().getMessageStoreConfig();
        BrokerStatsManager brokerStatsManager = scheduleMessageService.getBrokerController().getBrokerStatsManager();

        if (!messageStoreConfig.isEnableScheduleMessageStats() || result.isRemotePut()) {
            return;
        }

        brokerStatsManager.incQueueGetNums(MQConstants.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel - 1, result.getAppendMessageResult().getMsgNum());
        brokerStatsManager.incQueueGetSize(MQConstants.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel - 1, result.getAppendMessageResult().getWroteBytes());
        brokerStatsManager.incGroupGetNums(MQConstants.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, result.getAppendMessageResult().getMsgNum());
        brokerStatsManager.incGroupGetSize(MQConstants.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, result.getAppendMessageResult().getWroteBytes());

        Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
            .put(LABEL_TOPIC, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)
            .put(LABEL_CONSUMER_GROUP, MQConstants.SCHEDULE_CONSUMER_GROUP)
            .put(LABEL_IS_SYSTEM, true)
            .build();
        BrokerMetricsManager.messagesOutTotal.add(result.getAppendMessageResult().getMsgNum(), attributes);
        BrokerMetricsManager.throughputOutTotal.add(result.getAppendMessageResult().getWroteBytes(), attributes);

        brokerStatsManager.incTopicPutNums(this.topic, result.getAppendMessageResult().getMsgNum(), 1);
        brokerStatsManager.incTopicPutSize(this.topic, result.getAppendMessageResult().getWroteBytes());
        brokerStatsManager.incBrokerPutNums(this.topic, result.getAppendMessageResult().getMsgNum());

        attributes = BrokerMetricsManager.newAttributesBuilder()
            .put(LABEL_TOPIC, topic)
            .put(LABEL_MESSAGE_TYPE, TopicMessageType.DELAY.getMetricsValue())
            .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(topic))
            .build();
        BrokerMetricsManager.messagesInTotal.add(result.getAppendMessageResult().getMsgNum(), attributes);
        BrokerMetricsManager.throughputInTotal.add(result.getAppendMessageResult().getWroteBytes(), attributes);
        BrokerMetricsManager.messageSize.record(result.getAppendMessageResult().getWroteBytes() / result.getAppendMessageResult().getMsgNum(), attributes);
    }

    public void onException() {
        log.warn("ScheduleMessageService onException, info: {}", this.toString());
        if (this.autoResend) {
            this.status = ProcessStatus.EXCEPTION;
        } else {
            this.status = ProcessStatus.SKIP;
        }
    }

    public ProcessStatus getStatus() {
        return this.status;
    }

    public PutMessageResult get() {
        try {
            return this.future.get();
        } catch (InterruptedException | ExecutionException e) {
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        }
    }

    public void doResend() {
        log.info("Resend message, info: {}", this.toString());

        // Gradually increase the resend interval.
        try {
            Thread.sleep(Math.min(this.resendCount.incrementAndGet() * 100, 60 * 1000));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            MessageExt msgExt = scheduleMessageService.getBrokerController().getMessageStore().lookMessageByOffset(this.physicOffset, this.physicSize);
            if (msgExt == null) {
                log.warn("ScheduleMessageService resend not found message. info: {}", this.toString());
                this.status = need2Skip() ? ProcessStatus.SKIP : ProcessStatus.EXCEPTION;
                return;
            }

            MessageExtBrokerInner msgInner = scheduleMessageService.messageTimeUp(msgExt);
            PutMessageResult result = scheduleMessageService.getBrokerController().getEscapeBridge().putMessage(msgInner);
            this.handleResult(result);
            if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                log.info("Resend message success, info: {}", this.toString());
            }
        } catch (Exception e) {
            this.status = ProcessStatus.EXCEPTION;
            log.error("Resend message error, info: {}", this.toString(), e);
        }
    }

    public boolean need2Blocked() {
        int maxResendNum2Blocked = scheduleMessageService.getBrokerController().getMessageStore().getMessageStoreConfig()
            .getScheduleAsyncDeliverMaxResendNum2Blocked();
        return this.resendCount.get() > maxResendNum2Blocked;
    }

    public boolean need2Skip() {
        int maxResendNum2Blocked = scheduleMessageService.getBrokerController().getMessageStore().getMessageStoreConfig()
            .getScheduleAsyncDeliverMaxResendNum2Blocked();
        return this.resendCount.get() > maxResendNum2Blocked * 2;
    }

    @Override
    public String toString() {
        return "PutResultProcess{" +
            "topic='" + topic + '\'' +
            ", offset=" + offset +
            ", physicOffset=" + physicOffset +
            ", physicSize=" + physicSize +
            ", delayLevel=" + delayLevel +
            ", msgId='" + msgId + '\'' +
            ", autoResend=" + autoResend +
            ", resendCount=" + resendCount +
            ", status=" + status +
            '}';
    }
}
