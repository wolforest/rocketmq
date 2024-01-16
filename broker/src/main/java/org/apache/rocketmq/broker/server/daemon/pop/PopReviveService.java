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
package org.apache.rocketmq.broker.server.daemon.pop;

import com.alibaba.fastjson.JSON;
import io.opentelemetry.api.common.Attributes;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.server.metrics.PopMetricsManager;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.domain.topic.KeyBuilder;
import org.apache.rocketmq.common.lang.Pair;
import org.apache.rocketmq.common.domain.constant.PopConstants;
import org.apache.rocketmq.common.lang.thread.ServiceThread;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.domain.topic.TopicFilterType;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageAccessor;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.domain.topic.TopicValidator;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.api.dto.AppendMessageStatus;
import org.apache.rocketmq.store.api.dto.GetMessageResult;
import org.apache.rocketmq.store.api.dto.GetMessageStatus;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.api.pop.AckMsg;
import org.apache.rocketmq.store.api.pop.BatchAckMsg;
import org.apache.rocketmq.store.api.pop.PopCheckPoint;
import org.apache.rocketmq.store.api.pop.PopKeyBuilder;

import static org.apache.rocketmq.broker.server.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.server.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.server.metrics.BrokerMetricsConstant.LABEL_TOPIC;

/**
 *
 *
 * this is a standalone thread service, do not accept any input
 * only api is the run method
 */
public class PopReviveService extends ServiceThread {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);

    private final int queueId;
    private final Broker broker;
    private final String reviveTopic;
    private long currentReviveMessageTimestamp = -1;
    private volatile boolean shouldRunPopRevive = false;

    /**
     *
     */
    private final NavigableMap<PopCheckPoint/* oldCK */, Pair<Long/* timestamp */, Boolean/* result */>> inflightReviveRequestMap = Collections.synchronizedNavigableMap(new TreeMap<>());
    private long reviveOffset;

    public PopReviveService(Broker broker, String reviveTopic, int queueId) {
        this.queueId = queueId;
        this.broker = broker;
        this.reviveTopic = reviveTopic;
        this.reviveOffset = broker.getConsumerOffsetManager().queryOffset(PopConstants.REVIVE_GROUP, reviveTopic, queueId);
    }

    @Override
    public void run() {
        int slow = 1;
        while (!this.isStopped()) {
            try {
                if (!shouldRun()) continue;

                POP_LOGGER.info("start revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                ConsumeReviveObj consumeReviveObj = consumeReviveMessage();

                if (!shouldRunPopRevive) {
                    POP_LOGGER.info("slave skip scan, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                    continue;
                }

                mergeAndRevive(consumeReviveObj);

                slow = calculateSlow(slow, consumeReviveObj);
            } catch (Throwable e) {
                POP_LOGGER.error("reviveQueueId={}, revive error", queueId, e);
            }
        }
    }

    private boolean shouldRun() {
        if (System.currentTimeMillis() < broker.getShouldStartTime()) {
            POP_LOGGER.info("PopReviveService Ready to run after {}", broker.getShouldStartTime());
            this.waitForRunning(1000);
            return false;
        }
        this.waitForRunning(broker.getBrokerConfig().getReviveInterval());
        if (!shouldRunPopRevive) {
            POP_LOGGER.info("skip start revive topic={}, reviveQueueId={}", reviveTopic, queueId);
            return false;
        }

        if (!broker.getMessageStore().getMessageStoreConfig().isTimerWheelEnable()) {
            POP_LOGGER.warn("skip revive topic because timerWheelEnable is false");
            return false;
        }

        return true;
    }

    private int calculateSlow(int slow, ConsumeReviveObj consumeReviveObj) {
        ArrayList<PopCheckPoint> sortList = consumeReviveObj.getSortList();
        long delay = 0;
        if (sortList != null && !sortList.isEmpty()) {
            delay = (System.currentTimeMillis() - sortList.get(0).getReviveTime()) / 1000;
            currentReviveMessageTimestamp = sortList.get(0).getReviveTime();
            slow = 1;
        } else {
            currentReviveMessageTimestamp = System.currentTimeMillis();
        }

        POP_LOGGER.info("reviveQueueId={}, revive finish,old offset is {}, new offset is {}, ckDelay={}  ",
            queueId, consumeReviveObj.getOldOffset(), consumeReviveObj.getNewOffset(), delay);

        if (sortList == null || sortList.isEmpty()) {
            POP_LOGGER.info("reviveQueueId={}, has no new msg, take a rest {}", queueId, slow);
            this.waitForRunning(slow * broker.getBrokerConfig().getReviveInterval());
            if (slow < broker.getBrokerConfig().getReviveMaxSlow()) {
                slow++;
            }
        }

        return slow;
    }

    @Override
    public String getServiceName() {
        if (broker != null && broker.getBrokerConfig().isInBrokerContainer()) {
            return broker.getBrokerIdentity().getIdentifier() + "PopReviveService_" + this.queueId;
        }
        return "PopReviveService_" + this.queueId;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setShouldRunPopRevive(final boolean shouldRunPopRevive) {
        this.shouldRunPopRevive = shouldRunPopRevive;
    }

    public boolean isShouldRunPopRevive() {
        return shouldRunPopRevive;
    }

    private void initMsgTopic(PopCheckPoint popCheckPoint, MessageExtBrokerInner msgInner) {
        if (!popCheckPoint.getTopic().startsWith(MQConstants.RETRY_GROUP_TOPIC_PREFIX)) {
            msgInner.setTopic(KeyBuilder.buildPopRetryTopic(popCheckPoint.getTopic(), popCheckPoint.getCId(), broker.getBrokerConfig().isEnableRetryTopicV2()));
        } else {
            msgInner.setTopic(popCheckPoint.getTopic());
        }
    }

    private void initMsgTag(MessageExt messageExt, MessageExtBrokerInner msgInner) {
        if (messageExt.getTags() != null) {
            msgInner.setTags(messageExt.getTags());
        } else {
            MessageAccessor.setProperties(msgInner, new HashMap<>());
        }
    }

    private void initMsgProperties(PopCheckPoint popCheckPoint, MessageExt messageExt, MessageExtBrokerInner msgInner) {
        if (messageExt.getReconsumeTimes() == 0 || msgInner.getProperties().get(MessageConst.PROPERTY_FIRST_POP_TIME) == null) {
            msgInner.getProperties().put(MessageConst.PROPERTY_FIRST_POP_TIME, String.valueOf(popCheckPoint.getPopTime()));
        }
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
    }

    private MessageExtBrokerInner createMessageExtBrokerInner(PopCheckPoint popCheckPoint, MessageExt messageExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        initMsgTopic(popCheckPoint, msgInner);
        initMsgTag(messageExt, msgInner);

        msgInner.setBody(messageExt.getBody());
        msgInner.setQueueId(0);
        msgInner.setBornTimestamp(messageExt.getBornTimestamp());
        msgInner.setFlag(messageExt.getFlag());
        msgInner.setSysFlag(messageExt.getSysFlag());
        msgInner.setBornHost(broker.getStoreHost());
        msgInner.setStoreHost(broker.getStoreHost());
        msgInner.setReconsumeTimes(messageExt.getReconsumeTimes() + 1);
        msgInner.getProperties().putAll(messageExt.getProperties());

        initMsgProperties(popCheckPoint, messageExt, msgInner);

        return msgInner;
    }

    private boolean reviveRetry(PopCheckPoint popCheckPoint, MessageExt messageExt) {
        MessageExtBrokerInner msgInner = createMessageExtBrokerInner(popCheckPoint, messageExt);

        addRetryTopicIfNoExit(msgInner.getTopic(), popCheckPoint.getCId());
        PutMessageResult putMessageResult = broker.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        incRetryCount(popCheckPoint, messageExt, putMessageResult);

        if (putMessageResult.getAppendMessageResult() == null ||
            putMessageResult.getAppendMessageResult().getStatus() != AppendMessageStatus.PUT_OK) {
            POP_LOGGER.error("reviveQueueId={}, revive error, msg is: {}", queueId, msgInner);
            return false;
        }

        incRetryMatrix(popCheckPoint, msgInner, putMessageResult);
        return true;
    }

    private void incRetryCount(PopCheckPoint popCheckPoint, MessageExt messageExt, PutMessageResult putMessageResult) {
        PopMetricsManager.incPopReviveRetryMessageCount(popCheckPoint, putMessageResult.getPutMessageStatus());
        if (!broker.getBrokerConfig().isEnablePopLog()) {
            return;
        }

        POP_LOGGER.info("reviveQueueId={},retry msg, ck={}, msg queueId {}, offset {}, reviveDelay={}, result is {} ",
            queueId, popCheckPoint, messageExt.getQueueId(), messageExt.getQueueOffset(),
            (System.currentTimeMillis() - popCheckPoint.getReviveTime()) / 1000, putMessageResult);
    }

    private void incRetryMatrix(PopCheckPoint popCheckPoint, MessageExtBrokerInner msgInner, PutMessageResult putMessageResult) {
        this.broker.getPopInflightMessageCounter().decrementInFlightMessageNum(popCheckPoint);
        this.broker.getBrokerStatsManager().incBrokerPutNums(popCheckPoint.getTopic(), 1);
        this.broker.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
        this.broker.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(), putMessageResult.getAppendMessageResult().getWroteBytes());
    }

    private void initPopRetryOffset(String topic, String consumerGroup) {
        long offset = this.broker.getConsumerOffsetManager().queryOffset(consumerGroup, topic, 0);
        if (offset >= 0) {
            return;
        }

        this.broker.getConsumerOffsetManager().commitOffset("initPopRetryOffset", consumerGroup, topic, 0, 0);
    }

    private void addRetryTopicIfNoExit(String topic, String consumerGroup) {
        if (broker == null) {
            return;
        }

        TopicConfig topicConfig = broker.getTopicConfigManager().selectTopicConfig(topic);
        if (topicConfig != null) {
            return;
        }
        topicConfig = new TopicConfig(topic);
        topicConfig.setReadQueueNums(PopConstants.retryQueueNum);
        topicConfig.setWriteQueueNums(PopConstants.retryQueueNum);
        topicConfig.setTopicFilterType(TopicFilterType.SINGLE_TAG);
        topicConfig.setPerm(6);
        topicConfig.setTopicSysFlag(0);
        broker.getTopicConfigManager().updateTopicConfig(topicConfig);

        initPopRetryOffset(topic, consumerGroup);
    }

    protected List<MessageExt> getReviveMessage(long offset, int queueId) {
        PullResult pullResult = getMessage(PopConstants.REVIVE_GROUP, reviveTopic, queueId, offset, 32, true);
        if (pullResult == null) {
            return null;
        }

        if (reachTail(pullResult, offset)) {
            if (this.broker.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("reviveQueueId={}, reach tail,offset {}", queueId, offset);
            }
        } else if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            POP_LOGGER.error("reviveQueueId={}, OFFSET_ILLEGAL {}, result is {}", queueId, offset, pullResult);
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip offset correct topic={}, reviveQueueId={}", reviveTopic, queueId);
                return null;
            }

            this.broker.getConsumerOffsetManager().commitOffset(PopConstants.LOCAL_HOST, PopConstants.REVIVE_GROUP, reviveTopic, queueId, pullResult.getNextBeginOffset() - 1);
        }
        return pullResult.getMsgFoundList();
    }

    private boolean reachTail(PullResult pullResult, long offset) {
        return pullResult.getPullStatus() == PullStatus.NO_NEW_MSG
            || pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL && offset == pullResult.getMaxOffset();
    }

    /**
     * @renamed from getBizMessage to getMessageAsync
     */
    private CompletableFuture<Pair<GetMessageStatus, MessageExt>> getMessageAsync(String topic, long offset, int queueId, String brokerName) {
        return this.broker.getEscapeBridge().getMessageAsync(topic, offset, queueId, brokerName, false);
    }

    private PullResult getMessage(String group, String topic, int queueId, long offset, int nums, boolean deCompressBody) {
        GetMessageResult getMessageResult = this.broker.getMessageStore().getMessage(group, topic, queueId, offset, nums, null);

        if (getMessageResult == null) {
            return formatNullResult(group, topic, offset);
        }

        return formatGetResult(group, topic, offset, deCompressBody, getMessageResult);
    }

    private PullResult formatNullResult(String group, String topic, long offset) {
        long maxQueueOffset = broker.getMessageStore().getMaxOffsetInQueue(topic, queueId);
        if (maxQueueOffset > offset) {
            POP_LOGGER.error("get message from store return null. topic={}, groupId={}, requestOffset={}, maxQueueOffset={}", topic, group, offset, maxQueueOffset);
        }
        return null;
    }

    private PullResult formatGetResult(String group, String topic, long offset, boolean deCompressBody, GetMessageResult getMessageResult) {
        PullStatus pullStatus = PullStatus.NO_NEW_MSG;
        List<MessageExt> foundList = null;
        switch (getMessageResult.getStatus()) {
            case FOUND:
                pullStatus = PullStatus.FOUND;
                foundList = decodeMsgList(getMessageResult, deCompressBody);
                incGetMessageMatrix(group, topic, getMessageResult, foundList);

                break;
            case NO_MATCHED_MESSAGE:
                pullStatus = PullStatus.NO_MATCHED_MSG;
                POP_LOGGER.debug("no matched message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}", getMessageResult.getStatus(), topic, group, offset);
                break;
            case NO_MESSAGE_IN_QUEUE:
                POP_LOGGER.debug("no new message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}", getMessageResult.getStatus(), topic, group, offset);
                break;
            case MESSAGE_WAS_REMOVING:
            case NO_MATCHED_LOGIC_QUEUE:
            case OFFSET_FOUND_NULL:
            case OFFSET_OVERFLOW_BADLY:
            case OFFSET_TOO_SMALL:
                pullStatus = PullStatus.OFFSET_ILLEGAL;
                POP_LOGGER.warn("offset illegal. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}", getMessageResult.getStatus(), topic, group, offset);
                break;
            case OFFSET_OVERFLOW_ONE:
                // no need to print WARN, because we use "offset + 1" to get the next message
                pullStatus = PullStatus.OFFSET_ILLEGAL;
                break;
            default:
                assert false;
                break;
        }

        return new PullResult(pullStatus, getMessageResult.getNextBeginOffset(), getMessageResult.getMinOffset(), getMessageResult.getMaxOffset(), foundList);
    }

    private void incGetMessageMatrix(String group, String topic, GetMessageResult getMessageResult, List<MessageExt> foundList) {
        broker.getBrokerStatsManager().incGroupGetNums(group, topic, getMessageResult.getMessageCount());
        broker.getBrokerStatsManager().incGroupGetSize(group, topic, getMessageResult.getBufferTotalSize());
        broker.getBrokerStatsManager().incBrokerGetNums(topic, getMessageResult.getMessageCount());
        broker.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId, broker.getMessageStore().now() - foundList.get(foundList.size() - 1).getStoreTimestamp());

        Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
            .put(LABEL_TOPIC, topic)
            .put(LABEL_CONSUMER_GROUP, group)
            .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(topic) || MQConstants.isSysConsumerGroup(group))
            .build();
        BrokerMetricsManager.messagesOutTotal.add(getMessageResult.getMessageCount(), attributes);
        BrokerMetricsManager.throughputOutTotal.add(getMessageResult.getBufferTotalSize(), attributes);
    }

    private List<MessageExt> decodeMsgList(GetMessageResult getMessageResult, boolean deCompressBody) {
        List<MessageExt> foundList = new ArrayList<>();
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            if (messageBufferList == null) {
                return foundList;
            }

            for (int i = 0; i < messageBufferList.size(); i++) {
                ByteBuffer bb = messageBufferList.get(i);
                if (bb == null) {
                    POP_LOGGER.error("bb is null {}", getMessageResult);
                    continue;
                }
                MessageExt msgExt = MessageDecoder.decode(bb, true, deCompressBody);
                if (msgExt == null) {
                    POP_LOGGER.error("decode msgExt is null {}", getMessageResult);
                    continue;
                }
                // use CQ offset, not offset in Message
                msgExt.setQueueOffset(getMessageResult.getMessageQueueOffset().get(i));
                foundList.add(msgExt);
            }
        } finally {
            getMessageResult.release();
        }

        return foundList;
    }

    protected ConsumeReviveObj consumeReviveMessage() {
        long consumeOffset = this.broker.getConsumerOffsetManager().queryOffset(PopConstants.REVIVE_GROUP, reviveTopic, queueId);
        ReviveContext context = new ReviveContext(consumeOffset, reviveOffset);
        POP_LOGGER.info("reviveQueueId={}, old offset is {} ", queueId, context.getOldOffset());

        while (true) {
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip scan, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                break;
            }

            List<MessageExt> messageList = getReviveMessage(context.getOffset(), queueId);
            if (messageList == null || messageList.isEmpty()) {
                if (!handleEmptyReviveMessage(context)) {
                    break;
                }
                continue;
            }

            context.setNoMsgCount(0);
            if (System.currentTimeMillis() - context.getStartTime() > broker.getBrokerConfig().getReviveScanTime()) {
                POP_LOGGER.info("reviveQueueId={}, scan timeout ", queueId);
                break;
            }

            parseReviveMessages(context, messageList);
            context.setOffset(context.getOffset() + messageList.size());
        }

        context.getConsumeReviveObj().getMap().putAll(context.getMockPointMap());
        context.getConsumeReviveObj().setEndTime(context.getEndTime());

        return context.getConsumeReviveObj();
    }

    private boolean handleEmptyReviveMessage(ReviveContext context) {
        long old = context.getEndTime();
        long timerDelay = broker.getMessageStore().getTimerMessageStore().getTimerState().getDequeueBehind();
        long commitLogDelay = broker.getMessageStore().getTimerMessageStore().getEnqueueBehind();
        // move endTime
        long now = System.currentTimeMillis();
        if (context.getEndTime() != 0 &&  now - context.getEndTime() > 3 * PopConstants.SECOND && timerDelay <= 0 && commitLogDelay <= 0) {
            context.setEndTime(now);
        }
        POP_LOGGER.info("reviveQueueId={}, offset is {}, can not get new msg, old endTime {}, new endTime {}, timerDelay={}, commitLogDelay={} ",
            queueId, context.getOffset(), old, context.getEndTime(), timerDelay, commitLogDelay);
        if (context.getEndTime() - context.getFirstRt() > PopConstants.ackTimeInterval + PopConstants.SECOND) {
            return false;
        }
        context.increaseNoMsgCount();
        // Fixme: why sleep is useful here?
        ThreadUtils.sleep(100);
        return context.getNoMsgCount() * 100L <= 4 * PopConstants.SECOND;
    }

    private void parseReviveMessages(ReviveContext context, List<MessageExt> messageExts) {
        for (MessageExt messageExt : messageExts) {

            if (PopConstants.CK_TAG.equals(messageExt.getTags())) {
                if (!parseCheckPointMessage(context, messageExt)) continue;
            } else if (PopConstants.ACK_TAG.equals(messageExt.getTags())) {
                if (!parseAckMessage(context, messageExt)) continue;
            } else if (PopConstants.BATCH_ACK_TAG.equals(messageExt.getTags())) {
                if (!parseBatchAckMessage(context, messageExt)) continue;
            }

            long deliverTime = messageExt.getDeliverTimeMs();
            if (deliverTime > context.getEndTime()) {
                context.setEndTime(deliverTime);
            }
        }
    }

    private boolean parseCheckPointMessage(ReviveContext context, MessageExt messageExt) {
        String raw = new String(messageExt.getBody(), DataConverter.CHARSET_UTF8);
        if (broker.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("reviveQueueId={},find ck, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
        }
        PopCheckPoint point = JSON.parseObject(raw, PopCheckPoint.class);
        if (point.getTopic() == null || point.getCId() == null) {
            return false;
        }
        context.getMap().put(PopKeyBuilder.buildReviveKey(point), point);
        PopMetricsManager.incPopReviveCkGetCount(point, queueId);
        point.setReviveOffset(messageExt.getQueueOffset());
        if (context.getFirstRt() == 0) {
            context.setFirstRt(point.getReviveTime());
        }

        return true;
    }

    private boolean parseAckMessage(ReviveContext context, MessageExt messageExt) {
        String raw = new String(messageExt.getBody(), DataConverter.CHARSET_UTF8);
        if (broker.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("reviveQueueId={}, find ack, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
        }
        AckMsg ackMsg = JSON.parseObject(raw, AckMsg.class);
        PopMetricsManager.incPopReviveAckGetCount(ackMsg, queueId);
        String mergeKey = PopKeyBuilder.buildReviveKey(ackMsg);
        PopCheckPoint point = context.getMap().get(mergeKey);
        if (point == null) {
            if (!broker.getBrokerConfig().isEnableSkipLongAwaitingAck()) {
                return false;
            }
            if (mockCkForAck(messageExt, ackMsg, mergeKey, context.getMockPointMap()) && context.getFirstRt() == 0) {
                context.setFirstRt(context.getMockPointMap().get(mergeKey).getReviveTime());
            }
            return true;
        }

        int indexOfAck = point.indexOfAck(ackMsg.getAckOffset());
        if (indexOfAck > -1) {
            point.setBitMap(DataConverter.setBit(point.getBitMap(), indexOfAck, true));
        } else {
            POP_LOGGER.error("invalid ack index, {}, {}", ackMsg, point);
        }
        return true;
    }

    private boolean parseBatchAckMessage(ReviveContext context, MessageExt messageExt) {
        String raw = new String(messageExt.getBody(), DataConverter.CHARSET_UTF8);
        if (broker.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("reviveQueueId={}, find batch ack, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
        }

        BatchAckMsg bAckMsg = JSON.parseObject(raw, BatchAckMsg.class);
        PopMetricsManager.incPopReviveAckGetCount(bAckMsg, queueId);
        String mergeKey = PopKeyBuilder.buildReviveKey(bAckMsg);
        PopCheckPoint point = context.getMap().get(mergeKey);
        if (point == null) {
            if (!broker.getBrokerConfig().isEnableSkipLongAwaitingAck()) {
                return false;
            }
            if (mockCkForAck(messageExt, bAckMsg, mergeKey, context.getMockPointMap()) && context.getFirstRt() == 0) {
                context.setFirstRt(context.getMockPointMap().get(mergeKey).getReviveTime());
            }
            return true;
        }

        List<Long> ackOffsetList = bAckMsg.getAckOffsetList();
        for (Long ackOffset : ackOffsetList) {
            int indexOfAck = point.indexOfAck(ackOffset);
            if (indexOfAck > -1) {
                point.setBitMap(DataConverter.setBit(point.getBitMap(), indexOfAck, true));
            } else {
                POP_LOGGER.error("invalid batch ack index, {}, {}", bAckMsg, point);
            }
        }

        return true;
    }

    private boolean mockCkForAck(MessageExt messageExt, AckMsg ackMsg, String mergeKey, HashMap<String, PopCheckPoint> mockPointMap) {
        long ackWaitTime = System.currentTimeMillis() - messageExt.getDeliverTimeMs();
        long reviveAckWaitMs = broker.getBrokerConfig().getReviveAckWaitMs();
        if (ackWaitTime <= reviveAckWaitMs) {
            return false;
        }

        // will use the reviveOffset of popCheckPoint to commit offset in mergeAndRevive
        PopCheckPoint mockPoint = createMockCkForAck(ackMsg, messageExt.getQueueOffset());
        POP_LOGGER.warn("ack wait for {}ms cannot find ck, skip this ack. mergeKey:{}, ack:{}, mockCk:{}", reviveAckWaitMs, mergeKey, ackMsg, mockPoint);
        mockPointMap.put(mergeKey, mockPoint);
        return true;
    }

    private PopCheckPoint createMockCkForAck(AckMsg ackMsg, long offset) {
        PopCheckPoint point = new PopCheckPoint();
        point.setStartOffset(ackMsg.getStartOffset());
        point.setPopTime(ackMsg.getPopTime());
        point.setQueueId(ackMsg.getQueueId());
        point.setCId(ackMsg.getConsumerGroup());
        point.setTopic(ackMsg.getTopic());
        point.setNum((byte) 0);
        point.setBitMap(0);
        point.setReviveOffset(offset);
        point.setBrokerName(ackMsg.getBrokerName());
        return point;
    }

    protected void mergeAndRevive(ConsumeReviveObj consumeReviveObj) {
        ArrayList<PopCheckPoint> sortList = consumeReviveObj.genSortList();
        logMergeAndRevive(sortList);

        long newOffset = consumeReviveObj.getOldOffset();
        for (PopCheckPoint popCheckPoint : sortList) {
            if (shouldBreakRevive(consumeReviveObj, popCheckPoint)) break;

            // check normal topic, skip ck , if normal topic is not exist
            String topic = KeyBuilder.removeRetryPrefix(popCheckPoint.getTopic(), popCheckPoint.getCId());
            if (broker.getTopicConfigManager().selectTopicConfig(topic) == null) {
                POP_LOGGER.warn("reviveQueueId={}, can not get normal topic {}, then continue", queueId, popCheckPoint.getTopic());
                newOffset = popCheckPoint.getReviveOffset();
                continue;
            }

            if (null == broker.getSubscriptionGroupManager().findSubscriptionGroupConfig(popCheckPoint.getCId())) {
                POP_LOGGER.warn("reviveQueueId={}, can not get cid {}, then continue", queueId, popCheckPoint.getCId());
                newOffset = popCheckPoint.getReviveOffset();
                continue;
            }

            reputExpiredCheckPoint();
            reviveMsgFromCk(popCheckPoint);

            newOffset = popCheckPoint.getReviveOffset();
        }

        resetReviveOffset(consumeReviveObj, newOffset);
    }

    private void logMergeAndRevive(ArrayList<PopCheckPoint> sortList) {
        POP_LOGGER.info("reviveQueueId={}, ck listSize={}", queueId, sortList.size());
        if (sortList.size() == 0) {
            return;
        }

        POP_LOGGER.info("reviveQueueId={}, 1st ck, startOffset={}, reviveOffset={}; last ck, startOffset={}, reviveOffset={}", queueId, sortList.get(0).getStartOffset(),
            sortList.get(0).getReviveOffset(), sortList.get(sortList.size() - 1).getStartOffset(), sortList.get(sortList.size() - 1).getReviveOffset());
    }

    private boolean shouldBreakRevive(ConsumeReviveObj consumeReviveObj, PopCheckPoint popCheckPoint) {
        if (!shouldRunPopRevive) {
            POP_LOGGER.info("slave skip ck process, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
            return true;
        }

        return consumeReviveObj.getEndTime() - popCheckPoint.getReviveTime() <= (PopConstants.ackTimeInterval + PopConstants.SECOND);
    }

    /**
     * reput timeout check point to revive queue
     */
    private void reputExpiredCheckPoint() {
        while (inflightReviveRequestMap.size() > 3) {
            waitForRunning(100);

            Pair<Long, Boolean> pair = inflightReviveRequestMap.firstEntry().getValue();
            // if check point result is true, continue
            if (pair.getObject2()) {
                continue;
            }

            // if check point is not timeout, continue
            if (System.currentTimeMillis() - pair.getObject1() <= 1000 * 30) {
                continue;
            }

            PopCheckPoint oldCK = inflightReviveRequestMap.firstKey();
            rePutCK(oldCK, pair);
            inflightReviveRequestMap.remove(oldCK);
        }
    }

    private void reviveMsgFromCk(PopCheckPoint popCheckPoint) {
        if (!shouldRunPopRevive) {
            POP_LOGGER.info("slave skip retry, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
            return;
        }

        inflightReviveRequestMap.put(popCheckPoint, new Pair<>(System.currentTimeMillis(), false));
        List<CompletableFuture<Pair<Long, Boolean>>> futureList = getMessageFuture(popCheckPoint);

        reviveAndCleanInflightMap(futureList, popCheckPoint);
    }

    private List<CompletableFuture<Pair<Long, Boolean>>> getMessageFuture(PopCheckPoint popCheckPoint) {
        List<CompletableFuture<Pair<Long, Boolean>>> futureList = new ArrayList<>(popCheckPoint.getNum());

        for (int j = 0; j < popCheckPoint.getNum(); j++) {
            // if the message has been ack, continue
            if (DataConverter.getBit(popCheckPoint.getBitMap(), j)) {
                continue;
            }

            // retry msg
            long msgOffset = popCheckPoint.ackOffsetByIndex((byte) j);
            CompletableFuture<Pair<Long, Boolean>> future = getMessageAsync(
                popCheckPoint.getTopic(), msgOffset, popCheckPoint.getQueueId(), popCheckPoint.getBrokerName()
            ).thenApply(createCallback(popCheckPoint, msgOffset));

            futureList.add(future);
        }

        return futureList;
    }

    private Function<Pair<GetMessageStatus, MessageExt>, Pair<Long, Boolean>> createCallback(PopCheckPoint popCheckPoint, long msgOffset) {
        return resultPair -> {
            GetMessageStatus getMessageStatus = resultPair.getObject1();
            MessageExt message = resultPair.getObject2();
            if (message == null) {
                POP_LOGGER.warn("reviveQueueId={}, can not get biz msg topic is {}, offset is {}, then continue", queueId, popCheckPoint.getTopic(), msgOffset);
                switch (getMessageStatus) {
                    case MESSAGE_WAS_REMOVING:
                    case OFFSET_TOO_SMALL:
                    case NO_MATCHED_LOGIC_QUEUE:
                    case NO_MESSAGE_IN_QUEUE:
                        return new Pair<>(msgOffset, true);
                    default:
                        return new Pair<>(msgOffset, false);

                }
            }
            //skip ck from last epoch
            if (popCheckPoint.getPopTime() < message.getStoreTimestamp()) {
                POP_LOGGER.warn("reviveQueueId={}, skip ck from last epoch {}", queueId, popCheckPoint);
                return new Pair<>(msgOffset, true);
            }

            boolean result = reviveRetry(popCheckPoint, message);
            return new Pair<>(msgOffset, result);
        };
    }

    private void reviveAndCleanInflightMap(List<CompletableFuture<Pair<Long, Boolean>>> futureList, PopCheckPoint popCheckPoint) {
        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).whenComplete((v, e) -> {
            for (CompletableFuture<Pair<Long, Boolean>> future : futureList) {
                Pair<Long, Boolean> pair = future.getNow(new Pair<>(0L, false));
                // reput expired message to revive queue
                if (!pair.getObject2()) {
                    rePutCK(popCheckPoint, pair);
                }
            }

            // remove check point from inflight revive request map
            if (inflightReviveRequestMap.containsKey(popCheckPoint)) {
                inflightReviveRequestMap.get(popCheckPoint).setObject2(true);
            }

            cleanInflightMap();
        });
    }

    /**
     * delete finished check point in the inflight map,
     * until meet one unfinished
     */
    private void cleanInflightMap() {
        for (Map.Entry<PopCheckPoint, Pair<Long, Boolean>> entry : inflightReviveRequestMap.entrySet()) {
            Pair<Long, Boolean> pair = entry.getValue();
            if (pair.getObject2()) {
                PopCheckPoint oldCK = entry.getKey();
                broker.getConsumerOffsetManager().commitOffset(PopConstants.LOCAL_HOST, PopConstants.REVIVE_GROUP, reviveTopic, queueId, oldCK.getReviveOffset());
                inflightReviveRequestMap.remove(oldCK);
            } else {
                break;
            }
        }
    }

    private void resetReviveOffset(ConsumeReviveObj consumeReviveObj, long newOffset) {
        if (newOffset > consumeReviveObj.getOldOffset()) {
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip commit, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                return;
            }
            this.broker.getConsumerOffsetManager().commitOffset(PopConstants.LOCAL_HOST, PopConstants.REVIVE_GROUP, reviveTopic, queueId, newOffset);
        }

        reviveOffset = newOffset;
        consumeReviveObj.setNewOffset(newOffset);
    }

    private void rePutCK(PopCheckPoint oldCK, Pair<Long, Boolean> pair) {
        PopCheckPoint newCk = new PopCheckPoint();
        newCk.setBitMap(0);
        newCk.setNum((byte) 1);
        newCk.setPopTime(oldCK.getPopTime());
        newCk.setInvisibleTime(oldCK.getInvisibleTime());
        newCk.setStartOffset(pair.getObject1());
        newCk.setCId(oldCK.getCId());
        newCk.setTopic(oldCK.getTopic());
        newCk.setQueueId(oldCK.getQueueId());
        newCk.setBrokerName(oldCK.getBrokerName());
        newCk.addDiff(0);
        MessageExtBrokerInner ckMsg = broker.getBrokerNettyServer().getPopServiceManager().buildCkMsg(newCk, queueId);
        broker.getMessageStore().putMessage(ckMsg);
    }

    public long getReviveBehindMillis() {
        if (currentReviveMessageTimestamp <= 0) {
            return 0;
        }
        long maxOffset = broker.getMessageStore().getMaxOffsetInQueue(reviveTopic, queueId);
        if (maxOffset - reviveOffset > 1) {
            return Math.max(0, System.currentTimeMillis() - currentReviveMessageTimestamp);
        }
        return 0;
    }

    public long getReviveBehindMessages() {
        if (currentReviveMessageTimestamp <= 0) {
            return 0;
        }
        long diff = broker.getMessageStore().getMaxOffsetInQueue(reviveTopic, queueId) - reviveOffset;
        return Math.max(0, diff);
    }

}
