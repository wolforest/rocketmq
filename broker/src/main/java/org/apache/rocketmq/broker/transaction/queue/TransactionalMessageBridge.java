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
package org.apache.rocketmq.broker.transaction.queue;

import io.opentelemetry.api.common.Attributes;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.constant.MQConstants;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;

/**
 * Facade of BrokerController for Transactional message
 */
public class TransactionalMessageBridge {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private final ConcurrentHashMap<Integer, MessageQueue> opQueueMap = new ConcurrentHashMap<>();
    private final BrokerController brokerController;
    private final MessageStore store;
    private final SocketAddress storeHost;

    public TransactionalMessageBridge(BrokerController brokerController, MessageStore store) {
        try {
            this.brokerController = brokerController;
            this.store = store;
            this.storeHost =
                new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(),
                    brokerController.getNettyServerConfig().getListenPort());
        } catch (Exception e) {
            LOGGER.error("Init TransactionBridge error", e);
            throw new RuntimeException(e);
        }
    }

    public long fetchConsumeOffset(MessageQueue mq) {
        long offset = brokerController.getConsumerOffsetManager().queryOffset(TransactionalMessageUtil.buildConsumerGroup(),
            mq.getTopic(), mq.getQueueId());
        if (offset == -1) {
            offset = store.getMinOffsetInQueue(mq.getTopic(), mq.getQueueId());
        }
        return offset;
    }

    public Set<MessageQueue> fetchMessageQueues(String topic) {
        Set<MessageQueue> mqSet = new HashSet<>();
        TopicConfig topicConfig = selectTopicConfig(topic);
        if (topicConfig == null || topicConfig.getReadQueueNums() <= 0) {
            return mqSet;
        }

        for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
            MessageQueue mq = new MessageQueue();
            mq.setTopic(topic);
            mq.setBrokerName(brokerController.getBrokerConfig().getBrokerName());
            mq.setQueueId(i);
            mqSet.add(mq);
        }
        return mqSet;
    }

    public void updateConsumeOffset(MessageQueue mq, long offset) {
        this.brokerController.getConsumerOffsetManager().commitOffset(
            RemotingHelper.parseSocketAddressAddr(this.storeHost), TransactionalMessageUtil.buildConsumerGroup(), mq.getTopic(),
            mq.getQueueId(), offset);
    }

    public PullResult getHalfMessage(int queueId, long offset, int nums) {
        String group = TransactionalMessageUtil.buildConsumerGroup();
        String topic = TransactionalMessageUtil.buildHalfTopic();
        SubscriptionData sub = new SubscriptionData(topic, "*");
        return getMessage(group, topic, queueId, offset, nums, sub);
    }

    /**
     * get message from MessageStore(DefaultMessageStore)
     * @param queueId queueId
     * @param offset offset
     * @param nums nums
     * @return PullResult
     */
    public PullResult getOpMessage(int queueId, long offset, int nums) {
        String group = TransactionalMessageUtil.buildConsumerGroup();
        String topic = TransactionalMessageUtil.buildOpTopic();
        SubscriptionData sub = new SubscriptionData(topic, "*");
        return getMessage(group, topic, queueId, offset, nums, sub);
    }

    public PutMessageResult putHalfMessage(MessageExtBrokerInner messageInner) {
        return store.putMessage(parseHalfMessageInner(messageInner));
    }

    public CompletableFuture<PutMessageResult> asyncPutHalfMessage(MessageExtBrokerInner messageInner) {
        return store.asyncPutMessage(parseHalfMessageInner(messageInner));
    }

    public PutMessageResult putMessageReturnResult(MessageExtBrokerInner messageInner) {
        LOGGER.debug("[BUG-TO-FIX] Thread:{} msgID:{}", Thread.currentThread().getName(), messageInner.getMsgId());
        PutMessageResult result = store.putMessage(messageInner);

        if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            BrokerStatsManager statsManager = this.brokerController.getBrokerStatsManager();
            statsManager.incTopicPutNums(messageInner.getTopic());
            statsManager.incTopicPutSize(messageInner.getTopic(), result.getAppendMessageResult().getWroteBytes());
            statsManager.incBrokerPutNums();
        }
        return result;
    }

    public boolean putMessage(MessageExtBrokerInner messageInner) {
        PutMessageResult putMessageResult = store.putMessage(messageInner);

        if (putMessageResult != null && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            return true;
        }

        LOGGER.error("Put message failed, topic: {}, queueId: {}, msgId: {}", messageInner.getTopic(), messageInner.getQueueId(), messageInner.getMsgId());
        return false;
    }

    public MessageExtBrokerInner renewImmunityHalfMessageInner(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = renewHalfMessageInner(msgExt);
        String queueOffsetFromPrepare = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        if (null != queueOffsetFromPrepare) {
            MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
                queueOffsetFromPrepare);
        } else {
            MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
                String.valueOf(msgExt.getQueueOffset()));
        }

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        return msgInner;
    }

    public MessageExtBrokerInner renewHalfMessageInner(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(msgExt.getTopic());
        msgInner.setBody(msgExt.getBody());
        msgInner.setQueueId(msgExt.getQueueId());
        msgInner.setMsgId(msgExt.getMsgId());
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setTags(msgExt.getTags());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgInner.getTags()));
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setWaitStoreMsgOK(false);
        return msgInner;
    }

    private MessageQueue getOpQueue(Integer queueId) {
        MessageQueue opQueue = opQueueMap.get(queueId);
        if (opQueue != null) {
            return opQueue;
        }

        opQueue = getOpQueueByHalf(queueId, this.brokerController.getBrokerConfig().getBrokerName());
        MessageQueue oldQueue = opQueueMap.putIfAbsent(queueId, opQueue);
        if (oldQueue != null) {
            opQueue = oldQueue;
        }

        return opQueue;
    }

    public boolean writeOp(Integer queueId,Message message) {
        MessageQueue opQueue = getOpQueue(queueId);

        PutMessageResult result = putMessageReturnResult(makeOpMessageInner(message, opQueue));
        if (result == null) {
            return false;
        }

        return result.getPutMessageStatus() == PutMessageStatus.PUT_OK;
    }

    public MessageExt lookMessageByOffset(final long commitLogOffset) {
        return this.store.lookMessageByOffset(commitLogOffset);
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }

    public boolean escapeMessage(MessageExtBrokerInner messageInner) {
        PutMessageResult putMessageResult = this.brokerController.getEscapeBridge().putMessage(messageInner);
        if (putMessageResult != null && putMessageResult.isOk()) {
            return true;
        } else {
            LOGGER.error("Escaping message failed, topic: {}, queueId: {}, msgId: {}",
                messageInner.getTopic(), messageInner.getQueueId(), messageInner.getMsgId());
            return false;
        }
    }

    private PullResult getMessage(String group, String topic, int queueId, long offset, int nums, SubscriptionData sub) {
        GetMessageResult getMessageResult = store.getMessage(group, topic, queueId, offset, nums, null);
        if (null == getMessageResult) {
            LOGGER.error("Get message from store return null. topic={}, groupId={}, requestOffset={}", topic, group, offset);
            return null;
        }

        GetMessageContext context = new GetMessageContext(getMessageResult, group, topic, queueId, offset);
        switch (getMessageResult.getStatus()) {
            case FOUND:
                parseFoundStatus(context);
                break;
            case NO_MATCHED_MESSAGE:
                parseNoMatchMessage(context);
                break;
            case NO_MESSAGE_IN_QUEUE:
            case OFFSET_OVERFLOW_ONE:
                parseOffsetOverFlowOne(context);
                break;
            case MESSAGE_WAS_REMOVING:
            case NO_MATCHED_LOGIC_QUEUE:
            case OFFSET_FOUND_NULL:
            case OFFSET_OVERFLOW_BADLY:
            case OFFSET_TOO_SMALL:
                parseOffsetTooSmall(context);
                break;
            default:
                assert false;
                break;
        }

        return new PullResult(context.getPullStatus(), getMessageResult.getNextBeginOffset(), getMessageResult.getMinOffset(),
            getMessageResult.getMaxOffset(), context.getFoundList());
    }

    private void parseOffsetTooSmall(GetMessageContext context) {
        context.setPullStatus(PullStatus.OFFSET_ILLEGAL);
        LOGGER.warn("Offset illegal. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
            context.getGetMessageResult().getStatus(), context.getTopic(), context.getGroup(), context.getOffset());
    }

    private void parseOffsetOverFlowOne(GetMessageContext context) {
        context.setPullStatus(PullStatus.NO_NEW_MSG);
        LOGGER.warn("No new message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
            context.getGetMessageResult().getStatus(), context.getTopic(), context.getGroup(), context.getOffset());
    }

    private void parseNoMatchMessage(GetMessageContext context) {
        context.setPullStatus(PullStatus.NO_MATCHED_MSG);
        LOGGER.warn("No matched message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
            context.getGetMessageResult().getStatus(), context.getTopic(), context.getGroup(), context.getOffset());
    }

    private void parseFoundStatus(GetMessageContext context) {
        context.setPullStatus(PullStatus.FOUND);
        context.setFoundList(decodeMsgList(context.getGetMessageResult()));

        this.brokerController.getBrokerStatsManager().incGroupGetNums(context.getGroup(), context.getTopic(), context.getGetMessageResult().getMessageCount());
        this.brokerController.getBrokerStatsManager().incGroupGetSize(context.getGroup(), context.getTopic(), context.getGetMessageResult().getBufferTotalSize());
        this.brokerController.getBrokerStatsManager().incBrokerGetNums(context.getTopic(), context.getGetMessageResult().getMessageCount());
        if (context.isFoundListEmpty()) {
            return;
        }
        this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(context.getGroup(), context.getTopic(), context.getQueueId(),
            this.brokerController.getMessageStore().now() - context.getLastFound().getStoreTimestamp());

        Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
            .put(LABEL_TOPIC, context.getTopic())
            .put(LABEL_CONSUMER_GROUP, context.getGroup())
            .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(context.getTopic()) || MQConstants.isSysConsumerGroup(context.getGroup()))
            .build();
        BrokerMetricsManager.messagesOutTotal.add(context.getGetMessageResult().getMessageCount(), attributes);
        BrokerMetricsManager.throughputOutTotal.add(context.getGetMessageResult().getBufferTotalSize(), attributes);
    }

    private List<MessageExt> decodeMsgList(GetMessageResult getMessageResult) {
        List<MessageExt> foundList = new ArrayList<>();
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            addFoundList(foundList, messageBufferList);
        } finally {
            getMessageResult.release();
        }

        return foundList;
    }

    private void addFoundList(List<MessageExt> foundList, List<ByteBuffer> messageBufferList) {
        for (ByteBuffer bb : messageBufferList) {
            MessageExt msgExt = MessageDecoder.decode(bb, true, false);
            if (msgExt != null) {
                foundList.add(msgExt);
            }
        }
    }

    private MessageExtBrokerInner parseHalfMessageInner(MessageExtBrokerInner msgInner) {
        String uniqId = msgInner.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        if (uniqId != null && !uniqId.isEmpty()) {
            MessageAccessor.putProperty(msgInner, TransactionalMessageUtil.TRANSACTION_ID, uniqId);
        }
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC, msgInner.getTopic());
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msgInner.getQueueId()));

        //reset msg transaction type and set topic = TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC
        msgInner.setSysFlag(MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), MessageSysFlag.TRANSACTION_NOT_TYPE));
        msgInner.setTopic(TransactionalMessageUtil.buildHalfTopic());
        msgInner.setQueueId(0);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        return msgInner;
    }

    private MessageExtBrokerInner makeOpMessageInner(Message message, MessageQueue messageQueue) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(message.getTopic());
        msgInner.setBody(message.getBody());
        msgInner.setQueueId(messageQueue.getQueueId());
        msgInner.setTags(message.getTags());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgInner.getTags()));
        msgInner.setSysFlag(0);
        MessageAccessor.setProperties(msgInner, message.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(message.getProperties()));
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.storeHost);
        msgInner.setStoreHost(this.storeHost);
        msgInner.setWaitStoreMsgOK(false);
        MessageClientIDSetter.setUniqID(msgInner);
        return msgInner;
    }

    private TopicConfig selectTopicConfig(String topic) {
        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (topicConfig == null) {
            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                topic, 1, PermName.PERM_WRITE | PermName.PERM_READ, 0);
        }
        return topicConfig;
    }

    private MessageQueue getOpQueueByHalf(Integer queueId, String brokerName) {
        MessageQueue opQueue = new MessageQueue();
        opQueue.setTopic(TransactionalMessageUtil.buildOpTopic());
        opQueue.setBrokerName(brokerName);
        opQueue.setQueueId(queueId);
        return opQueue;
    }


}
