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
package org.apache.rocketmq.broker.processor;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.util.BitSet;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.PopMetricsManager;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BatchAck;
import org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.BatchAckMsg;

public class AckMessageProcessor implements NettyRequestProcessor {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final BrokerController brokerController;

    public AckMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
                                          RemotingCommand request) throws RemotingCommandException {
        return this.processRequest(ctx.channel(), request, true);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand processRequest(final Channel channel, RemotingCommand request, boolean brokerAllowSuspend) throws RemotingCommandException {
        if (request.getCode() == RequestCode.ACK_MESSAGE) {
            return handleAckMessage(channel, request);
        } else if (request.getCode() == RequestCode.BATCH_ACK_MESSAGE) {
            return handleBatchAckMessage(channel, request);
        } else {
            return handleIllegalRequest(channel, request);
        }
    }

    private RemotingCommand handleAckMessage(final Channel channel, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setOpaque(request.getOpaque());

        AckMessageRequestHeader requestHeader = (AckMessageRequestHeader) request.decodeCommandCustomHeader(AckMessageRequestHeader.class);

        TopicConfig topicConfig = getTopicConfig(requestHeader, channel, response);
        if (null == topicConfig) {
            return response;
        }

        if (!checkQueueId(channel, requestHeader, topicConfig, response)) {
            return response;
        }

        if (!checkOffset(requestHeader, response)) {
            return response;
        }

        appendAck(requestHeader, null, response, channel, null);
        return response;
    }

    private RemotingCommand handleBatchAckMessage(final Channel channel, RemotingCommand request) {
        BatchAckMessageRequestBody reqBody = null;
        final RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setOpaque(request.getOpaque());

        if (request.getBody() != null) {
            reqBody = BatchAckMessageRequestBody.decode(request.getBody(), BatchAckMessageRequestBody.class);
        }
        if (reqBody == null || reqBody.getAcks() == null || reqBody.getAcks().isEmpty()) {
            response.setCode(ResponseCode.NO_MESSAGE);
            return response;
        }
        for (BatchAck bAck : reqBody.getAcks()) {
            appendAck(null, bAck, response, channel, reqBody.getBrokerName());
        }
        return response;
    }

    private TopicConfig getTopicConfig(AckMessageRequestHeader requestHeader, Channel channel, RemotingCommand response) {
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null != topicConfig) {
            return topicConfig;
        }

        POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
        response.setCode(ResponseCode.TOPIC_NOT_EXIST);
        response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
        return null;
    }

    private boolean checkQueueId(Channel channel, AckMessageRequestHeader requestHeader, TopicConfig topicConfig, RemotingCommand response) {
        if (requestHeader.getQueueId() < topicConfig.getReadQueueNums() && requestHeader.getQueueId() >= 0) {
            return true;
        }

        String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
            requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
        POP_LOGGER.warn(errorInfo);
        response.setCode(ResponseCode.MESSAGE_ILLEGAL);
        response.setRemark(errorInfo);
        return false;
    }

    private boolean checkOffset(AckMessageRequestHeader requestHeader, RemotingCommand response) {
        long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
        long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());

        if (requestHeader.getOffset() >= minOffset && requestHeader.getOffset() <= maxOffset) {
            return true;
        }

        String errorInfo = String.format("offset is illegal, key:%s@%d, commit:%d, store:%d~%d",
            requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getOffset(), minOffset, maxOffset);
        POP_LOGGER.warn(errorInfo);
        response.setCode(ResponseCode.NO_MESSAGE);
        response.setRemark(errorInfo);

        return false;
    }

    private RemotingCommand handleIllegalRequest(final Channel channel, RemotingCommand request) {
        POP_LOGGER.error("AckMessageProcessor failed to process RequestCode: {}, consumer: {} ", request.getCode(), RemotingHelper.parseChannelRemoteAddr(channel));

        final RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setOpaque(request.getOpaque());
        response.setCode(ResponseCode.MESSAGE_ILLEGAL);
        response.setRemark(String.format("AckMessageProcessor failed to process RequestCode: %d", request.getCode()));

        return response;
    }

    private void appendAck(final AckMessageRequestHeader requestHeader, final BatchAck batchAck, final RemotingCommand response, final Channel channel, String brokerName) {
        BatchAckMsg ackMsg = new BatchAckMsg();
        int rqId = getRqid(requestHeader, batchAck);

        int ackCount = ackMsg(requestHeader, response, channel, batchAck, brokerName, ackMsg);
        if (ackCount < 1) {
            return;
        }

        this.brokerController.getBrokerStatsManager().incBrokerAckNums(ackCount);
        this.brokerController.getBrokerStatsManager().incGroupAckNums(ackMsg.getConsumerGroup(), ackMsg.getTopic(), ackCount);
        if (this.brokerController.getBrokerNettyServer().getPopMessageProcessor().getPopBufferMergeService().addAckMsg(rqId, ackMsg)) {
            brokerController.getPopInflightMessageCounter().decrementInFlightMessageNum(ackMsg.getTopic(), ackMsg.getConsumerGroup(), ackMsg.getPopTime(), ackMsg.getQueueId(), ackCount);
            return;
        }

        MessageExtBrokerInner msgInner = initMessageInner(ackMsg, requestHeader, batchAck);
        PutMessageResult putMessageResult = this.brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        logPutMessageResult(putMessageResult);


        PopMetricsManager.incPopReviveAckPutCount(ackMsg, putMessageResult.getPutMessageStatus());
        brokerController.getPopInflightMessageCounter().decrementInFlightMessageNum(ackMsg.getTopic(), ackMsg.getConsumerGroup(), ackMsg.getPopTime(), ackMsg.getQueueId(), ackCount);
    }

    private void initAckMsg(AckMessageRequestHeader requestHeader, String[] extraInfo, AckMsg ackMsg) {
        ackMsg.setConsumerGroup(requestHeader.getConsumerGroup());
        ackMsg.setTopic(requestHeader.getTopic());
        ackMsg.setQueueId(requestHeader.getQueueId());
        ackMsg.setStartOffset(ExtraInfoUtil.getCkQueueOffset(extraInfo));
        ackMsg.setAckOffset(requestHeader.getOffset());
        ackMsg.setPopTime(ExtraInfoUtil.getPopTime(extraInfo));
        ackMsg.setBrokerName(ExtraInfoUtil.getBrokerName(extraInfo));
    }

    private void ackOrderly(String topic, String consumeGroup, int qId, long ackOffset, long popTime, long invisibleTime, Channel channel, RemotingCommand response) {

        long oldOffset = this.brokerController.getConsumerOffsetManager().queryOffset(consumeGroup, topic, qId);
        if (ackOffset < oldOffset) {
            return;
        }

        PopMessageProcessor popMessageProcessor = this.brokerController.getBrokerNettyServer().getPopMessageProcessor();
        String lockKey = topic + PopAckConstants.SPLIT + consumeGroup + PopAckConstants.SPLIT + qId;

        lockQueue(popMessageProcessor, lockKey);
        try {
            oldOffset = this.brokerController.getConsumerOffsetManager().queryOffset(consumeGroup, topic, qId);
            if (ackOffset < oldOffset) {
                return;
            }

            long nextOffset = brokerController.getConsumerOrderInfoManager().commitAndNext(topic, consumeGroup, qId, ackOffset, popTime);
            if (nextOffset > -1) {
                handleCommitSuccess(topic, consumeGroup, qId, nextOffset, popMessageProcessor, invisibleTime, channel);
            } else if (nextOffset == -1) {
                handleIllegalCommit(lockKey, oldOffset, ackOffset, nextOffset, channel, response);
                return;
            }
        } finally {
            unlockQueue(popMessageProcessor, lockKey);
        }
        brokerController.getPopInflightMessageCounter().decrementInFlightMessageNum(topic, consumeGroup, popTime, qId, 1);
    }

    private void lockQueue(PopMessageProcessor popMessageProcessor, String lockKey) {
        while (!popMessageProcessor.getQueueLockManager().tryLock(lockKey)) {
        }
    }

    private void unlockQueue(PopMessageProcessor popMessageProcessor, String lockKey) {
        popMessageProcessor.getQueueLockManager().unLock(lockKey);
    }

    private void handleCommitSuccess(String topic, String consumeGroup, int qId, long nextOffset, PopMessageProcessor popMessageProcessor, long invisibleTime, Channel channel) {
        if (!this.brokerController.getConsumerOffsetManager().hasOffsetReset(
            topic, consumeGroup, qId)) {
            this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(),
                consumeGroup, topic, qId, nextOffset);
        }
        if (!this.brokerController.getConsumerOrderInfoManager().checkBlock(null, topic,
            consumeGroup, qId, invisibleTime)) {
            popMessageProcessor.notifyMessageArriving(
                topic, consumeGroup, qId);
        }
    }

    private void handleIllegalCommit(String lockKey, long oldOffset, long ackOffset, long nextOffset, Channel channel, RemotingCommand response) {
        String errorInfo = String.format("offset is illegal, key:%s, old:%d, commit:%d, next:%d, %s", lockKey, oldOffset, ackOffset, nextOffset, channel.remoteAddress());
        POP_LOGGER.warn(errorInfo);
        response.setCode(ResponseCode.MESSAGE_ILLEGAL);
        response.setRemark(errorInfo);
    }

    private int ackSingleMsg(final AckMessageRequestHeader requestHeader, final RemotingCommand response, final Channel channel, AckMsg ackMsg) {
        // single ack
        String[] extraInfo = ExtraInfoUtil.split(requestHeader.getExtraInfo());
        initAckMsg(requestHeader, extraInfo, ackMsg);

        if (ExtraInfoUtil.getReviveQid(extraInfo) != KeyBuilder.POP_ORDER_REVIVE_QUEUE) {
            return 1;
        }

        ackOrderly(requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueId(), requestHeader.getOffset(),
            ExtraInfoUtil.getPopTime(extraInfo), ExtraInfoUtil.getInvisibleTime(extraInfo), channel, response);
        return -1;
    }

    private int ackBatchMsg(final RemotingCommand response, final Channel channel, final BatchAck batchAck, String brokerName, BatchAckMsg ackMsg) {
        // batch ack
        String topic = ExtraInfoUtil.getRealTopic(batchAck.getTopic(), batchAck.getConsumerGroup(), ExtraInfoUtil.RETRY_TOPIC.equals(batchAck.getRetry()));

        long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, batchAck.getQueueId());
        long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, batchAck.getQueueId());
        if (minOffset == -1 || maxOffset == -1) {
            POP_LOGGER.error("Illegal topic or queue found when batch ack {}", batchAck);
            return -1;
        }

        BatchAckMsg batchAckMsg = ackBatchMsgOneByOne(response, channel, batchAck, minOffset, maxOffset);
        if (batchAck.getReviveQueueId() == KeyBuilder.POP_ORDER_REVIVE_QUEUE || batchAckMsg.getAckOffsetList().isEmpty()) {
            return -1;
        }

        ackMsg.setAckOffsetList(batchAckMsg.getAckOffsetList());
        initBatchMsg(ackMsg, batchAck, topic, brokerName);

        return batchAckMsg.getAckOffsetList().size();
    }

    private BatchAckMsg ackBatchMsgOneByOne(final RemotingCommand response, final Channel channel, BatchAck batchAck, long minOffset, long maxOffset) {
        BatchAckMsg batchAckMsg = new BatchAckMsg();
        BitSet bitSet = batchAck.getBitSet();
        String topic = ExtraInfoUtil.getRealTopic(batchAck.getTopic(), batchAck.getConsumerGroup(), ExtraInfoUtil.RETRY_TOPIC.equals(batchAck.getRetry()));


        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
            if (i == Integer.MAX_VALUE) {
                break;
            }
            long offset = batchAck.getStartOffset() + i;
            if (offset < minOffset || offset > maxOffset) {
                continue;
            }
            if (batchAck.getReviveQueueId() == KeyBuilder.POP_ORDER_REVIVE_QUEUE) {
                ackOrderly(topic, batchAck.getConsumerGroup(), batchAck.getQueueId(), offset, batchAck.getPopTime(), batchAck.getInvisibleTime(), channel, response);
            } else {
                batchAckMsg.getAckOffsetList().add(offset);
            }
        }

        return batchAckMsg;
    }

    private void initBatchMsg(BatchAckMsg ackMsg, BatchAck batchAck, String topic, String brokerName) {
        ackMsg.setConsumerGroup(batchAck.getConsumerGroup());
        ackMsg.setTopic(topic);
        ackMsg.setQueueId(batchAck.getQueueId());
        ackMsg.setStartOffset(batchAck.getStartOffset());
        ackMsg.setAckOffset(-1);
        ackMsg.setPopTime(batchAck.getPopTime());
        ackMsg.setBrokerName(brokerName);
    }

    private int ackMsg(final AckMessageRequestHeader requestHeader, final RemotingCommand response, final Channel channel, final BatchAck batchAck, String brokerName, BatchAckMsg ackMsg) {
        int ackCount = 0;
        if (batchAck == null) {
            ackCount = ackSingleMsg(requestHeader, response, channel, ackMsg);
        } else {
            ackCount = ackBatchMsg(response, channel, batchAck, brokerName, ackMsg);
        }

        return ackCount;
    }

    private int getRqid(AckMessageRequestHeader requestHeader, final BatchAck batchAck) {
        if (batchAck == null) {
            String[] extraInfo = ExtraInfoUtil.split(requestHeader.getExtraInfo());
            return ExtraInfoUtil.getReviveQid(extraInfo);
        } else {
            return batchAck.getReviveQueueId();
        }
    }

    private long getInvisibleTime(final AckMessageRequestHeader requestHeader, final BatchAck batchAck) {
        if (batchAck == null) {
            String[] extraInfo = ExtraInfoUtil.split(requestHeader.getExtraInfo());
            return ExtraInfoUtil.getInvisibleTime(extraInfo);
        } else {
            return batchAck.getInvisibleTime();
        }
    }

    private MessageExtBrokerInner initMessageInner(AckMsg ackMsg, AckMessageRequestHeader requestHeader, BatchAck batchAck) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(brokerController.getBrokerNettyServer().getPopReviveManager().getReviveTopic());
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(DataConverter.charset));
        msgInner.setQueueId(getRqid(requestHeader, batchAck));
        if (null != batchAck) {
            msgInner.setTags(PopAckConstants.BATCH_ACK_TAG);
            msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genBatchAckUniqueId((BatchAckMsg) ackMsg));
        } else {
            msgInner.setTags(PopAckConstants.ACK_TAG);
            msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genAckUniqueId(ackMsg));
        }
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());
        msgInner.setDeliverTimeMs(ackMsg.getPopTime() + getInvisibleTime(requestHeader, batchAck));
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genAckUniqueId(ackMsg));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        return msgInner;
    }

    private void logPutMessageResult(PutMessageResult putMessageResult) {
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("put ack msg error:" + putMessageResult);
        }
    }
}
