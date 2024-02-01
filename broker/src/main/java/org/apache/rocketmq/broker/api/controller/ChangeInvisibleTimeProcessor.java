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
package org.apache.rocketmq.broker.api.controller;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.metrics.PopMetricsManager;
import org.apache.rocketmq.broker.domain.queue.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.server.daemon.pop.PopBufferMergeThread;
import org.apache.rocketmq.broker.server.daemon.pop.QueueLockManager;
import org.apache.rocketmq.common.domain.topic.KeyBuilder;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.constant.PopConstants;
import org.apache.rocketmq.common.app.help.FAQUrl;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;
import org.apache.rocketmq.store.api.broker.pop.AckMsg;
import org.apache.rocketmq.store.api.broker.pop.PopCheckPoint;
import org.apache.rocketmq.store.api.broker.pop.PopKeyBuilder;

/**
 * process nack(not ack) request
 * - ack orderly message
 * - add copied message with body is check point
 * - ack the not ack message
 */
public class ChangeInvisibleTimeProcessor implements NettyRequestProcessor {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final Broker broker;
    private final String reviveTopic;

    public ChangeInvisibleTimeProcessor(final Broker broker) {
        this.broker = broker;
        this.reviveTopic = KeyBuilder.buildClusterReviveTopic(this.broker.getBrokerConfig().getBrokerClusterName());
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        return this.processRequest(ctx.channel(), request);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand processRequest(final Channel channel, RemotingCommand request) throws RemotingCommandException {
        ChangeInvisibleTimeRequestHeader requestHeader = (ChangeInvisibleTimeRequestHeader) request.decodeCommandCustomHeader(ChangeInvisibleTimeRequestHeader.class);
        RemotingCommand response = createResponse(request);
        ChangeInvisibleTimeResponseHeader responseHeader = (ChangeInvisibleTimeResponseHeader) response.readCustomHeader();

        if (!allowAccess(requestHeader, channel, response)) {
            return response;
        }

        String[] extraInfo = ExtraInfoUtil.split(requestHeader.getExtraInfo());
        if (ExtraInfoUtil.isOrder(extraInfo)) {
            return processChangeInvisibleTimeForOrder(requestHeader, extraInfo, response, responseHeader);
        }

        long now = System.currentTimeMillis();
        if (!processCheckPoint(requestHeader, extraInfo, now, response)) {
            return response;
        }

        processAck(requestHeader, extraInfo);

        formatResponseHeader(requestHeader, extraInfo, now, responseHeader);
        return response;
    }

    private RemotingCommand createResponse(RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(ChangeInvisibleTimeResponseHeader.class);
        response.setCode(ResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());
        return response;
    }

    private boolean allowAccess(ChangeInvisibleTimeRequestHeader requestHeader, Channel channel, RemotingCommand response) {
        TopicConfig topicConfig = this.broker.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return false;
        }

        if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums() || requestHeader.getQueueId() < 0) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
                requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
            POP_LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark(errorInfo);
            return false;
        }
        long minOffset = this.broker.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
        long maxOffset = this.broker.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
        if (requestHeader.getOffset() < minOffset || requestHeader.getOffset() > maxOffset) {
            response.setCode(ResponseCode.NO_MESSAGE);
            return false;
        }

        return true;
    }

    private boolean processCheckPoint(ChangeInvisibleTimeRequestHeader requestHeader, String[] extraInfo, long now, RemotingCommand response) {
        // add new ck
        PutMessageResult ckResult = appendCheckPoint(requestHeader, ExtraInfoUtil.getReviveQid(extraInfo), requestHeader.getQueueId(), requestHeader.getOffset(), now, ExtraInfoUtil.getBrokerName(extraInfo));

        if (ckResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            return true;
        }

        if (ckResult.getPutMessageStatus() == PutMessageStatus.FLUSH_DISK_TIMEOUT) {
            return true;
        }

        if (ckResult.getPutMessageStatus() == PutMessageStatus.FLUSH_SLAVE_TIMEOUT) {
            return true;
        }

        if (ckResult.getPutMessageStatus() == PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            return true;
        }

        POP_LOGGER.error("change Invisible, put new ck error: {}", ckResult);
        response.setCode(ResponseCode.SYSTEM_ERROR);
        return false;
    }

    private void processAck(ChangeInvisibleTimeRequestHeader requestHeader, String[] extraInfo) {
        // ack old msg.
        try {
            ackOrigin(requestHeader, extraInfo);
        } catch (Throwable e) {
            POP_LOGGER.error("change Invisible, put ack msg error: {}, {}", requestHeader.getExtraInfo(), e.getMessage());
            // cancel new ck?
        }
    }

    private void formatResponseHeader(ChangeInvisibleTimeRequestHeader requestHeader, String[] extraInfo, long now, ChangeInvisibleTimeResponseHeader responseHeader) {
        responseHeader.setInvisibleTime(requestHeader.getInvisibleTime());
        responseHeader.setPopTime(now);
        responseHeader.setReviveQid(ExtraInfoUtil.getReviveQid(extraInfo));
    }

    protected RemotingCommand processChangeInvisibleTimeForOrder(ChangeInvisibleTimeRequestHeader requestHeader,
        String[] extraInfo, RemotingCommand response, ChangeInvisibleTimeResponseHeader responseHeader) {
        long popTime = ExtraInfoUtil.getPopTime(extraInfo);

        ConsumerOffsetManager offsetManager = this.broker.getConsumerOffsetManager();
        long oldOffset = offsetManager.queryOffset(requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
        if (requestHeader.getOffset() < oldOffset) {
            return response;
        }

        QueueLockManager lockManager = this.broker.getBrokerNettyServer().getPopServiceManager().getQueueLockManager();
        while (!lockManager.tryLock(requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueId())) {
        }
        try {
            oldOffset = offsetManager.queryOffset(requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
            if (requestHeader.getOffset() < oldOffset) {
                return response;
            }

            long nextVisibleTime = System.currentTimeMillis() + requestHeader.getInvisibleTime();
            this.broker.getConsumerOrderInfoManager().updateNextVisibleTime(
                requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueId(), requestHeader.getOffset(), popTime, nextVisibleTime);

            responseHeader.setInvisibleTime(nextVisibleTime - popTime);
            responseHeader.setPopTime(popTime);
            responseHeader.setReviveQid(ExtraInfoUtil.getReviveQid(extraInfo));
        } finally {
            lockManager.unLock(requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueId());
        }
        return response;
    }

    private void ackOrigin(final ChangeInvisibleTimeRequestHeader requestHeader, String[] extraInfo) {
        AckMsg ackMsg = createAckMsg(requestHeader, extraInfo);

        int rqId = ExtraInfoUtil.getReviveQid(extraInfo);

        this.broker.getBrokerStatsManager().incBrokerAckNums(1);
        this.broker.getBrokerStatsManager().incGroupAckNums(requestHeader.getConsumerGroup(), requestHeader.getTopic(), 1);

        PopBufferMergeThread ackService = broker.getBrokerNettyServer().getPopServiceManager().getPopBufferMergeService();
        if (ackService.addAckMsg(rqId, ackMsg)) {
            return;
        }

        MessageExtBrokerInner msgInner = createMessage(ackMsg, rqId, extraInfo);
        PutMessageResult putMessageResult = this.broker.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        logPutMessageResult(putMessageResult, ackMsg);

        PopMetricsManager.incPopReviveAckPutCount(ackMsg, putMessageResult.getPutMessageStatus());
    }

    private AckMsg createAckMsg(ChangeInvisibleTimeRequestHeader requestHeader, String[] extraInfo) {
        AckMsg ackMsg = new AckMsg();

        ackMsg.setAckOffset(requestHeader.getOffset());
        ackMsg.setStartOffset(ExtraInfoUtil.getCkQueueOffset(extraInfo));
        ackMsg.setConsumerGroup(requestHeader.getConsumerGroup());
        ackMsg.setTopic(requestHeader.getTopic());
        ackMsg.setQueueId(requestHeader.getQueueId());
        ackMsg.setPopTime(ExtraInfoUtil.getPopTime(extraInfo));
        ackMsg.setBrokerName(ExtraInfoUtil.getBrokerName(extraInfo));

        return ackMsg;
    }

    private MessageExtBrokerInner createMessage(AckMsg ackMsg, int rqId, String[] extraInfo) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(reviveTopic);
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(DataConverter.CHARSET_UTF8));
        msgInner.setQueueId(rqId);
        msgInner.setTags(PopConstants.ACK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.broker.getStoreHost());
        msgInner.setStoreHost(this.broker.getStoreHost());
        msgInner.setDeliverTimeMs(ExtraInfoUtil.getPopTime(extraInfo) + ExtraInfoUtil.getInvisibleTime(extraInfo));
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopKeyBuilder.genAckUniqueId(ackMsg));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        return msgInner;
    }

    private void logPutMessageResult(PutMessageResult putMessageResult, AckMsg ackMsg) {
        if (putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            return;
        }

        if (putMessageResult.getPutMessageStatus() == PutMessageStatus.FLUSH_DISK_TIMEOUT) {
            return;
        }

        if (putMessageResult.getPutMessageStatus() == PutMessageStatus.FLUSH_SLAVE_TIMEOUT) {
            return;
        }

        if (putMessageResult.getPutMessageStatus() == PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            return;
        }

        POP_LOGGER.error("change Invisible, put ack msg fail: {}, {}", ackMsg, putMessageResult);
    }

    private PutMessageResult appendCheckPoint(final ChangeInvisibleTimeRequestHeader requestHeader, int reviveQid, int queueId, long offset, long popTime, String brokerName) {
        PopCheckPoint ck = createPopCheckPoint(requestHeader, queueId, offset, popTime, brokerName);
        MessageExtBrokerInner msgInner = createCheckPointMessage(reviveQid, ck);

        // add check point msg to revive log
        PutMessageResult putMessageResult = this.broker.getEscapeBridge().putMessageToSpecificQueue(msgInner);

        if (broker.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("change Invisible , appendCheckPoint, topic {}, queueId {},reviveId {}, cid {}, startOffset {}, rt {}, result {}",
                requestHeader.getTopic(), queueId, reviveQid, requestHeader.getConsumerGroup(), offset, ck.getReviveTime(), putMessageResult);
        }

        incCheckPointCount(putMessageResult, ck, requestHeader);

        return putMessageResult;
    }

    private PopCheckPoint createPopCheckPoint(ChangeInvisibleTimeRequestHeader requestHeader, int queueId, long offset, long popTime, String brokerName) {
        PopCheckPoint ck = new PopCheckPoint();
        ck.setBitMap(0);
        ck.setNum((byte) 1);
        ck.setPopTime(popTime);
        ck.setInvisibleTime(requestHeader.getInvisibleTime());
        ck.setStartOffset(offset);
        ck.setCId(requestHeader.getConsumerGroup());
        ck.setTopic(requestHeader.getTopic());
        ck.setQueueId(queueId);
        ck.addDiff(0);
        ck.setBrokerName(brokerName);

        return ck;
    }

    private MessageExtBrokerInner createCheckPointMessage(int reviveQid, PopCheckPoint ck) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(reviveTopic);
        msgInner.setBody(JSON.toJSONString(ck).getBytes(DataConverter.CHARSET_UTF8));
        msgInner.setQueueId(reviveQid);
        msgInner.setTags(PopConstants.CK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.broker.getStoreHost());
        msgInner.setStoreHost(this.broker.getStoreHost());
        msgInner.setDeliverTimeMs(ck.getReviveTime() - PopConstants.ackTimeInterval);
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopKeyBuilder.genCkUniqueId(ck));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        return msgInner;
    }

    private void incCheckPointCount(PutMessageResult putMessageResult, PopCheckPoint ck, ChangeInvisibleTimeRequestHeader requestHeader) {
        if (putMessageResult == null) {
            return;
        }

        PopMetricsManager.incPopReviveCkPutCount(ck, putMessageResult.getPutMessageStatus());
        if (!putMessageResult.isOk()) {
            return;
        }

        this.broker.getBrokerStatsManager().incBrokerCkNums(1);
        this.broker.getBrokerStatsManager().incGroupCkNums(requestHeader.getConsumerGroup(), requestHeader.getTopic(), 1);
    }
}
