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

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.domain.transaction.OperationResult;
import org.apache.rocketmq.broker.domain.transaction.queue.TransactionalMessageUtil;
import org.apache.rocketmq.broker.server.metrics.BrokerMetricsManager;
import org.apache.rocketmq.common.domain.topic.TopicFilterType;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageAccessor;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.domain.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.server.config.BrokerRole;

import static org.apache.rocketmq.broker.server.metrics.BrokerMetricsConstant.LABEL_TOPIC;

/**
 * EndTransaction processor: process commit and rollback message
 * prepare message will be processed by SendMessageProcessor
 *
 * There are two more groups of threads:
 *      1. to check prepared message status
 *      2. to send check message to client
 */
public class EndTransactionProcessor implements NettyRequestProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);
    private final Broker broker;

    public EndTransactionProcessor(final Broker broker) {
        this.broker = broker;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final EndTransactionRequestHeader requestHeader = (EndTransactionRequestHeader) request.decodeCommandCustomHeader(EndTransactionRequestHeader.class);
        LOGGER.debug("Transaction request:{}", requestHeader);

        if (BrokerRole.SLAVE == broker.getMessageStoreConfig().getBrokerRole()) {
            response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
            LOGGER.warn("Message store is slave mode, so end transaction is forbidden. ");
            return response;
        }

        if (!checkTransactionState(ctx, request, requestHeader)) {
            return null;
        }

        OperationResult result = new OperationResult();
        if (MessageSysFlag.TRANSACTION_COMMIT_TYPE == requestHeader.getCommitOrRollback()) {
            return processCommitRequest(requestHeader, response);
        }

        if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == requestHeader.getCommitOrRollback()) {
            return processRollbackRequest(requestHeader, response);
        }

        return response.setCodeAndRemark(result.getResponseCode(), result.getResponseRemark());
    }

    private RemotingCommand processCommitRequest(EndTransactionRequestHeader requestHeader, RemotingCommand response) {
        OperationResult result = this.broker.getBrokerMessageService().getTransactionalMessageService().commitMessage(requestHeader);
        if (result.getResponseCode() != ResponseCode.SUCCESS) {
            return response.setCodeAndRemark(result.getResponseCode(), result.getResponseRemark());
        }

        if (rejectCommitOrRollback(requestHeader, result.getPrepareMessage())) {
            response.setCode(ResponseCode.ILLEGAL_OPERATION);
            LOGGER.warn("Message commit fail [producer end]. currentTimeMillis - bornTime > checkImmunityTime, msgId={},commitLogOffset={}, wait check",
                requestHeader.getMsgId(), requestHeader.getCommitLogOffset());
            return response;
        }
        RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
        if (res.getCode() != ResponseCode.SUCCESS) {
            return res;
        }

        MessageExtBrokerInner msgInner = endMessageTransaction(result.getPrepareMessage());
        msgInner.setSysFlag(MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), requestHeader.getCommitOrRollback()));
        msgInner.setQueueOffset(requestHeader.getTranStateTableOffset());
        msgInner.setPreparedTransactionOffset(requestHeader.getCommitLogOffset());
        msgInner.setStoreTimestamp(result.getPrepareMessage().getStoreTimestamp());
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED);
        RemotingCommand sendResult = sendFinalMessage(msgInner);
        if (sendResult.getCode() == ResponseCode.SUCCESS) {
            this.broker.getBrokerMessageService().getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
            // successful committed, then total num of half-messages minus 1
            this.broker.getBrokerMessageService().getTransactionalMessageService().getTransactionMetrics().addAndGet(msgInner.getTopic(), -1);
            BrokerMetricsManager.commitMessagesTotal.add(1, BrokerMetricsManager.newAttributesBuilder()
                .put(LABEL_TOPIC, msgInner.getTopic())
                .build());
            // record the commit latency.
            Long commitLatency = (System.currentTimeMillis() - result.getPrepareMessage().getBornTimestamp()) / 1000;
            BrokerMetricsManager.transactionFinishLatency.record(commitLatency, BrokerMetricsManager.newAttributesBuilder()
                .put(LABEL_TOPIC, msgInner.getTopic())
                .build());
        }
        return sendResult;
    }

    private RemotingCommand processRollbackRequest(EndTransactionRequestHeader requestHeader, RemotingCommand response) {
        OperationResult result = this.broker.getBrokerMessageService().getTransactionalMessageService().rollbackMessage(requestHeader);
        if (result.getResponseCode() != ResponseCode.SUCCESS) {
            return response.setCodeAndRemark(result.getResponseCode(), result.getResponseRemark());
        }

        if (rejectCommitOrRollback(requestHeader, result.getPrepareMessage())) {
            response.setCode(ResponseCode.ILLEGAL_OPERATION);
            LOGGER.warn("Message rollback fail [producer end]. currentTimeMillis - bornTime > checkImmunityTime, msgId={},commitLogOffset={}, wait check",
                requestHeader.getMsgId(), requestHeader.getCommitLogOffset());
            return response;
        }
        RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
        if (res.getCode() == ResponseCode.SUCCESS) {
            this.broker.getBrokerMessageService().getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
            // roll back, then total num of half-messages minus 1
            this.broker.getBrokerMessageService().getTransactionalMessageService().getTransactionMetrics().addAndGet(result.getPrepareMessage().getProperty(MessageConst.PROPERTY_REAL_TOPIC), -1);
            BrokerMetricsManager.rollBackMessagesTotal.add(1, BrokerMetricsManager.newAttributesBuilder()
                .put(LABEL_TOPIC, result.getPrepareMessage().getProperty(MessageConst.PROPERTY_REAL_TOPIC))
                .build());
        }
        return res;
    }

    /**
     * If you specify a custom first check time CheckImmunityTimeInSeconds,
     * And the commit/rollback request whose validity period exceeds CheckImmunityTimeInSeconds and is not checked back will be processed and failed
     * returns ILLEGAL_OPERATION 604 error
     * @param requestHeader requestHeader
     * @param messageExt messageExt
     * @return boolean
     */
    public boolean rejectCommitOrRollback(EndTransactionRequestHeader requestHeader, MessageExt messageExt) {
        if (requestHeader.getFromTransactionCheck()) {
            return false;
        }
        long transactionTimeout = broker.getBrokerConfig().getTransactionTimeOut();

        String checkImmunityTimeStr = messageExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
        if (StringUtils.isNotEmpty(checkImmunityTimeStr)) {
            long valueOfCurrentMinusBorn = System.currentTimeMillis() - messageExt.getBornTimestamp();
            long checkImmunityTime = TransactionalMessageUtil.getImmunityTime(checkImmunityTimeStr, transactionTimeout);
            //Non-check requests that exceed the specified custom first check time fail to return
            return valueOfCurrentMinusBorn > checkImmunityTime;
        }
        return false;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private boolean checkTransactionState(ChannelHandlerContext ctx, RemotingCommand request, EndTransactionRequestHeader requestHeader) {
        if (requestHeader.getFromTransactionCheck()) {
            return checkStateFromTransaction(ctx, request, requestHeader);
        } else {
            return checkState(ctx, request, requestHeader);
        }
    }

    private boolean checkStateFromTransaction(ChannelHandlerContext ctx, RemotingCommand request, EndTransactionRequestHeader requestHeader) {
        switch (requestHeader.getCommitOrRollback()) {
            case MessageSysFlag.TRANSACTION_NOT_TYPE: {
                LOGGER.warn("Check producer[{}] transaction state, but it's pending status."
                        + "RequestHeader: {} Remark: {}",
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                    requestHeader.toString(),
                    request.getRemark());
                return false;
            }

            case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {
                LOGGER.warn("Check producer[{}] transaction state, the producer commit the message."
                        + "RequestHeader: {} Remark: {}",
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                    requestHeader.toString(),
                    request.getRemark());

                break;
            }

            case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {
                LOGGER.warn("Check producer[{}] transaction state, the producer rollback the message."
                        + "RequestHeader: {} Remark: {}",
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                    requestHeader.toString(),
                    request.getRemark());
                break;
            }
            default:
                return false;
        }

        return true;
    }

    private boolean checkState(ChannelHandlerContext ctx, RemotingCommand request, EndTransactionRequestHeader requestHeader) {
        switch (requestHeader.getCommitOrRollback()) {
            case MessageSysFlag.TRANSACTION_NOT_TYPE: {
                LOGGER.warn("The producer[{}] end transaction in sending message,  and it's pending status."
                        + "RequestHeader: {} Remark: {}",
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                    requestHeader.toString(),
                    request.getRemark());
                return false;
            }

            case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {
                break;
            }

            case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {
                LOGGER.warn("The producer[{}] end transaction in sending message, rollback the message."
                        + "RequestHeader: {} Remark: {}",
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                    requestHeader.toString(),
                    request.getRemark());
                break;
            }
            default:
                return false;
        }

        return true;
    }

    private RemotingCommand checkPrepareMessage(MessageExt msgExt, EndTransactionRequestHeader requestHeader) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        if (msgExt == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("Find prepared transaction message failed");
            return response;
        }

        final String pgroupRead = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
        if (!pgroupRead.equals(requestHeader.getProducerGroup())) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The producer group wrong");
            return response;
        }

        if (msgExt.getQueueOffset() != requestHeader.getTranStateTableOffset()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The transaction state table offset wrong");
            return response;
        }

        if (msgExt.getCommitLogOffset() != requestHeader.getCommitLogOffset()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The commit log offset wrong");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        return response;
    }

    private MessageExtBrokerInner endMessageTransaction(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
        msgInner.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
        msgInner.setWaitStoreMsgOK(false);
        msgInner.setTransactionId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        msgInner.setSysFlag(msgExt.getSysFlag());
        TopicFilterType topicFilterType = (msgInner.getSysFlag() & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG
                ? TopicFilterType.MULTI_TAG
                : TopicFilterType.SINGLE_TAG;

        long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID);
        return msgInner;
    }

    private RemotingCommand sendFinalMessage(MessageExtBrokerInner msgInner) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final PutMessageResult putMessageResult = this.broker.getMessageStore().putMessage(msgInner);
        if (putMessageResult == null) {
            return response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "store putMessage return null");
        }

        switch (putMessageResult.getPutMessageStatus()) {
            // Success
            case PUT_OK:
                this.broker.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
                this.broker.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(), putMessageResult.getAppendMessageResult().getWroteBytes());
                this.broker.getBrokerStatsManager().incBrokerPutNums(msgInner.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum());
            case FLUSH_DISK_TIMEOUT:
            case FLUSH_SLAVE_TIMEOUT:
            case SLAVE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                break;
            // Failed
            case CREATE_MAPPED_FILE_FAILED:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("Create mapped file failed.");
                break;
            case MESSAGE_ILLEGAL:
            case PROPERTIES_SIZE_EXCEEDED:
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(String.format("The message is illegal, maybe msg body or properties length not matched. msg body length limit %dB, msg properties length limit 32KB.",
                    this.broker.getMessageStoreConfig().getMaxMessageSize()));
                break;
            case SERVICE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                response.setRemark("Service not available now.");
                break;
            case OS_PAGE_CACHE_BUSY:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("OS page cache busy, please try another machine");
                break;
            case WHEEL_TIMER_MSG_ILLEGAL:
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(String.format("timer message illegal, the delay time should not be bigger than the max delay %dms; or if set del msg, the delay time should be bigger than the current time",
                    this.broker.getMessageStoreConfig().getTimerMaxDelaySec() * 1000L));
                break;
            case WHEEL_TIMER_FLOW_CONTROL:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(String.format("timer message is under flow control, max num limit is %d or the current value is greater than %d and less than %d, trigger random flow control",
                    this.broker.getMessageStoreConfig().getTimerCongestNumEachSlot() * 2L, this.broker.getMessageStoreConfig().getTimerCongestNumEachSlot(), this.broker.getMessageStoreConfig().getTimerCongestNumEachSlot() * 2L));
                break;
            case WHEEL_TIMER_NOT_ENABLE:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(String.format("accurate timer message is not enabled, timerWheelEnable is %s",
                    this.broker.getMessageStoreConfig().isTimerWheelEnable()));
                break;
            case UNKNOWN_ERROR:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR");
                break;
            case IN_SYNC_REPLICAS_NOT_ENOUGH:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("in-sync replicas not enough");
                break;
            case PUT_TO_REMOTE_BROKER_FAIL:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("put to remote broker fail");
                break;
            default:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR DEFAULT");
                break;
        }
        return response;

    }
}
