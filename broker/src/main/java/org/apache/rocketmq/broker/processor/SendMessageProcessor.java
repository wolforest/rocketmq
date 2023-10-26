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

import io.netty.channel.ChannelHandlerContext;
import io.opentelemetry.api.common.Attributes;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.common.AbortProcessException;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.attribute.CleanupPolicy;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.CleanupPolicyUtils;
import org.apache.rocketmq.common.utils.IOTinyUtils;
import org.apache.rocketmq.common.utils.MQUtils;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.common.utils.TimeUtils;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingContext;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_MESSAGE_TYPE;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;
import static org.apache.rocketmq.remoting.protocol.RemotingCommand.buildErrorResponse;

public class SendMessageProcessor extends AbstractSendMessageProcessor implements NettyRequestProcessor {

    public SendMessageProcessor(final BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        if (request.getCode() == RequestCode.CONSUMER_SEND_MSG_BACK) {
            return this.consumerSendMsgBack(ctx, request);
        }

        return processSendRequest(ctx, request);
    }

    @Override
    public boolean rejectRequest() {
        if (!this.brokerController.getBrokerConfig().isEnableSlaveActingMaster() && this.brokerController.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
            return true;
        }

        if (this.brokerController.getMessageStore().isOSPageCacheBusy() || this.brokerController.getMessageStore().isTransientStorePoolDeficient()) {
            return true;
        }

        return false;
    }

    private RemotingCommand processSendRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        SendMessageContext sendMessageContext;
        SendMessageRequestHeader requestHeader = parseRequestHeader(request);
        if (requestHeader == null) {
            return null;
        }
        TopicQueueMappingContext mappingContext = this.brokerController.getTopicQueueMappingManager().buildTopicQueueMappingContext(requestHeader, true);
        RemotingCommand rewriteResult = this.brokerController.getTopicQueueMappingManager().rewriteRequestForStaticTopic(requestHeader, mappingContext);
        if (rewriteResult != null) {
            return rewriteResult;
        }
        sendMessageContext = buildMsgContext(ctx, requestHeader, request);
        RemotingCommand hookResult = executeSendMessageHookBefore(sendMessageContext, request);
        if (hookResult != null) {
            return hookResult;
        }

        if (requestHeader.isBatch()) {
            return this.sendBatchMessage(ctx, request, sendMessageContext, requestHeader, mappingContext,
                (ctx1, response1) -> executeSendMessageHookAfter(response1, ctx1));
        }

        return this.sendMessage(ctx, request, sendMessageContext, requestHeader, mappingContext,
            (ctx12, response12) -> executeSendMessageHookAfter(response12, ctx12));
    }

    private RemotingCommand executeSendMessageHookBefore(SendMessageContext sendMessageContext, RemotingCommand request) {
        try {
            this.executeSendMessageHookBefore(sendMessageContext);
        } catch (AbortProcessException e) {
            final RemotingCommand errorResponse = RemotingCommand.createResponseCommand(e.getResponseCode(), e.getErrorMessage());
            errorResponse.setOpaque(request.getOpaque());
            return errorResponse;
        }

        return null;
    }

    /**
     * If the response is not null, it meets some errors
     *
     * @return response
     */
    private RemotingCommand rewriteResponseForStaticTopic(SendMessageResponseHeader responseHeader,
        TopicQueueMappingContext mappingContext) {
        try {
            if (mappingContext.getMappingDetail() == null) {
                return null;
            }
            TopicQueueMappingDetail mappingDetail = mappingContext.getMappingDetail();

            LogicQueueMappingItem mappingItem = mappingContext.getLeaderItem();
            if (mappingItem == null) {
                return buildErrorResponse(ResponseCode.NOT_LEADER_FOR_QUEUE, String.format("%s-%d does not exit in request process of current broker %s", mappingContext.getTopic(), mappingContext.getGlobalId(), mappingDetail.getBname()));
            }
            //no need to care the broker name
            long staticLogicOffset = mappingItem.computeStaticQueueOffsetLoosely(responseHeader.getQueueOffset());
            if (staticLogicOffset < 0) {
                //if the logic offset is -1, just let it go
                //maybe we need a dynamic config
                //return buildErrorResponse(ResponseCode.NOT_LEADER_FOR_QUEUE, String.format("%s-%d convert offset error in current broker %s", mappingContext.getTopic(), mappingContext.getGlobalId(), mappingDetail.getBname()));
            }
            responseHeader.setQueueId(mappingContext.getGlobalId());
            responseHeader.setQueueOffset(staticLogicOffset);
        } catch (Throwable t) {
            return buildErrorResponse(ResponseCode.SYSTEM_ERROR, t.getMessage());
        }
        return null;
    }

    private void setMsgFlag(MessageExt msg, SendMessageRequestHeader requestHeader, TopicConfig topicConfig) {
        int sysFlag = requestHeader.getSysFlag();
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }
        msg.setSysFlag(sysFlag);
    }

    private boolean handleRetryAndDLQ(SendMessageRequestHeader requestHeader, RemotingCommand response,
        RemotingCommand request, MessageExt msg, TopicConfig topicConfig, Map<String, String> properties) {
        setMsgFlag(msg, requestHeader, topicConfig);
        String newTopic = requestHeader.getTopic();
        if (null == newTopic || !newTopic.startsWith(MQUtils.RETRY_GROUP_TOPIC_PREFIX)) {
            return true;
        }

        String groupName = newTopic.substring(MQUtils.RETRY_GROUP_TOPIC_PREFIX.length());
        SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(groupName);
        if (null == subscriptionGroupConfig) {
            response.setCodeAndRemark(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST, "subscription group not exist, " + groupName + " " + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
            return false;
        }

        int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
        if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal() && requestHeader.getMaxReconsumeTimes() != null) {
            maxReconsumeTimes = requestHeader.getMaxReconsumeTimes();
        }

        int reconsumeTimes = requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes();
        boolean sendRetryMessageToDeadLetterQueueDirectly = false;

        if (!brokerController.getBrokerClusterService().getRebalanceLockManager().isLockAllExpired(groupName)) {
            LOGGER.info("Group has unexpired lock record, which show it is ordered message, send it to DLQ "
                    + "right now group={}, topic={}, reconsumeTimes={}, maxReconsumeTimes={}.", groupName, newTopic, reconsumeTimes, maxReconsumeTimes);
            sendRetryMessageToDeadLetterQueueDirectly = true;
        }

        if (reconsumeTimes <= maxReconsumeTimes && !sendRetryMessageToDeadLetterQueueDirectly) {
            return true;
        }

        setDlqMatrix(requestHeader);
        properties.put(MessageConst.PROPERTY_DELAY_TIME_LEVEL, "-1");
        newTopic = MQUtils.getDLQTopic(groupName);
        setMsgInfo(msg, newTopic);

        topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic, DLQ_NUMS_PER_GROUP, PermName.PERM_WRITE | PermName.PERM_READ, 0);
        if (null == topicConfig) {
            response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "topic[" + newTopic + "] not exist");
            return false;
        }

        return true;
    }

    private void setMsgInfo(MessageExt msg, String newTopic) {
        int queueIdInt = randomQueueId(DLQ_NUMS_PER_GROUP);
        msg.setTopic(newTopic);
        msg.setQueueId(queueIdInt);
        msg.setDelayTimeLevel(0);
    }

    private void setDlqMatrix(SendMessageRequestHeader requestHeader) {
        Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
            .put(LABEL_CONSUMER_GROUP, requestHeader.getProducerGroup())
            .put(LABEL_TOPIC, requestHeader.getTopic())
            .put(LABEL_IS_SYSTEM, BrokerMetricsManager.isSystem(requestHeader.getTopic(), requestHeader.getProducerGroup()))
            .build();
        BrokerMetricsManager.sendToDlqMessages.add(1, attributes);
    }

    private int getQueueId(SendMessageRequestHeader requestHeader, TopicConfig topicConfig) {
        int queueIdInt = requestHeader.getQueueId();
        if (queueIdInt < 0) {
            queueIdInt = randomQueueId(topicConfig.getWriteQueueNums());
        }

        return queueIdInt;
    }

    private MessageExtBrokerInner createMessageExtBrokerInner(RemotingCommand request, SendMessageRequestHeader requestHeader, int queueIdInt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setQueueId(queueIdInt);
        msgInner.setBody(request.getBody());
        msgInner.setFlag(requestHeader.getFlag());

        return msgInner;
    }

    private void setMsgProperties(MessageExtBrokerInner msgInner, Map<String, String> oriProps) {
        String uniqKey = oriProps.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        if (uniqKey == null || uniqKey.length() <= 0) {
            uniqKey = MessageClientIDSetter.createUniqID();
            oriProps.put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, uniqKey);
        }

        MessageAccessor.setProperties(msgInner, oriProps);
    }

    private boolean checkCleanupPolicy(MessageExtBrokerInner msgInner, TopicConfig topicConfig, RemotingCommand response) {
        CleanupPolicy cleanupPolicy = CleanupPolicyUtils.getDeletePolicy(Optional.of(topicConfig));
        if (!Objects.equals(cleanupPolicy, CleanupPolicy.COMPACTION)) {
            return true;
        }

        if (StringUtils.isBlank(msgInner.getKeys())) {
            response.setCodeAndRemark(ResponseCode.MESSAGE_ILLEGAL, "Required message key is missing");
            return false;
        }

        return true;
    }

    private void addMsgInfo(MessageExtBrokerInner msgInner, TopicConfig topicConfig, SendMessageRequestHeader requestHeader, ChannelHandlerContext ctx) {
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(topicConfig.getTopicFilterType(), msgInner.getTags()));
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        String clusterName = this.brokerController.getBrokerConfig().getBrokerClusterName();
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_CLUSTER, clusterName);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
    }

    public RemotingCommand sendMessage(final ChannelHandlerContext ctx, final RemotingCommand request, final SendMessageContext sendMessageContext,
        final SendMessageRequestHeader requestHeader, final TopicQueueMappingContext mappingContext, final SendMessageCallback sendMessageCallback) throws RemotingCommandException {

        final RemotingCommand response = preSend(ctx, request, requestHeader);
        if (response.getCode() != -1) {
            return response;
        }

        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

        int queueIdInt = getQueueId(requestHeader, topicConfig);
        MessageExtBrokerInner msgInner = createMessageExtBrokerInner(request, requestHeader, queueIdInt);

        Map<String, String> oriProps = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        if (!handleRetryAndDLQ(requestHeader, response, request, msgInner, topicConfig, oriProps)) {
            return response;
        }

        setMsgProperties(msgInner, oriProps);
        if (!checkCleanupPolicy(msgInner, topicConfig, response)) {
            return response;
        }

        addMsgInfo(msgInner, topicConfig, requestHeader, ctx);

        // Map<String, String> oriProps = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        String traFlag = oriProps.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        boolean sendTransactionPrepareMessage = false;
        if (Boolean.parseBoolean(traFlag) && !(msgInner.getReconsumeTimes() > 0 && msgInner.getDelayTimeLevel() > 0)) { //For client under version 4.6.1
            if (isRejectTransactionMessage(response)) {
                return response;
            }
            sendTransactionPrepareMessage = true;
        }

        long beginTimeMillis = this.brokerController.getMessageStore().now();
        if (brokerController.getBrokerConfig().isAsyncSendEnable()) {
            return asyncSendMessage(msgInner, response, request, requestHeader, responseHeader, sendMessageContext, sendMessageCallback, ctx, queueIdInt, beginTimeMillis, sendTransactionPrepareMessage, mappingContext);
        }
        return sendMessage(msgInner, response, request, requestHeader, responseHeader, sendMessageContext, sendMessageCallback, ctx, queueIdInt, beginTimeMillis, sendTransactionPrepareMessage, mappingContext);
    }

    private boolean isRejectTransactionMessage(RemotingCommand response) {
        if (!this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
            return false;
        }

        response.setCode(ResponseCode.NO_PERMISSION);
        response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1() + "] sending transaction message is forbidden");
        return true;
    }

    private RemotingCommand asyncSendMessage(MessageExtBrokerInner msgInner, RemotingCommand response, RemotingCommand request, SendMessageRequestHeader requestHeader,
        SendMessageResponseHeader responseHeader, SendMessageContext sendMessageContext, SendMessageCallback sendMessageCallback, ChannelHandlerContext ctx,
        int queueIdInt, long beginTimeMillis, boolean sendTransactionPrepareMessage, TopicQueueMappingContext mappingContext) {
        CompletableFuture<PutMessageResult> asyncPutMessageFuture;
        if (sendTransactionPrepareMessage) {
            asyncPutMessageFuture = this.brokerController.getBrokerMessageService().getTransactionalMessageService().asyncPrepareMessage(msgInner);
        } else {
            asyncPutMessageFuture = this.brokerController.getMessageStore().asyncPutMessage(msgInner);
        }

        final int finalQueueIdInt = queueIdInt;
        final MessageExtBrokerInner finalMsgInner = msgInner;
        asyncPutMessageFuture.thenAcceptAsync(putMessageResult -> {
            RemotingCommand responseFuture = handlePutMessageResult(putMessageResult, response, request, finalMsgInner, responseHeader, sendMessageContext,
                ctx, finalQueueIdInt, beginTimeMillis, mappingContext, BrokerMetricsManager.getMessageType(requestHeader));
            if (responseFuture != null) {
                doResponse(ctx, request, responseFuture);
            }
            sendMessageCallback.onComplete(sendMessageContext, response);
        }, this.brokerController.getBrokerNettyServer().getPutMessageFutureExecutor());
        // Returns null to release the send message thread
        return null;
    }

    private RemotingCommand sendMessage(MessageExtBrokerInner msgInner, RemotingCommand response, RemotingCommand request, SendMessageRequestHeader requestHeader,
        SendMessageResponseHeader responseHeader, SendMessageContext sendMessageContext, SendMessageCallback sendMessageCallback, ChannelHandlerContext ctx,
        int queueIdInt, long beginTimeMillis, boolean sendTransactionPrepareMessage, TopicQueueMappingContext mappingContext) {

        PutMessageResult putMessageResult;
        if (sendTransactionPrepareMessage) {
            putMessageResult = this.brokerController.getBrokerMessageService().getTransactionalMessageService().prepareMessage(msgInner);
        } else {
            putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        }
        handlePutMessageResult(putMessageResult, response, request, msgInner, responseHeader, sendMessageContext, ctx, queueIdInt, beginTimeMillis, mappingContext, BrokerMetricsManager.getMessageType(requestHeader));
        sendMessageCallback.onComplete(sendMessageContext, response);
        return response;
    }


    private RemotingCommand handlePutMessageResult(PutMessageResult putMessageResult, RemotingCommand response,
        RemotingCommand request, MessageExt msg, SendMessageResponseHeader responseHeader,
        SendMessageContext sendMessageContext, ChannelHandlerContext ctx, int queueIdInt, long beginTimeMillis,
        TopicQueueMappingContext mappingContext, TopicMessageType messageType) {
        if (putMessageResult == null) {
            return response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "store putMessage return null");
        }

        if (!isSendOk(putMessageResult, response)) {
            executeSendMessageHook(putMessageResult, request, sendMessageContext, responseHeader, false);
            return response;
        }

        saveStatInfo(putMessageResult, msg, messageType, queueIdInt, beginTimeMillis);
        formatResponse(response, responseHeader, putMessageResult, queueIdInt, msg);

        RemotingCommand rewriteResult = rewriteResponseForStaticTopic(responseHeader, mappingContext);
        if (rewriteResult != null) {
            return rewriteResult;
        }

        doResponse(ctx, request, response);
        executeSendMessageHook(putMessageResult, request, sendMessageContext, responseHeader, true);
        return null;
    }

    private boolean isSendOk(PutMessageResult putMessageResult, RemotingCommand response) {
        boolean sendOK = false;

        switch (putMessageResult.getPutMessageStatus()) {
            // Success
            case PUT_OK:
                sendOK = true;
                response.setCode(ResponseCode.SUCCESS);
                break;
            case FLUSH_DISK_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_DISK_TIMEOUT);
                sendOK = true;
                break;
            case FLUSH_SLAVE_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_SLAVE_TIMEOUT);
                sendOK = true;
                break;
            case SLAVE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
                sendOK = true;
                break;

            // Failed
            case IN_SYNC_REPLICAS_NOT_ENOUGH:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("in-sync replicas not enough");
                break;
            case CREATE_MAPPED_FILE_FAILED:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("create mapped file failed, server is busy or broken.");
                break;
            case MESSAGE_ILLEGAL:
            case PROPERTIES_SIZE_EXCEEDED:
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(String.format("the message is illegal, maybe msg body or properties length not matched. msg body length limit %dB, msg properties length limit 32KB.",
                    this.brokerController.getMessageStoreConfig().getMaxMessageSize()));
                break;
            case WHEEL_TIMER_MSG_ILLEGAL:
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(String.format("timer message illegal, the delay time should not be bigger than the max delay %dms; or if set del msg, the delay time should be bigger than the current time",
                    this.brokerController.getMessageStoreConfig().getTimerMaxDelaySec() * 1000L));
                break;
            case WHEEL_TIMER_FLOW_CONTROL:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(String.format("timer message is under flow control, max num limit is %d or the current value is greater than %d and less than %d, trigger random flow control",
                    this.brokerController.getMessageStoreConfig().getTimerCongestNumEachSlot() * 2L, this.brokerController.getMessageStoreConfig().getTimerCongestNumEachSlot(), this.brokerController.getMessageStoreConfig().getTimerCongestNumEachSlot() * 2L));
                break;
            case WHEEL_TIMER_NOT_ENABLE:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(String.format("accurate timer message is not enabled, timerWheelEnable is %s",
                    this.brokerController.getMessageStoreConfig().isTimerWheelEnable()));
                break;
            case SERVICE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                response.setRemark(
                    "service not available now. It may be caused by one of the following reasons: " +
                        "the broker's disk is full [" + diskUtil() + "], messages are put to the slave, message store has been shut down, etc.");
                break;
            case OS_PAGE_CACHE_BUSY:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("[PC_SYNCHRONIZED]broker busy, start flow control for a while");
                break;
            case LMQ_CONSUME_QUEUE_NUM_EXCEEDED:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("[LMQ_CONSUME_QUEUE_NUM_EXCEEDED]broker config enableLmq and enableMultiDispatch, lmq consumeQueue num exceed maxLmqConsumeQueueNum config num, default limit 2w.");
                break;
            case UNKNOWN_ERROR:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR");
                break;
            default:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR DEFAULT");
                break;
        }

        return sendOK;
    }

    private void saveStatInfo(PutMessageResult putMessageResult, MessageExt msg, TopicMessageType messageType, int queueIdInt, long beginTimeMillis) {
        if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(msg.getTopic())) {
            this.brokerController.getBrokerStatsManager().incQueuePutNums(msg.getTopic(), msg.getQueueId(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
            this.brokerController.getBrokerStatsManager().incQueuePutSize(msg.getTopic(), msg.getQueueId(), putMessageResult.getAppendMessageResult().getWroteBytes());
        }

        this.brokerController.getBrokerStatsManager().incTopicPutNums(msg.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
        this.brokerController.getBrokerStatsManager().incTopicPutSize(msg.getTopic(), putMessageResult.getAppendMessageResult().getWroteBytes());
        this.brokerController.getBrokerStatsManager().incBrokerPutNums(msg.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum());
        this.brokerController.getBrokerStatsManager().incTopicPutLatency(msg.getTopic(), queueIdInt, (int) (this.brokerController.getMessageStore().now() - beginTimeMillis));

        if (BrokerMetricsManager.isRetryOrDlqTopic(msg.getTopic())) {
            return;
        }

        Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
            .put(LABEL_TOPIC, msg.getTopic())
            .put(LABEL_MESSAGE_TYPE, messageType.getMetricsValue())
            .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(msg.getTopic()))
            .build();
        BrokerMetricsManager.messagesInTotal.add(putMessageResult.getAppendMessageResult().getMsgNum(), attributes);
        BrokerMetricsManager.throughputInTotal.add(putMessageResult.getAppendMessageResult().getWroteBytes(), attributes);
        BrokerMetricsManager.messageSize.record(putMessageResult.getAppendMessageResult().getWroteBytes() / putMessageResult.getAppendMessageResult().getMsgNum(), attributes);
    }

    private void formatResponse(RemotingCommand response, SendMessageResponseHeader responseHeader, PutMessageResult putMessageResult, int queueIdInt, MessageExt msg) {
        response.setRemark(null);
        responseHeader.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
        responseHeader.setQueueId(queueIdInt);
        responseHeader.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());
        responseHeader.setTransactionId(MessageClientIDSetter.getUniqID(msg));
    }

    private void executeSendMessageHook(PutMessageResult putMessageResult, RemotingCommand request, SendMessageContext sendMessageContext,
        SendMessageResponseHeader responseHeader, boolean sendOk) {
        if (!hasSendMessageHook()) {
            return;
        }

        String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
        String authType = request.getExtFields().get(BrokerStatsManager.ACCOUNT_AUTH_TYPE);
        String ownerParent = request.getExtFields().get(BrokerStatsManager.ACCOUNT_OWNER_PARENT);
        String ownerSelf = request.getExtFields().get(BrokerStatsManager.ACCOUNT_OWNER_SELF);
        int commercialSizePerMsg = brokerController.getBrokerConfig().getCommercialSizePerMsg();

        int wroteSize, msgNum, commercialMsgNum, commercialSendTimes;
        BrokerStatsManager.StatsType state;
        if (sendOk) {
            state = BrokerStatsManager.StatsType.SEND_SUCCESS;
            sendMessageContext.setMsgId(responseHeader.getMsgId());
            sendMessageContext.setQueueId(responseHeader.getQueueId());
            sendMessageContext.setQueueOffset(responseHeader.getQueueOffset());

            int commercialBaseCount = brokerController.getBrokerConfig().getCommercialBaseCount();
            wroteSize = putMessageResult.getAppendMessageResult().getWroteBytes();
            msgNum = putMessageResult.getAppendMessageResult().getMsgNum();
            commercialMsgNum = (int) Math.ceil(wroteSize / (double) commercialSizePerMsg);
            commercialSendTimes = commercialMsgNum * commercialBaseCount;
        } else {
            state = BrokerStatsManager.StatsType.SEND_FAILURE;
            AppendMessageResult appendMessageResult = putMessageResult.getAppendMessageResult();
            // TODO process partial failures of batch message
            wroteSize = request.getBody().length;
            msgNum = Math.max(appendMessageResult != null ? appendMessageResult.getMsgNum() : 1, 1);
            commercialMsgNum = (int) Math.ceil(wroteSize / (double) commercialSizePerMsg);
            commercialSendTimes = commercialMsgNum;
        }

        sendMessageContext.setCommercialSendStats(state);
        sendMessageContext.setCommercialSendTimes(commercialSendTimes);
        sendMessageContext.setCommercialSendSize(wroteSize);
        sendMessageContext.setCommercialOwner(owner);

        sendMessageContext.setSendStat(state);
        sendMessageContext.setCommercialSendMsgNum(commercialMsgNum);
        sendMessageContext.setAccountAuthType(authType);
        sendMessageContext.setAccountOwnerParent(ownerParent);
        sendMessageContext.setAccountOwnerSelf(ownerSelf);
        sendMessageContext.setSendMsgSize(wroteSize);
        sendMessageContext.setSendMsgNum(msgNum);
    }

    private MessageExtBatch createMessageExtBatch(ChannelHandlerContext ctx, RemotingCommand request, SendMessageRequestHeader requestHeader, TopicConfig topicConfig, int queueIdInt) {
        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(requestHeader.getTopic());
        messageExtBatch.setQueueId(queueIdInt);

        int sysFlag = requestHeader.getSysFlag();
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }
        messageExtBatch.setSysFlag(sysFlag);

        messageExtBatch.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(messageExtBatch, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        messageExtBatch.setBody(request.getBody());
        messageExtBatch.setBornTimestamp(requestHeader.getBornTimestamp());
        messageExtBatch.setBornHost(ctx.channel().remoteAddress());
        messageExtBatch.setStoreHost(this.getStoreHost());
        messageExtBatch.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        String clusterName = this.brokerController.getBrokerConfig().getBrokerClusterName();
        MessageAccessor.putProperty(messageExtBatch, MessageConst.PROPERTY_CLUSTER, clusterName);

        if (QueueTypeUtils.isBatchCq(Optional.of(topicConfig)) && MessageClientIDSetter.getUniqID(messageExtBatch) != null) {
            // newly introduced inner-batch message
            messageExtBatch.setSysFlag(messageExtBatch.getSysFlag() | MessageSysFlag.NEED_UNWRAP_FLAG);
            messageExtBatch.setSysFlag(messageExtBatch.getSysFlag() | MessageSysFlag.INNER_BATCH_FLAG);
            messageExtBatch.setInnerBatch(true);

            int innerNum = MessageDecoder.countInnerMsgNum(ByteBuffer.wrap(messageExtBatch.getBody()));
            MessageAccessor.putProperty(messageExtBatch, MessageConst.PROPERTY_INNER_NUM, String.valueOf(innerNum));
            messageExtBatch.setPropertiesString(MessageDecoder.messageProperties2String(messageExtBatch.getProperties()));
        }

        return messageExtBatch;
    }

    private boolean validateBatchRequest(SendMessageRequestHeader requestHeader, RemotingCommand response) {
        if (response.getCode() != -1) {
            return false;
        }

        if (requestHeader.getTopic().length() > Byte.MAX_VALUE) {
            response.setCodeAndRemark(ResponseCode.MESSAGE_ILLEGAL, "message topic length too long " + requestHeader.getTopic().length());
            return false;
        }

        if (requestHeader.getTopic() != null && requestHeader.getTopic().startsWith(MQUtils.RETRY_GROUP_TOPIC_PREFIX)) {
            response.setCodeAndRemark(ResponseCode.MESSAGE_ILLEGAL, "batch request does not support retry group " + requestHeader.getTopic());
            return false;
        }

        return true;
    }

    private RemotingCommand sendBatchMessage(final ChannelHandlerContext ctx, final RemotingCommand request, final SendMessageContext sendMessageContext,
        final SendMessageRequestHeader requestHeader, TopicQueueMappingContext mappingContext, final SendMessageCallback sendMessageCallback) {

        final RemotingCommand response = preSend(ctx, request, requestHeader);
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();
        if (!validateBatchRequest(requestHeader, response)) {
            return response;
        }

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        int queueIdInt = getQueueId(requestHeader, topicConfig);
        MessageExtBatch messageExtBatch = createMessageExtBatch(ctx, request, requestHeader, topicConfig, queueIdInt);

        boolean isInnerBatch = false;
        if (QueueTypeUtils.isBatchCq(Optional.of(topicConfig)) && MessageClientIDSetter.getUniqID(messageExtBatch) != null) {
            // tell the producer that it's an inner-batch message response.
            responseHeader.setBatchUniqId(MessageClientIDSetter.getUniqID(messageExtBatch));
            isInnerBatch = true;
        }

        long beginTimeMillis = this.brokerController.getMessageStore().now();
        if (this.brokerController.getBrokerConfig().isAsyncSendEnable()) {
            return asyncSendBatchMessage(response, request, messageExtBatch, responseHeader, sendMessageContext, ctx, queueIdInt, beginTimeMillis, mappingContext, isInnerBatch, requestHeader, sendMessageCallback);
        }
        return sendBatchMessage(response, request, messageExtBatch, responseHeader, sendMessageContext, ctx, queueIdInt, beginTimeMillis, mappingContext, isInnerBatch, requestHeader, sendMessageCallback);
    }

    private RemotingCommand asyncSendBatchMessage(RemotingCommand response,
        RemotingCommand request, MessageExtBatch messageExtBatch, SendMessageResponseHeader responseHeader,
        SendMessageContext sendMessageContext, ChannelHandlerContext ctx, int queueIdInt, long beginTimeMillis,
        TopicQueueMappingContext mappingContext, boolean isInnerBatch, SendMessageRequestHeader requestHeader,
        SendMessageCallback sendMessageCallback) {
        CompletableFuture<PutMessageResult> asyncPutMessageFuture;
        if (isInnerBatch) {
            asyncPutMessageFuture = this.brokerController.getMessageStore().asyncPutMessage(messageExtBatch);
        } else {
            asyncPutMessageFuture = this.brokerController.getMessageStore().asyncPutMessages(messageExtBatch);
        }
        final int finalQueueIdInt = queueIdInt;
        asyncPutMessageFuture.thenAcceptAsync(putMessageResult -> {
            RemotingCommand responseFuture = handlePutMessageResult(putMessageResult, response, request, messageExtBatch, responseHeader,
                    sendMessageContext, ctx, finalQueueIdInt, beginTimeMillis, mappingContext, BrokerMetricsManager.getMessageType(requestHeader));
            if (responseFuture != null) {
                doResponse(ctx, request, responseFuture);
            }
            sendMessageCallback.onComplete(sendMessageContext, response);
        }, this.brokerController.getBrokerNettyServer().getSendMessageExecutor());
        // Returns null to release send message thread
        return null;
    }

    private RemotingCommand sendBatchMessage(RemotingCommand response,
        RemotingCommand request, MessageExtBatch messageExtBatch, SendMessageResponseHeader responseHeader,
        SendMessageContext sendMessageContext, ChannelHandlerContext ctx, int queueIdInt, long beginTimeMillis,
        TopicQueueMappingContext mappingContext, boolean isInnerBatch, SendMessageRequestHeader requestHeader, SendMessageCallback sendMessageCallback) {

        PutMessageResult putMessageResult;
        if (isInnerBatch) {
            putMessageResult = this.brokerController.getMessageStore().putMessage(messageExtBatch);
        } else {
            putMessageResult = this.brokerController.getMessageStore().putMessages(messageExtBatch);
        }

        handlePutMessageResult(putMessageResult, response, request, messageExtBatch, responseHeader,
            sendMessageContext, ctx, queueIdInt, beginTimeMillis, mappingContext, BrokerMetricsManager.getMessageType(requestHeader));
        sendMessageCallback.onComplete(sendMessageContext, response);
        return response;

    }

    private String diskUtil() {
        double physicRatio = 100;
        String storePath;
        MessageStore messageStore = this.brokerController.getMessageStore();
        if (messageStore instanceof DefaultMessageStore) {
            storePath = ((DefaultMessageStore) messageStore).getStorePathPhysic();
        } else {
            storePath = this.brokerController.getMessageStoreConfig().getStorePathCommitLog();
        }
        String[] paths = storePath.trim().split(IOTinyUtils.MULTI_PATH_SPLITTER);
        for (String storePathPhysic : paths) {
            physicRatio = Math.min(physicRatio, IOTinyUtils.getDiskPartitionSpaceUsedPercent(storePathPhysic));
        }

        String storePathLogis = StorePathConfigHelper.getStorePathConsumeQueue(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        double logisRatio = IOTinyUtils.getDiskPartitionSpaceUsedPercent(storePathLogis);

        String storePathIndex = StorePathConfigHelper.getStorePathIndex(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        double indexRatio = IOTinyUtils.getDiskPartitionSpaceUsedPercent(storePathIndex);

        return String.format("CL: %5.2f CQ: %5.2f INDEX: %5.2f", physicRatio, logisRatio, indexRatio);
    }

    private RemotingCommand preSend(ChannelHandlerContext ctx, RemotingCommand request, SendMessageRequestHeader requestHeader) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        response.setOpaque(request.getOpaque());
        response.addExtField(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));
        LOGGER.debug("Receive SendMessage request command {}", request);

        final long startTimestamp = this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp();
        if (this.brokerController.getMessageStore().now() < startTimestamp) {
            return response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, String.format("broker unable to service, until %s", TimeUtils.timeMillisToHumanString2(startTimestamp)));
        }

        response.setCode(-1);
        super.msgCheck(ctx, requestHeader, request, response);

        return response;
    }

}
