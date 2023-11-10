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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.util.concurrent.Future;
import io.opentelemetry.api.common.Attributes;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.broker.longpolling.PollingHeader;
import org.apache.rocketmq.broker.longpolling.PollingResult;
import org.apache.rocketmq.broker.longpolling.PopLongPollingService;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.broker.service.pop.PopBufferMergeService;
import org.apache.rocketmq.broker.service.pop.QueueLockManager;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.constant.PopConstants;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.constant.MQConstants;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.metrics.RemotingMetricsManager;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.logfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.pop.PopCheckPoint;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_RETRY;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_REQUEST_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESPONSE_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESULT;

/**
 * Server side rebalance, Pop mode
 * @link https://github.com/apache/rocketmq/wiki/%5BRIP-19%5D-Server-side-rebalance,--lightweight-consumer-client-support
 */
public class PopMessageProcessor implements NettyRequestProcessor {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);

    private final BrokerController brokerController;
    private final Random random = new Random(System.currentTimeMillis());
    private static final String BORN_TIME = "bornTime";

    private final AtomicLong ckMessageNumber;

    public PopMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.ckMessageNumber = new AtomicLong();
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(PopMessageResponseHeader.class);
        PopMessageRequestHeader requestHeader = (PopMessageRequestHeader) request.decodeCommandCustomHeader(PopMessageRequestHeader.class, true);

        StringBuilder startOffsetInfo = new StringBuilder(64);
        StringBuilder msgOffsetInfo = new StringBuilder(64);
        // if not consume orderly, orderCountInfo = null
        StringBuilder orderCountInfo = initOrderCountInfo(requestHeader);

        initRequestAndResponse(request, response, requestHeader);
        if (!allowAccess(requestHeader, ctx.channel(), response)) {
            return response;
        }

        ExpressionMessageFilter messageFilter = null;
        if (requestHeader.getExp() != null && requestHeader.getExp().length() > 0) {
            messageFilter = initExpressionMessageFilter(requestHeader, response);
            if (messageFilter == null) {
                return response;
            }
        }
        compensateSubscribeData(requestHeader);

        int reviveQid = getReviveQid(requestHeader);
        long popTime = System.currentTimeMillis();

        GetMessageResult getMessageResult = new GetMessageResult(requestHeader.getMaxMsgNums());
        CompletableFuture<Long> getMessageFuture = popMessage(ctx, requestHeader, getMessageResult, messageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, reviveQid, popTime);
        bindGetMessageFutureCallback(ctx, requestHeader, getMessageResult, startOffsetInfo, msgOffsetInfo, orderCountInfo, reviveQid, popTime, getMessageFuture, response, request);

        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private boolean allowAccess(PopMessageRequestHeader requestHeader, Channel channel, RemotingCommand response) {
        if (requestHeader.isTimeoutTooMuch()) {
            response.setCode(ResponseCode.POLLING_TIMEOUT);
            response.setRemark(String.format("the broker[%s] pop message is timeout too much",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return false;
        }
        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[%s] pop message is forbidden",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return false;
        }
        if (requestHeader.getMaxMsgNums() > 32) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("the broker[%s] pop message's num is greater than 32",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return false;
        }

        if (!brokerController.getMessageStore().getMessageStoreConfig().isTimerWheelEnable()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("the broker[%s] pop message is forbidden because timerWheelEnable is false",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return false;
        }

        TopicConfig topicConfig =
            this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(),
                RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(),
                FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return false;
        }

        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] peeking message is forbidden");
            return false;
        }

        if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] " +
                    "consumer:[%s]",
                requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(),
                channel.remoteAddress());
            POP_LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);
            return false;
        }
        SubscriptionGroupConfig subscriptionGroupConfig =
            this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("subscription group [%s] does not exist, %s",
                requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return false;
        }

        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return false;
        }

        return true;
    }

    private void initRequestAndResponse(RemotingCommand request, RemotingCommand response, PopMessageRequestHeader requestHeader) {
        brokerController.getConsumerManager().compensateBasicConsumerInfo(requestHeader.getConsumerGroup(),
            ConsumeType.CONSUME_POP, MessageModel.CLUSTERING);

        request.addExtFieldIfNotExist(BORN_TIME, String.valueOf(System.currentTimeMillis()));
        if (Objects.equals(request.getExtFields().get(BORN_TIME), "0")) {
            request.addExtField(BORN_TIME, String.valueOf(System.currentTimeMillis()));
        }

        response.setOpaque(request.getOpaque());

        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("receive PopMessage request command, {}", request);
        }
    }

    private ExpressionMessageFilter initExpressionMessageFilter(PopMessageRequestHeader requestHeader, RemotingCommand response) {
        try {
            SubscriptionData subscriptionData = FilterAPI.build(requestHeader.getTopic(), requestHeader.getExp(), requestHeader.getExpType());
            brokerController.getConsumerManager().compensateSubscribeData(requestHeader.getConsumerGroup(), requestHeader.getTopic(), subscriptionData);

            String retryTopic = KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup());
            SubscriptionData retrySubscriptionData = FilterAPI.build(retryTopic, SubscriptionData.SUB_ALL, requestHeader.getExpType());
            brokerController.getConsumerManager().compensateSubscribeData(requestHeader.getConsumerGroup(), retryTopic, retrySubscriptionData);

            ConsumerFilterData consumerFilterData = null;
            if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                return new ExpressionMessageFilter(subscriptionData, consumerFilterData, brokerController.getConsumerFilterManager());
            }

            consumerFilterData = ConsumerFilterManager.build(requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getExp(), requestHeader.getExpType(), System.currentTimeMillis());
            if (consumerFilterData != null) {
                return new ExpressionMessageFilter(subscriptionData, consumerFilterData, brokerController.getConsumerFilterManager());
            }

            POP_LOGGER.warn("Parse the consumer's subscription[{}] failed, group: {}", requestHeader.getExp(), requestHeader.getConsumerGroup());
            response.setCodeAndRemark(ResponseCode.SUBSCRIPTION_PARSE_FAILED, "parse the consumer's subscription failed");
            return null;
        } catch (Exception e) {
            POP_LOGGER.warn("Parse the consumer's subscription[{}] error, group: {}", requestHeader.getExp(), requestHeader.getConsumerGroup());
            response.setCodeAndRemark(ResponseCode.SUBSCRIPTION_PARSE_FAILED, "parse the consumer's subscription failed");
            return null;
        }
    }

    private void compensateSubscribeData(PopMessageRequestHeader requestHeader) {
        if (requestHeader.getExp() == null || requestHeader.getExp().length() <= 0) {
            return;
        }
        try {
            SubscriptionData subscriptionData = FilterAPI.build(requestHeader.getTopic(), "*", ExpressionType.TAG);
            brokerController.getConsumerManager().compensateSubscribeData(requestHeader.getConsumerGroup(), requestHeader.getTopic(), subscriptionData);

            String retryTopic = KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup());
            SubscriptionData retrySubscriptionData = FilterAPI.build(retryTopic, "*", ExpressionType.TAG);
            brokerController.getConsumerManager().compensateSubscribeData(requestHeader.getConsumerGroup(), retryTopic, retrySubscriptionData);
        } catch (Exception e) {
            POP_LOGGER.warn("Build default subscription error, group: {}", requestHeader.getConsumerGroup());
        }
    }

    private int getReviveQid(PopMessageRequestHeader requestHeader) {
        int reviveQid;
        if (requestHeader.isOrder()) {
            reviveQid = KeyBuilder.POP_ORDER_REVIVE_QUEUE;
        } else {
            reviveQid = (int) Math.abs(ckMessageNumber.getAndIncrement() % this.brokerController.getBrokerConfig().getReviveQueueNum());
        }

        return reviveQid;
    }

    private CompletableFuture<Long> popMessage(ChannelHandlerContext ctx, PopMessageRequestHeader requestHeader, GetMessageResult getMessageResult, ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder finalOrderCountInfo, int reviveQid, long popTime) {
        int randomQ = random.nextInt(100);
        boolean needRetry = randomQ % 5 == 0;

        CompletableFuture<Long> getMessageFuture = CompletableFuture.completedFuture(0L);
        if (needRetry && !requestHeader.isOrder()) {
            getMessageFuture = popRetryMessage(ctx, requestHeader, getMessageResult, messageFilter, startOffsetInfo, msgOffsetInfo, finalOrderCountInfo, reviveQid, popTime, randomQ, getMessageFuture);
        }

        getMessageFuture = popMessage(ctx, requestHeader, getMessageResult, messageFilter, startOffsetInfo, msgOffsetInfo, finalOrderCountInfo, reviveQid, popTime, randomQ, getMessageFuture);

        // if not full , fetch retry again
        if (!needRetry && getMessageResult.getMessageMapedList().size() < requestHeader.getMaxMsgNums() && !requestHeader.isOrder()) {
            getMessageFuture = popRetryMessage(ctx, requestHeader, getMessageResult, messageFilter, startOffsetInfo, msgOffsetInfo, finalOrderCountInfo, reviveQid, popTime, randomQ, getMessageFuture);
        }

        return getMessageFuture;
    }

    private CompletableFuture<Long> popMessage(ChannelHandlerContext ctx, PopMessageRequestHeader requestHeader, GetMessageResult getMessageResult, ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder finalOrderCountInfo, int reviveQid, long popTime, int randomQ, CompletableFuture<Long> getMessageFuture) {

        if (requestHeader.getQueueId() >= 0) {
            return getMessageFuture.thenCompose(restNum -> popMsgFromQueue(requestHeader.getAttemptId(), false, getMessageResult, requestHeader, requestHeader.getQueueId(), restNum, reviveQid, ctx.channel(), popTime, messageFilter, startOffsetInfo, msgOffsetInfo, finalOrderCountInfo));
        }

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        // read all queue
        for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
            int queueId = (randomQ + i) % topicConfig.getReadQueueNums();
            getMessageFuture = getMessageFuture.thenCompose(restNum -> popMsgFromQueue(requestHeader.getAttemptId(), false, getMessageResult, requestHeader, queueId, restNum, reviveQid, ctx.channel(), popTime, messageFilter, startOffsetInfo, msgOffsetInfo, finalOrderCountInfo));
        }

        return getMessageFuture;
    }

    /**
     * TODO:
     *  - when did the retryTopic create?
     *  - How many read queue nums does retryTopic have?
     *  - What does retryTopic queue id look like?
     *  - Why use random value to get queue id?
     *  - How and When did message enqueue retryTopic?
     *
     */
    private CompletableFuture<Long> popRetryMessage(ChannelHandlerContext ctx, PopMessageRequestHeader requestHeader, GetMessageResult getMessageResult, ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder finalOrderCountInfo, int reviveQid, long popTime, int randomQ, CompletableFuture<Long> getMessageFuture) {

        TopicConfig retryTopicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup()));
        if (retryTopicConfig == null) {
            return getMessageFuture;
        }

        for (int i = 0; i < retryTopicConfig.getReadQueueNums(); i++) {
            int queueId = (randomQ + i) % retryTopicConfig.getReadQueueNums();
            getMessageFuture = getMessageFuture.thenCompose(restNum -> popMsgFromQueue(requestHeader.getAttemptId(), true, getMessageResult, requestHeader, queueId, restNum, reviveQid, ctx.channel(), popTime, messageFilter,
                startOffsetInfo, msgOffsetInfo, finalOrderCountInfo));
        }

        return getMessageFuture;
    }

    private boolean handlePollingAction(ChannelHandlerContext ctx, RemotingCommand request, PopMessageRequestHeader requestHeader, GetMessageResult getMessageResult, RemotingCommand finalResponse, long restNum) {
        PopLongPollingService popLongPollingService = brokerController.getBrokerNettyServer().getPopServiceManager().getPopPollingService();
        if (!getMessageResult.getMessageBufferList().isEmpty()) {
            finalResponse.setCode(ResponseCode.SUCCESS);
            getMessageResult.setStatus(GetMessageStatus.FOUND);
            if (restNum > 0) {
                // all queue pop can not notify specified queue pop, and vice versa
                popLongPollingService.notifyMessageArriving(requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueId());
            }
        } else {
            PollingResult pollingResult = popLongPollingService.polling(ctx, request, new PollingHeader(requestHeader));
            if (PollingResult.POLLING_SUC == pollingResult) {
                return false;
            } else if (PollingResult.POLLING_FULL == pollingResult) {
                finalResponse.setCode(ResponseCode.POLLING_FULL);
            } else {
                finalResponse.setCode(ResponseCode.POLLING_TIMEOUT);
            }
            getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
        }

        return true;
    }

    private void initResponseHeader(PopMessageResponseHeader responseHeader, PopMessageRequestHeader requestHeader, StringBuilder startOffsetInfo, StringBuilder msgOffsetInfo, StringBuilder finalOrderCountInfo, int reviveQid, long popTime, long restNum) {
        responseHeader.setInvisibleTime(requestHeader.getInvisibleTime());
        responseHeader.setPopTime(popTime);
        responseHeader.setReviveQid(reviveQid);
        responseHeader.setRestNum(restNum);
        responseHeader.setStartOffsetInfo(startOffsetInfo.toString());
        responseHeader.setMsgOffsetInfo(msgOffsetInfo.toString());
        if (requestHeader.isOrder() && finalOrderCountInfo != null) {
            responseHeader.setOrderCountInfo(finalOrderCountInfo.toString());
        }
    }

    private boolean handleSuccessResponse(ChannelHandlerContext ctx, RemotingCommand request, PopMessageRequestHeader requestHeader, GetMessageResult getMessageResult, RemotingCommand finalResponse) {
        if (this.brokerController.getBrokerConfig().isTransferMsgByHeap()) {
            final long beginTimeMills = this.brokerController.getMessageStore().now();
            final byte[] r = this.readGetMessageResult(getMessageResult, requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
            this.brokerController.getBrokerStatsManager().incGroupGetLatency(requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), (int) (this.brokerController.getMessageStore().now() - beginTimeMills));
            finalResponse.setBody(r);
            return true;
        }

        final GetMessageResult tmpGetMessageResult = getMessageResult;
        try {
            FileRegion fileRegion = new ManyMessageTransfer(finalResponse.encodeHeader(getMessageResult.getBufferTotalSize()), getMessageResult);
            ctx.channel().writeAndFlush(fileRegion).addListener((ChannelFutureListener) future -> {
                tmpGetMessageResult.release();
                recordRpcLatency(request, finalResponse, future);
                if (!future.isSuccess()) {
                    POP_LOGGER.error("Fail to transfer messages from page cache to {}", ctx.channel().remoteAddress(), future.cause());
                }
            });
        } catch (Throwable e) {
            POP_LOGGER.error("Error occurred when transferring messages from page cache", e);
            getMessageResult.release();
        }

        return false;
    }

    private void recordRpcLatency(RemotingCommand request, RemotingCommand finalResponse, Future<?> future) {
        Attributes attributes = RemotingMetricsManager.newAttributesBuilder()
            .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode()))
            .put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(finalResponse.getCode()))
            .put(LABEL_RESULT, RemotingMetricsManager.getWriteAndFlushResult(future))
            .build();

        RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributes);
    }

    private RemotingCommand handleFutureResponse(ChannelHandlerContext ctx, RemotingCommand request, PopMessageRequestHeader requestHeader, GetMessageResult getMessageResult, RemotingCommand finalResponse) {
        if (finalResponse.getCode() == ResponseCode.SUCCESS) {
            return finalResponse;
        }

        if (!handleSuccessResponse(ctx, request, requestHeader, getMessageResult, finalResponse)) {
            return null;
        }
        return finalResponse;
    }

    private void bindGetMessageFutureCallback(ChannelHandlerContext ctx, PopMessageRequestHeader requestHeader, GetMessageResult getMessageResult, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder finalOrderCountInfo, int reviveQid, long popTime, CompletableFuture<Long> getMessageFuture, RemotingCommand response, RemotingCommand request) {

        final PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) response.readCustomHeader();
        final RemotingCommand finalResponse = response;

        getMessageFuture.thenApply(restNum -> {
            if (!handlePollingAction(ctx, request, requestHeader, getMessageResult, finalResponse, restNum)) {
                return null;
            }
            initResponseHeader(responseHeader, requestHeader, startOffsetInfo, msgOffsetInfo, finalOrderCountInfo, reviveQid, popTime, restNum);
            finalResponse.setRemark(getMessageResult.getStatus().name());

            return handleFutureResponse(ctx, request, requestHeader, getMessageResult, finalResponse);
        }).thenAccept(result -> NettyRemotingAbstract.writeResponse(ctx.channel(), request, result));
    }

    private StringBuilder initOrderCountInfo(PopMessageRequestHeader requestHeader) {
        StringBuilder orderCountInfo = null;
        if (requestHeader.isOrder()) {
            orderCountInfo = new StringBuilder(64);
        }

        return orderCountInfo;
    }

    /**
     *
     * @param attemptId request attempt id
     * @param isRetry isRetry flag
     * @param getMessageResult getMessageResult
     * @param requestHeader requestHeader
     * @param queueId queueId: retry topic queueId | pop queueId
     * @param restNum restNum
     * @param reviveQid revive Queue id
     * @param channel netty channel
     * @param popTime popTime
     * @param messageFilter filter
     * @param startOffsetInfo startOffsetInfo
     * @param msgOffsetInfo msgOffsetInfo
     * @param orderCountInfo orderCountInfo : useless for non ordered Message
     * @return future<consumeOffset>
     */
    private CompletableFuture<Long> popMsgFromQueue(String attemptId, boolean isRetry, GetMessageResult getMessageResult,
        PopMessageRequestHeader requestHeader, int queueId, long restNum, int reviveQid,
        Channel channel, long popTime, ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder orderCountInfo) {

        //move it out of this method
        String topic = isRetry
            ? KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup())
            : requestHeader.getTopic();

        String lockKey = KeyBuilder.buildConsumeKey(topic, requestHeader.getConsumerGroup(), queueId);
        long offset = getPopOffset(topic, requestHeader.getConsumerGroup(), queueId, requestHeader.getInitMode(), false, lockKey, false);
        CompletableFuture<Long> future = new CompletableFuture<>();

        QueueLockManager queueLockManager = brokerController.getBrokerNettyServer().getPopServiceManager().getQueueLockManager();
        if (!queueLockManager.tryLock(lockKey)) {
            restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
            future.complete(restNum);
            return future;
        }

        try {
            future.whenComplete((result, throwable) -> queueLockManager.unLock(lockKey));
            offset = getPopOffset(topic, requestHeader.getConsumerGroup(), queueId, requestHeader.getInitMode(), true, lockKey, true);
            if (requestHeader.isOrder() && brokerController.getConsumerOrderInfoManager().checkBlock(attemptId, topic,
                requestHeader.getConsumerGroup(), queueId, requestHeader.getInvisibleTime())) {
                future.complete(this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum);
                return future;
            }

            if (requestHeader.isOrder()) {
                this.brokerController.getPopInflightMessageCounter().clearInFlightMessageNum(
                    topic,
                    requestHeader.getConsumerGroup(),
                    queueId
                );
            }

            if (getMessageResult.getMessageMapedList().size() >= requestHeader.getMaxMsgNums()) {
                restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
                future.complete(restNum);
                return future;
            }
        } catch (Exception e) {
            POP_LOGGER.error("Exception in popMsgFromQueue", e);
            future.complete(restNum);
            return future;
        }

        AtomicLong atomicRestNum = new AtomicLong(restNum);
        AtomicLong atomicOffset = new AtomicLong(offset);
        int maxMsgNums = requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size();
        long finalOffset = offset;
        return this.brokerController.getMessageStore()
            .getMessageAsync(requestHeader.getConsumerGroup(), topic, queueId, offset, maxMsgNums, messageFilter)
            .thenCompose(result -> {
                if (result == null) {
                    return CompletableFuture.completedFuture(null);
                }
                // maybe store offset is not correct.
                if (GetMessageStatus.OFFSET_TOO_SMALL.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_OVERFLOW_BADLY.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())) {
                    // commit offset, because the offset is not correct
                    // If offset in store is greater than cq offset, it will cause duplicate messages,
                    // because offset in PopBuffer is not committed.
                    POP_LOGGER.warn("Pop initial offset, because store is no correct, {}, {}->{}", lockKey, atomicOffset.get(), result.getNextBeginOffset());
                    this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), topic, queueId, result.getNextBeginOffset());
                    atomicOffset.set(result.getNextBeginOffset());
                    return this.brokerController.getMessageStore().getMessageAsync(requestHeader.getConsumerGroup(), topic, queueId, atomicOffset.get(),
                        requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size(), messageFilter);
                }
                return CompletableFuture.completedFuture(result);
            }).thenApply(result -> {
                if (result == null) {
                    atomicRestNum.set(brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - atomicOffset.get() + atomicRestNum.get());
                    return atomicRestNum.get();
                }
                if (!result.getMessageMapedList().isEmpty()) {
                    this.brokerController.getBrokerStatsManager().incBrokerGetNums(requestHeader.getTopic(), result.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetNums(requestHeader.getConsumerGroup(), topic, result.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetSize(requestHeader.getConsumerGroup(), topic, result.getBufferTotalSize());

                    Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                        .put(LABEL_TOPIC, requestHeader.getTopic())
                        .put(LABEL_CONSUMER_GROUP, requestHeader.getConsumerGroup())
                        .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(requestHeader.getTopic()) || MQConstants.isSysConsumerGroup(requestHeader.getConsumerGroup()))
                        .put(LABEL_IS_RETRY, isRetry)
                        .build();
                    BrokerMetricsManager.messagesOutTotal.add(result.getMessageCount(), attributes);
                    BrokerMetricsManager.throughputOutTotal.add(result.getBufferTotalSize(), attributes);

                    if (requestHeader.isOrder()) {
                        this.brokerController.getConsumerOrderInfoManager().update(requestHeader.getAttemptId(), isRetry, topic, requestHeader.getConsumerGroup(), queueId, popTime, requestHeader.getInvisibleTime(), result.getMessageQueueOffset(), orderCountInfo);
                        this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), topic, queueId, finalOffset);
                    } else {
                        if (!appendCheckPoint(requestHeader, topic, reviveQid, queueId, finalOffset, result, popTime, this.brokerController.getBrokerConfig().getBrokerName())) {
                            return atomicRestNum.get() + result.getMessageCount();
                        }
                    }
                    ExtraInfoUtil.buildStartOffsetInfo(startOffsetInfo, isRetry, queueId, finalOffset);
                    ExtraInfoUtil.buildMsgOffsetInfo(msgOffsetInfo, isRetry, queueId,
                        result.getMessageQueueOffset());
                } else if ((GetMessageStatus.NO_MATCHED_MESSAGE.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())
                    || GetMessageStatus.MESSAGE_WAS_REMOVING.equals(result.getStatus())
                    || GetMessageStatus.NO_MATCHED_LOGIC_QUEUE.equals(result.getStatus()))
                    && result.getNextBeginOffset() > -1) {
                    PopBufferMergeService popBufferMergeService = brokerController.getBrokerNettyServer().getPopServiceManager().getPopBufferMergeService();
                    popBufferMergeService.addCkMock(requestHeader.getConsumerGroup(), topic, queueId, finalOffset,
                        requestHeader.getInvisibleTime(), popTime, reviveQid, result.getNextBeginOffset(), brokerController.getBrokerConfig().getBrokerName());
//                this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), topic,
//                        queueId, getMessageTmpResult.getNextBeginOffset());
                }

                atomicRestNum.set(result.getMaxOffset() - result.getNextBeginOffset() + atomicRestNum.get());
                String brokerName = brokerController.getBrokerConfig().getBrokerName();
                for (SelectMappedBufferResult mapedBuffer : result.getMessageMapedList()) {
                    // We should not recode buffer when popResponseReturnActualRetryTopic is true or topic is not retry topic
                    if (brokerController.getBrokerConfig().isPopResponseReturnActualRetryTopic() || !isRetry) {
                        getMessageResult.addMessage(mapedBuffer);
                    } else {
                        List<MessageExt> messageExtList = MessageDecoder.decodesBatch(mapedBuffer.getByteBuffer(),
                            true, false, true);
                        mapedBuffer.release();
                        for (MessageExt messageExt : messageExtList) {
                            try {
                                String ckInfo = ExtraInfoUtil.buildExtraInfo(finalOffset, popTime, requestHeader.getInvisibleTime(),
                                    reviveQid, messageExt.getTopic(), brokerName, messageExt.getQueueId(), messageExt.getQueueOffset());
                                messageExt.getProperties().putIfAbsent(MessageConst.PROPERTY_POP_CK, ckInfo);

                                // Set retry message topic to origin topic and clear message store size to recode
                                messageExt.setTopic(requestHeader.getTopic());
                                messageExt.setStoreSize(0);

                                byte[] encode = MessageDecoder.encode(messageExt, false);
                                ByteBuffer buffer = ByteBuffer.wrap(encode);
                                SelectMappedBufferResult tmpResult =
                                    new SelectMappedBufferResult(mapedBuffer.getStartOffset(), buffer, encode.length, null);
                                getMessageResult.addMessage(tmpResult);
                            } catch (Exception e) {
                                POP_LOGGER.error("Exception in recode retry message buffer, topic={}", topic, e);
                            }
                        }
                    }
                }
                this.brokerController.getPopInflightMessageCounter().incrementInFlightMessageNum(
                    topic,
                    requestHeader.getConsumerGroup(),
                    queueId,
                    result.getMessageCount()
                );
                return atomicRestNum.get();
            }).whenComplete((result, throwable) -> {
                if (throwable != null) {
                    POP_LOGGER.error("Pop message error, {}", lockKey, throwable);
                }
                queueLockManager.unLock(lockKey);
            });
    }

    private long getPopOffset(String topic, String group, int queueId, int initMode, boolean init, String lockKey,
        boolean checkResetOffset) {

        long offset = this.brokerController.getConsumerOffsetManager().queryOffset(group, topic, queueId);
        if (offset < 0) {
            offset = this.getInitOffset(topic, group, queueId, initMode, init);
        }

        if (checkResetOffset) {
            Long resetOffset = resetPopOffset(topic, group, queueId);
            if (resetOffset != null) {
                return resetOffset;
            }
        }

        PopBufferMergeService popBufferMergeService = brokerController.getBrokerNettyServer().getPopServiceManager().getPopBufferMergeService();
        long bufferOffset = popBufferMergeService.getLatestOffset(lockKey);
        if (bufferOffset < 0) {
            return offset;
        } else {
            return Math.max(bufferOffset, offset);
        }
    }

    private long getInitOffset(String topic, String group, int queueId, int initMode, boolean init) {
        if (ConsumeInitMode.MIN == initMode) {
            return this.brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId);
        }

        long offset = getInitOffset(topic, queueId);

        if (init) {
            this.brokerController.getConsumerOffsetManager().commitOffset(
                "getPopOffset", group, topic, queueId, offset);
        }
        return offset;
    }

    private long getInitOffset(String topic, int queueId) {
        if (this.brokerController.getBrokerConfig().isInitPopOffsetByCheckMsgInMem() &&
            this.brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId) <= 0 &&
            this.brokerController.getMessageStore().checkInMemByConsumeOffset(topic, queueId, 0, 1)) {
            return  0;
        }

        // pop last one,then commit offset.
        long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - 1;
        // max & no consumer offset
        if (offset < 0) {
            offset = 0;
        }

        return offset;
    }

    private boolean appendCheckPoint(final PopMessageRequestHeader requestHeader,
        final String topic, final int reviveQid, final int queueId, final long offset,
        final GetMessageResult getMessageTmpResult, final long popTime, final String brokerName) {
        // add check point msg to revive log
        final PopCheckPoint ck = new PopCheckPoint();
        ck.setBitMap(0);
        ck.setNum((byte) getMessageTmpResult.getMessageMapedList().size());
        ck.setPopTime(popTime);
        ck.setInvisibleTime(requestHeader.getInvisibleTime());
        ck.setStartOffset(offset);
        ck.setCId(requestHeader.getConsumerGroup());
        ck.setTopic(topic);
        ck.setQueueId(queueId);
        ck.setBrokerName(brokerName);
        for (Long msgQueueOffset : getMessageTmpResult.getMessageQueueOffset()) {
            ck.addDiff((int) (msgQueueOffset - offset));
        }

        PopBufferMergeService popBufferMergeService = brokerController.getBrokerNettyServer().getPopServiceManager().getPopBufferMergeService();
        final boolean addBufferSuc = popBufferMergeService.addCheckPoint(
            ck, reviveQid, -1, getMessageTmpResult.getNextBeginOffset()
        );

        if (addBufferSuc) {
            return true;
        }
        return popBufferMergeService.addCkJustOffset(
            ck, reviveQid, -1, getMessageTmpResult.getNextBeginOffset()
        );
    }

    private Long resetPopOffset(String topic, String group, int queueId) {
        String lockKey = topic + PopConstants.SPLIT + group + PopConstants.SPLIT + queueId;
        Long resetOffset = this.brokerController.getConsumerOffsetManager().queryThenEraseResetOffset(topic, group, queueId);

        if (resetOffset == null) {
            return resetOffset;
        }

        this.brokerController.getConsumerOrderInfoManager().clearBlock(topic, group, queueId);
        this.brokerController.getBrokerNettyServer().getPopServiceManager().getPopBufferMergeService().clearOffsetQueue(lockKey);
        this.brokerController.getConsumerOffsetManager().commitOffset("ResetPopOffset", group, topic, queueId, resetOffset);
        return resetOffset;
    }

    private byte[] readGetMessageResult(final GetMessageResult getMessageResult, final String group, final String topic,
        final int queueId) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());

        long storeTimestamp = 0;
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {

                byteBuffer.put(bb);
                storeTimestamp = bb.getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION);
            }
        } finally {
            getMessageResult.release();
        }

        this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId,
            this.brokerController.getMessageStore().now() - storeTimestamp);
        return byteBuffer.array();
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }

}
