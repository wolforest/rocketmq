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
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.infra.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.infra.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.infra.filter.ExpressionMessageFilter;
import org.apache.rocketmq.broker.server.daemon.longpolling.PollingHeader;
import org.apache.rocketmq.broker.server.daemon.longpolling.PollingResult;
import org.apache.rocketmq.broker.server.daemon.longpolling.PopLongPollingService;
import org.apache.rocketmq.broker.server.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.infra.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.broker.server.daemon.pop.PopBufferMergeService;
import org.apache.rocketmq.broker.server.daemon.pop.QueueLockManager;
import org.apache.rocketmq.common.domain.topic.KeyBuilder;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.domain.constant.ConsumeInitMode;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.constant.PermName;
import org.apache.rocketmq.common.domain.filter.ExpressionType;
import org.apache.rocketmq.common.app.help.FAQUrl;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.topic.TopicValidator;
import org.apache.rocketmq.common.domain.constant.MQConstants;
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
import org.apache.rocketmq.store.api.dto.GetMessageResult;
import org.apache.rocketmq.store.api.dto.GetMessageStatus;
import org.apache.rocketmq.store.infra.file.SelectMappedBufferResult;
import org.apache.rocketmq.store.api.broker.pop.PopCheckPoint;

import static org.apache.rocketmq.broker.server.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.server.metrics.BrokerMetricsConstant.LABEL_IS_RETRY;
import static org.apache.rocketmq.broker.server.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.server.metrics.BrokerMetricsConstant.LABEL_TOPIC;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_REQUEST_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESPONSE_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESULT;

/**
 * Server side rebalance, Pop mode
 * @link https://github.com/apache/rocketmq/wiki/%5BRIP-19%5D-Server-side-rebalance,--lightweight-consumer-client-support
 */
public class PopMessageProcessor implements NettyRequestProcessor {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);

    private final Broker broker;
    private final Random random = new Random(System.currentTimeMillis());
    private static final String BORN_TIME = "bornTime";

    private final AtomicLong ckMessageNumber;

    public PopMessageProcessor(final Broker broker) {
        this.broker = broker;
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
                this.broker.getBrokerConfig().getBrokerIP1()));
            return false;
        }
        if (!PermName.isReadable(this.broker.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[%s] pop message is forbidden",
                this.broker.getBrokerConfig().getBrokerIP1()));
            return false;
        }
        if (requestHeader.getMaxMsgNums() > 32) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("the broker[%s] pop message's num is greater than 32",
                this.broker.getBrokerConfig().getBrokerIP1()));
            return false;
        }

        if (!broker.getMessageStore().getMessageStoreConfig().isTimerWheelEnable()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("the broker[%s] pop message is forbidden because timerWheelEnable is false",
                this.broker.getBrokerConfig().getBrokerIP1()));
            return false;
        }

        TopicConfig topicConfig =
            this.broker.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
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
            this.broker.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
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
        broker.getConsumerManager().compensateBasicConsumerInfo(requestHeader.getConsumerGroup(),
            ConsumeType.CONSUME_POP, MessageModel.CLUSTERING);

        request.addExtFieldIfNotExist(BORN_TIME, String.valueOf(System.currentTimeMillis()));
        if (Objects.equals(request.getExtFields().get(BORN_TIME), "0")) {
            request.addExtField(BORN_TIME, String.valueOf(System.currentTimeMillis()));
        }

        response.setOpaque(request.getOpaque());

        if (broker.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("receive PopMessage request command, {}", request);
        }
    }

    private ExpressionMessageFilter initExpressionMessageFilter(PopMessageRequestHeader requestHeader, RemotingCommand response) {
        try {
            SubscriptionData subscriptionData = FilterAPI.build(requestHeader.getTopic(), requestHeader.getExp(), requestHeader.getExpType());
            broker.getConsumerManager().compensateSubscribeData(requestHeader.getConsumerGroup(), requestHeader.getTopic(), subscriptionData);

            String retryTopic = KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup(), broker.getBrokerConfig().isEnableRetryTopicV2());
            SubscriptionData retrySubscriptionData = FilterAPI.build(retryTopic, SubscriptionData.SUB_ALL, requestHeader.getExpType());
            broker.getConsumerManager().compensateSubscribeData(requestHeader.getConsumerGroup(), retryTopic, retrySubscriptionData);

            ConsumerFilterData consumerFilterData = null;
            if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                return new ExpressionMessageFilter(subscriptionData, consumerFilterData, broker.getConsumerFilterManager());
            }

            consumerFilterData = ConsumerFilterManager.build(requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getExp(), requestHeader.getExpType(), System.currentTimeMillis());
            if (consumerFilterData != null) {
                return new ExpressionMessageFilter(subscriptionData, consumerFilterData, broker.getConsumerFilterManager());
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
            broker.getConsumerManager().compensateSubscribeData(requestHeader.getConsumerGroup(), requestHeader.getTopic(), subscriptionData);

            String retryTopic = KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup(), broker.getBrokerConfig().isEnableRetryTopicV2());
            SubscriptionData retrySubscriptionData = FilterAPI.build(retryTopic, "*", ExpressionType.TAG);
            broker.getConsumerManager().compensateSubscribeData(requestHeader.getConsumerGroup(), retryTopic, retrySubscriptionData);
        } catch (Exception e) {
            POP_LOGGER.warn("Build default subscription error, group: {}", requestHeader.getConsumerGroup());
        }
    }

    private int getReviveQid(PopMessageRequestHeader requestHeader) {
        int reviveQid;
        if (requestHeader.isOrder()) {
            reviveQid = KeyBuilder.POP_ORDER_REVIVE_QUEUE;
        } else {
            reviveQid = (int) Math.abs(ckMessageNumber.getAndIncrement() % this.broker.getBrokerConfig().getReviveQueueNum());
        }

        return reviveQid;
    }

    private CompletableFuture<Long> popMessage(ChannelHandlerContext ctx, PopMessageRequestHeader requestHeader, GetMessageResult getMessageResult, ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder finalOrderCountInfo, int reviveQid, long popTime) {
        int randomQ = random.nextInt(100);
        // Due to the design of the fields startOffsetInfo, msgOffsetInfo, and orderCountInfo,
        // a single POP request could only invoke the popMsgFromQueue method once
        // for either a normal topic or a retry topic's queue. Retry topics v1 and v2 are
        // considered the same type because they share the same retry flag in previous fields.
        // Therefore, needRetryV1 is designed as a subset of needRetry, and within a single request,
        // only one type of retry topic is able to call popMsgFromQueue.
        boolean needRetry = randomQ % 5 == 0;
        boolean needRetryV1 = false;
        if (broker.getBrokerConfig().isEnableRetryTopicV2() && broker.getBrokerConfig().isRetrieveMessageFromPopRetryTopicV1()) {
            needRetryV1 = randomQ % 2 == 0;
        }

        CompletableFuture<Long> getMessageFuture = CompletableFuture.completedFuture(0L);
        if (needRetry && !requestHeader.isOrder()) {
            getMessageFuture = popRetryMessage(needRetryV1, ctx, requestHeader, getMessageResult, messageFilter, startOffsetInfo, msgOffsetInfo, finalOrderCountInfo, reviveQid, popTime, randomQ, getMessageFuture);
        }

        getMessageFuture = popMessage(ctx, requestHeader, getMessageResult, messageFilter, startOffsetInfo, msgOffsetInfo, finalOrderCountInfo, reviveQid, popTime, randomQ, getMessageFuture);

        // if not full , fetch retry again
        if (!needRetry && getMessageResult.getMessageMapedList().size() < requestHeader.getMaxMsgNums() && !requestHeader.isOrder()) {
            getMessageFuture = popRetryMessage(needRetryV1, ctx, requestHeader, getMessageResult, messageFilter, startOffsetInfo, msgOffsetInfo, finalOrderCountInfo, reviveQid, popTime, randomQ, getMessageFuture);
        }

        return getMessageFuture;
    }

    private CompletableFuture<Long> popMessage(ChannelHandlerContext ctx, PopMessageRequestHeader requestHeader, GetMessageResult getMessageResult, ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder finalOrderCountInfo, int reviveQid, long popTime, int randomQ, CompletableFuture<Long> getMessageFuture) {

        TopicConfig topicConfig = this.broker.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (topicConfig == null) {
            return getMessageFuture;
        }

        if (requestHeader.getQueueId() >= 0) {
            return getMessageFuture.thenCompose(restNum -> popMsgFromQueue(topicConfig.getTopicName(), requestHeader.getAttemptId(), false, getMessageResult, requestHeader, requestHeader.getQueueId(), restNum, reviveQid, ctx.channel(), popTime, messageFilter, startOffsetInfo, msgOffsetInfo, finalOrderCountInfo));
        }

        // read all queue
        for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
            int queueId = (randomQ + i) % topicConfig.getReadQueueNums();
            getMessageFuture = getMessageFuture.thenCompose(restNum -> popMsgFromQueue(topicConfig.getTopicName(), requestHeader.getAttemptId(), false, getMessageResult, requestHeader, queueId, restNum, reviveQid, ctx.channel(), popTime, messageFilter, startOffsetInfo, msgOffsetInfo, finalOrderCountInfo));
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
    private CompletableFuture<Long> popRetryMessage(boolean needRetryV1, ChannelHandlerContext ctx, PopMessageRequestHeader requestHeader, GetMessageResult getMessageResult, ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder finalOrderCountInfo, int reviveQid, long popTime, int randomQ, CompletableFuture<Long> getMessageFuture) {

        TopicConfig topicConfig = getTopicConfig(needRetryV1, requestHeader);
        if (topicConfig == null) {
            return getMessageFuture;
        }

        String topic = topicConfig.getTopicName();
        for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
            int queueId = (randomQ + i) % topicConfig.getReadQueueNums();
            getMessageFuture = getMessageFuture.thenCompose(restNum -> popMsgFromQueue(topic, requestHeader.getAttemptId(), true, getMessageResult, requestHeader, queueId, restNum, reviveQid, ctx.channel(), popTime, messageFilter, startOffsetInfo, msgOffsetInfo, finalOrderCountInfo));
        }

        return getMessageFuture;
    }

    private TopicConfig getTopicConfig(boolean needRetryV1, PopMessageRequestHeader requestHeader) {
        TopicConfig topicConfig = null;
        if (needRetryV1) {
            String retryTopic = KeyBuilder.buildPopRetryTopicV1(requestHeader.getTopic(), requestHeader.getConsumerGroup());
            topicConfig = this.broker.getTopicConfigManager().selectTopicConfig(retryTopic);
        } else {
            String retryTopic = KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup(), broker.getBrokerConfig().isEnableRetryTopicV2());
            topicConfig = this.broker.getTopicConfigManager().selectTopicConfig(retryTopic);
        }
        return topicConfig;
    }

    private boolean handlePollingAction(ChannelHandlerContext ctx, RemotingCommand request, PopMessageRequestHeader requestHeader, GetMessageResult getMessageResult, RemotingCommand finalResponse, long restNum) {
        PopLongPollingService popLongPollingService = broker.getBrokerNettyServer().getPopServiceManager().getPopPollingService();

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
        if (this.broker.getBrokerConfig().isTransferMsgByHeap()) {
            final long beginTimeMills = this.broker.getMessageStore().now();
            final byte[] r = this.readGetMessageResult(getMessageResult, requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
            this.broker.getBrokerStatsManager().incGroupGetLatency(requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), (int) (this.broker.getMessageStore().now() - beginTimeMills));
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
        if (finalResponse.getCode() != ResponseCode.SUCCESS) {
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
    private CompletableFuture<Long> popMsgFromQueue(String topic, String attemptId, boolean isRetry, GetMessageResult getMessageResult,
        PopMessageRequestHeader requestHeader, int queueId, long restNum, int reviveQid,
        Channel channel, long popTime, ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder orderCountInfo) {

        // originally initialize offset and getPopOffset here, move to try lock block
        long offset;
        CompletableFuture<Long> future = new CompletableFuture<>();
        String lockKey = KeyBuilder.buildConsumeKey(topic, requestHeader.getConsumerGroup(), queueId);

        // try lock
        QueueLockManager queueLockManager = broker.getBrokerNettyServer().getPopServiceManager().getQueueLockManager();
        if (!queueLockManager.tryLock(lockKey)) {
            // move from offset initialization
            offset = getPopOffset(topic, requestHeader.getConsumerGroup(), queueId, requestHeader.getInitMode(), false, lockKey, false);

            restNum = this.broker.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
            future.complete(restNum);
            return future;
        }

        try {
            future.whenComplete((result, throwable) -> queueLockManager.unLock(lockKey));
            offset = getPopOffset(topic, requestHeader.getConsumerGroup(), queueId, requestHeader.getInitMode(), true, lockKey, true);

            if (requestHeader.isOrder() && broker.getConsumerOrderInfoManager().checkBlock(attemptId, topic,
                requestHeader.getConsumerGroup(), queueId, requestHeader.getInvisibleTime())) {
                future.complete(this.broker.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum);
                return future;
            }

            if (requestHeader.isOrder()) {
                this.broker.getPopInflightMessageCounter().clearInFlightMessageNum(topic, requestHeader.getConsumerGroup(), queueId);
            }

            if (getMessageResult.getMessageMapedList().size() >= requestHeader.getMaxMsgNums()) {
                restNum = this.broker.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
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
        return this.broker.getMessageStore()
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
                    this.broker.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), topic, queueId, result.getNextBeginOffset());
                    atomicOffset.set(result.getNextBeginOffset());
                    return this.broker.getMessageStore().getMessageAsync(requestHeader.getConsumerGroup(), topic, queueId, atomicOffset.get(), requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size(), messageFilter);
                }
                return CompletableFuture.completedFuture(result);
            }).thenApply(result -> {
                if (result == null) {
                    atomicRestNum.set(broker.getMessageStore().getMaxOffsetInQueue(topic, queueId) - atomicOffset.get() + atomicRestNum.get());
                    return atomicRestNum.get();
                }

                if (result.getMessageMapedList().isEmpty()) {
                    handleEmptyGetResult(result, requestHeader, topic, queueId, reviveQid, popTime, finalOffset);
                } else {
                    updatePopMetrics(result, requestHeader, topic, isRetry);

                    if (requestHeader.isOrder()) {
                        this.broker.getConsumerOrderInfoManager().update(requestHeader.getAttemptId(), isRetry, topic, requestHeader.getConsumerGroup(), queueId, popTime, requestHeader.getInvisibleTime(), result.getMessageQueueOffset(), orderCountInfo);
                        this.broker.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), topic, queueId, finalOffset);
                    } else {
                        if (!appendCheckPoint(requestHeader, topic, reviveQid, queueId, finalOffset, result, popTime, this.broker.getBrokerConfig().getBrokerName())) {
                            return atomicRestNum.get() + result.getMessageCount();
                        }
                    }
                    ExtraInfoUtil.buildStartOffsetInfo(startOffsetInfo, topic, queueId, finalOffset);
                    ExtraInfoUtil.buildMsgOffsetInfo(msgOffsetInfo, topic, queueId, result.getMessageQueueOffset());
                }

                atomicRestNum.set(result.getMaxOffset() - result.getNextBeginOffset() + atomicRestNum.get());

                parseGetResult(result, getMessageResult, requestHeader, topic, isRetry, reviveQid, popTime, finalOffset);

                this.broker.getPopInflightMessageCounter().incrementInFlightMessageNum(topic, requestHeader.getConsumerGroup(), queueId, result.getMessageCount());
                return atomicRestNum.get();
            }).whenComplete((result, throwable) -> {
                if (throwable != null) {
                    POP_LOGGER.error("Pop message error, {}", lockKey, throwable);
                }
                queueLockManager.unLock(lockKey);
            });
    }

    private void updatePopMetrics(GetMessageResult result, PopMessageRequestHeader requestHeader, String topic, boolean isRetry) {
        this.broker.getBrokerStatsManager().incBrokerGetNums(requestHeader.getTopic(), result.getMessageCount());
        this.broker.getBrokerStatsManager().incGroupGetNums(requestHeader.getConsumerGroup(), topic, result.getMessageCount());
        this.broker.getBrokerStatsManager().incGroupGetSize(requestHeader.getConsumerGroup(), topic, result.getBufferTotalSize());

        Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
            .put(LABEL_TOPIC, requestHeader.getTopic())
            .put(LABEL_CONSUMER_GROUP, requestHeader.getConsumerGroup())
            .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(requestHeader.getTopic()) || MQConstants.isSysConsumerGroup(requestHeader.getConsumerGroup()))
            .put(LABEL_IS_RETRY, isRetry)
            .build();
        BrokerMetricsManager.messagesOutTotal.add(result.getMessageCount(), attributes);
        BrokerMetricsManager.throughputOutTotal.add(result.getBufferTotalSize(), attributes);
    }

    private void handleEmptyGetResult(GetMessageResult result, PopMessageRequestHeader requestHeader, String topic, int queueId, int reviveQid, long popTime, long finalOffset) {
        if ((GetMessageStatus.NO_MATCHED_MESSAGE.equals(result.getStatus())
            || GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())
            || GetMessageStatus.MESSAGE_WAS_REMOVING.equals(result.getStatus())
            || GetMessageStatus.NO_MATCHED_LOGIC_QUEUE.equals(result.getStatus()))
            && result.getNextBeginOffset() > -1) {

            PopBufferMergeService popBufferMergeService = broker.getBrokerNettyServer().getPopServiceManager().getPopBufferMergeService();
            popBufferMergeService.mockCheckPoint(requestHeader.getConsumerGroup(), topic, queueId, finalOffset,
                requestHeader.getInvisibleTime(), popTime, reviveQid, result.getNextBeginOffset(), broker.getBrokerConfig().getBrokerName());
//                this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), topic,
//                        queueId, getMessageTmpResult.getNextBeginOffset());
        }
    }

    private void parseGetResult(GetMessageResult result, GetMessageResult getMessageResult, PopMessageRequestHeader requestHeader, String topic, boolean isRetry, int reviveQid, long popTime, long finalOffset) {
        String brokerName = broker.getBrokerConfig().getBrokerName();
        for (SelectMappedBufferResult mappedBuffer : result.getMessageMapedList()) {
            // We should not recode buffer when popResponseReturnActualRetryTopic is true or topic is not retry topic
            if (broker.getBrokerConfig().isPopResponseReturnActualRetryTopic() || !isRetry) {
                getMessageResult.addMessage(mappedBuffer);
                continue;
            }

            List<MessageExt> messageExtList = MessageDecoder.decodesBatch(mappedBuffer.getByteBuffer(),true, false, true);
            mappedBuffer.release();
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
                        new SelectMappedBufferResult(mappedBuffer.getStartOffset(), buffer, encode.length, null);
                    getMessageResult.addMessage(tmpResult);
                } catch (Exception e) {
                    POP_LOGGER.error("Exception in recode retry message buffer, topic={}", topic, e);
                }
            }

        }
    }

    /**
     * get consume offset for pop mode
     *
     * @param topic topic
     * @param group group
     * @param queueId queueId
     * @param initMode initMode ConsumeInitMode.MAX for pop mode
     * @param init flag of whether commit offset the first time pop message
     * @param lockKey lockKey
     * @param checkResetOffset flag of whether resetPopOffset
     * @return offset
     */
    private long getPopOffset(String topic, String group, int queueId, int initMode, boolean init, String lockKey,
        boolean checkResetOffset) {

        long offset = this.broker.getConsumerOffsetManager().queryOffset(group, topic, queueId);
        if (offset < 0) {
            //the first time consume, pop the latest message
            offset = this.getInitOffset(topic, group, queueId, initMode, init);
        }

        if (checkResetOffset) {
            // admin related feature, can ignore
            Long resetOffset = resetPopOffset(topic, group, queueId);
            if (resetOffset != null) {
                return resetOffset;
            }
        }

        PopBufferMergeService popBufferMergeService = broker.getBrokerNettyServer().getPopServiceManager().getPopBufferMergeService();
        long bufferOffset = popBufferMergeService.getLatestOffset(lockKey);
        if (bufferOffset < 0) {
            return offset;
        }

        return Math.max(bufferOffset, offset);
    }

    private long getInitOffset(String topic, String group, int queueId, int initMode, boolean init) {
        if (ConsumeInitMode.MIN == initMode) {
            return this.broker.getMessageStore().getMinOffsetInQueue(topic, queueId);
        }

        long offset = getMaxOffset(topic, queueId);

        if (init) {
            this.broker.getConsumerOffsetManager().commitOffset(
                "getPopOffset", group, topic, queueId, offset);
        }
        return offset;
    }

    private long getMaxOffset(String topic, int queueId) {
        if (this.broker.getBrokerConfig().isInitPopOffsetByCheckMsgInMem() &&
            this.broker.getMessageStore().getMinOffsetInQueue(topic, queueId) <= 0 &&
            this.broker.getMessageStore().checkInMemByConsumeOffset(topic, queueId, 0, 1)) {
            return  0;
        }

        // pop last one,then commit offset.
        long offset = this.broker.getMessageStore().getMaxOffsetInQueue(topic, queueId) - 1;
        // max & no consumer offset
        if (offset < 0) {
            offset = 0;
        }

        return offset;
    }

    private boolean appendCheckPoint(final PopMessageRequestHeader requestHeader, final String topic, final int reviveQid,
        final int queueId, final long offset, final GetMessageResult getMessageTmpResult, final long popTime, final String brokerName) {

        final PopCheckPoint ck = buildCheckPoint(requestHeader, topic, queueId, offset, getMessageTmpResult, popTime, brokerName);

        // add check point msg to revive log
        PopBufferMergeService ackService = broker.getBrokerNettyServer().getPopServiceManager().getPopBufferMergeService();
        if (ackService.addCheckPoint(ck, reviveQid, -1, getMessageTmpResult.getNextBeginOffset())) {
            return true;
        }

        return ackService.storeCheckPoint(ck, reviveQid, -1, getMessageTmpResult.getNextBeginOffset());
    }

    private PopCheckPoint buildCheckPoint(final PopMessageRequestHeader requestHeader, final String topic, final int queueId,
        final long offset, final GetMessageResult getMessageTmpResult, final long popTime, final String brokerName) {

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

        return ck;
    }

    private Long resetPopOffset(String topic, String group, int queueId) {
        String lockKey = KeyBuilder.buildConsumeKey(topic, group, queueId);
        Long resetOffset = this.broker.getConsumerOffsetManager().queryThenEraseResetOffset(topic, group, queueId);

        if (resetOffset == null) {
            return resetOffset;
        }

        this.broker.getConsumerOrderInfoManager().clearBlock(topic, group, queueId);
        this.broker.getBrokerNettyServer().getPopServiceManager().getPopBufferMergeService().clearOffsetQueue(lockKey);
        this.broker.getConsumerOffsetManager().commitOffset("ResetPopOffset", group, topic, queueId, resetOffset);
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

        this.broker.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId,
            this.broker.getMessageStore().now() - storeTimestamp);
        return byteBuffer.array();
    }

    public Broker getBrokerController() {
        return broker;
    }

}
