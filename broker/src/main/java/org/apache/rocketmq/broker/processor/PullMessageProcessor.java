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
import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.Objects;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filter.ExpressionForRetryMessageFilter;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.broker.longpolling.PullRequest;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.common.topic.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.common.constant.MQConstants;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.ForbiddenType;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.RequestSource;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingContext;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.rpc.RpcClientUtils;
import org.apache.rocketmq.remoting.rpc.RpcRequest;
import org.apache.rocketmq.remoting.rpc.RpcResponse;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import static org.apache.rocketmq.remoting.protocol.RemotingCommand.buildErrorResponse;

public class PullMessageProcessor implements NettyRequestProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private List<ConsumeMessageHook> consumeMessageHookList;
    private PullMessageResultHandler pullMessageResultHandler;
    private final BrokerController brokerController;

    public PullMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.pullMessageResultHandler = new DefaultPullMessageResultHandler(brokerController);
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        return this.processRequest(ctx.channel(), request, true, true);
    }

    @Override
    public boolean rejectRequest() {
        return !this.brokerController.getBrokerConfig().isSlaveReadEnable() && this.brokerController.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE;
    }

    public void registerConsumeMessageHook(List<ConsumeMessageHook> consumeMessageHookList) {
        this.consumeMessageHookList = consumeMessageHookList;
    }

    public void setPullMessageResultHandler(PullMessageResultHandler pullMessageResultHandler) {
        this.pullMessageResultHandler = pullMessageResultHandler;
    }

    public RemotingCommand processRequest(final Channel channel, RemotingCommand request, boolean brokerAllowSuspend, boolean brokerAllowFlowCtrSuspend) throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(PullMessageResponseHeader.class);
        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();
        final PullMessageRequestHeader requestHeader = (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);
        response.setOpaque(request.getOpaque());
        LOGGER.debug("receive PullMessage request command, {}", request);

        if (!allowAccess(response, responseHeader, channel, request, requestHeader)) {
            return response;
        }

        SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        TopicQueueMappingContext mappingContext = this.brokerController.getTopicQueueMappingManager().buildTopicQueueMappingContext(requestHeader, false);

        RemotingCommand rewriteResult = rewriteRequestForStaticTopic(requestHeader, mappingContext);
        if (rewriteResult != null) {
            return rewriteResult;
        }

        compensateBasicConsumerInfo(brokerController.getConsumerManager(), requestHeader);
        SubscriptionData subscriptionData = buildSubscriptionData(subscriptionGroupConfig, requestHeader, response, responseHeader);
        if (null == subscriptionData) {
            return response;
        }

        ConsumerFilterData consumerFilterData = null;
        if (!ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            consumerFilterData = buildConsumerFilterData(requestHeader, response);
            if (consumerFilterData == null) {
                return response;
            }
        }

        if (!ExpressionType.isTagType(subscriptionData.getExpressionType()) && !this.brokerController.getBrokerConfig().isEnablePropertyFilter()) {
            return response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "The broker does not support consumer to filter message by " + subscriptionData.getExpressionType());
        }

        MessageFilter messageFilter = initMessageFilter(subscriptionData, consumerFilterData);
        if (!checkStoreRules(request, requestHeader, channel, response, brokerAllowFlowCtrSuspend, subscriptionData, messageFilter)) {
            return response;
        }

        final boolean useResetOffsetFeature = brokerController.getBrokerConfig().isUseServerSideResetOffset();
        Long resetOffset = brokerController.getConsumerOffsetManager().queryThenEraseResetOffset(requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueId());
        if (useResetOffsetFeature && null != resetOffset) {
            return handlePullResult(resetOffset, request, requestHeader, channel, subscriptionData, subscriptionGroupConfig, brokerAllowSuspend, messageFilter, response, mappingContext);
        }

        long broadcastInitOffset = queryBroadcastPullInitOffset(requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueId(), requestHeader, channel);
        if (broadcastInitOffset >= 0) {
            return handleBroadcastResult(broadcastInitOffset, request, requestHeader, channel, subscriptionData, subscriptionGroupConfig, brokerAllowSuspend, messageFilter, response, mappingContext);
        }

        return handleAsyncResult(request, requestHeader, channel, subscriptionData, subscriptionGroupConfig, brokerAllowSuspend, messageFilter, response, mappingContext);
    }

    private void prepareRequestHeader(PullMessageRequestHeader requestHeader, LogicQueueMappingItem mappingItem) {
        Integer phyQueueId = mappingItem.getQueueId();
        Long phyQueueOffset = mappingItem.computePhysicalQueueOffset(requestHeader.getQueueOffset());
        requestHeader.setQueueId(phyQueueId);
        requestHeader.setQueueOffset(phyQueueOffset);
        if (mappingItem.checkIfEndOffsetDecided()
                && requestHeader.getMaxMsgNums() != null) {
            requestHeader.setMaxMsgNums((int) Math.min(mappingItem.getEndOffset() - mappingItem.getStartOffset(), requestHeader.getMaxMsgNums()));
        }

        int sysFlag = requestHeader.getSysFlag();
        requestHeader.setLo(false);
        requestHeader.setBname(mappingItem.getBname());
        sysFlag = PullSysFlag.clearSuspendFlag(sysFlag);
        sysFlag = PullSysFlag.clearCommitOffsetFlag(sysFlag);
        requestHeader.setSysFlag(sysFlag);
    }

    /**
     * static topic handler
     * @link docs/cn/statictopic/RocketMQ_Static_Topic_Logic_Queue_design.md
     * @param requestHeader requestHeader
     * @param mappingContext mappingContext
     * @return Response
     */
    private RemotingCommand rewriteRequestForStaticTopic(PullMessageRequestHeader requestHeader, TopicQueueMappingContext mappingContext) {
        try {
            if (mappingContext.getMappingDetail() == null) {
                return null;
            }
            // if the leader? consider the order consumer, which will lock the mq
            if (!mappingContext.isLeader()) {
                return buildErrorResponse(ResponseCode.NOT_LEADER_FOR_QUEUE, String.format("%s-%d cannot find mapping item in request process of current broker %s", mappingContext.getTopic(), mappingContext.getGlobalId(), mappingContext.getMappingDetail().getBname()));
            }

            LogicQueueMappingItem mappingItem = TopicQueueMappingUtils.findLogicQueueMappingItem(mappingContext.getMappingItemList(), requestHeader.getQueueOffset(), true);
            mappingContext.setCurrentItem(mappingItem);

            if (requestHeader.getQueueOffset() < mappingItem.getLogicOffset()) {
                //handleOffsetMoved
                //If the physical queue is reused, we should handle the PULL_OFFSET_MOVED independently
                //Otherwise, we could just transfer it to the physical process
            }

            if (mappingContext.getMappingDetail().getBname().equals(mappingItem.getBname())) {
                //just let it go, do the local pull process
                return null;
            }

            //below are physical info
            prepareRequestHeader(requestHeader, mappingItem);

            RpcRequest rpcRequest = new RpcRequest(RequestCode.PULL_MESSAGE, requestHeader, null);
            RpcResponse rpcResponse = this.brokerController.getBrokerOuterAPI().getRpcClient().invoke(rpcRequest, this.brokerController.getBrokerConfig().getForwardTimeout()).get();
            if (rpcResponse.getException() != null) {
                throw rpcResponse.getException();
            }

            PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) rpcResponse.getHeader();
            RemotingCommand rewriteResult = rewriteResponseForStaticTopic(requestHeader, responseHeader, mappingContext, rpcResponse.getCode());
            if (rewriteResult != null) {
                return rewriteResult;
            }

            return RpcClientUtils.createCommandForRpcResponse(rpcResponse);
        } catch (Throwable t) {
            LOGGER.warn("", t);
            return buildErrorResponse(ResponseCode.SYSTEM_ERROR, t.toString());
        }
    }

    /**
     * static topic handler
     * @link docs/cn/statictopic/RocketMQ_Static_Topic_Logic_Queue_design.md
     * @param requestHeader requestHeader
     * @param responseHeader responseHeader
     * @param mappingContext mappingContext
     * @param code int
     * @return Response
     */
    protected RemotingCommand rewriteResponseForStaticTopic(PullMessageRequestHeader requestHeader, PullMessageResponseHeader responseHeader, TopicQueueMappingContext mappingContext, final int code) {
        try {
            if (mappingContext.getMappingDetail() == null) {
                return null;
            }

            PullRewriteContext offsetMap = calculateOffsetMap(requestHeader, responseHeader, mappingContext, code);

            responseHeader.setNextBeginOffset(offsetMap.getCurrentItem().computeStaticQueueOffsetStrictly(offsetMap.getNextBeginOffset()));
            //handle min offset
            responseHeader.setMinOffset(offsetMap.getCurrentItem().computeStaticQueueOffsetStrictly(Math.max(offsetMap.getCurrentItem().getStartOffset(), offsetMap.getMinOffset())));
            //handle max offset
            responseHeader.setMaxOffset(Math.max(offsetMap.getCurrentItem().computeStaticQueueOffsetStrictly(offsetMap.getMaxOffset()),
                    TopicQueueMappingDetail.computeMaxOffsetFromMapping(mappingContext.getMappingDetail(), mappingContext.getGlobalId())));
            //set the offsetDelta
            responseHeader.setOffsetDelta(offsetMap.getCurrentItem().computeOffsetDelta());

            if (code != ResponseCode.SUCCESS) {
                return RemotingCommand.createResponseCommandWithHeader(offsetMap.getResponseCode(), responseHeader);
            } else {
                return null;
            }
        } catch (Throwable t) {
            LOGGER.warn("", t);
            return buildErrorResponse(ResponseCode.SYSTEM_ERROR, t.toString());
        }
    }

    private PullRewriteContext initOffsetMap(PullMessageRequestHeader requestHeader, PullMessageResponseHeader responseHeader, int code, LogicQueueMappingItem currentItem) {
        PullRewriteContext offsetMap = new PullRewriteContext();
        offsetMap.setRequestOffset(requestHeader.getQueueOffset());
        offsetMap.setNextBeginOffset(responseHeader.getNextBeginOffset());
        offsetMap.setMinOffset(responseHeader.getMinOffset());
        offsetMap.setMaxOffset(responseHeader.getMaxOffset());
        offsetMap.setResponseCode(code);
        offsetMap.setCurrentItem(currentItem);

        return offsetMap;
    }

    private void calculateLeaderOffset(PullRewriteContext offsetMap, TopicQueueMappingContext mappingContext, final int code) {
        if (mappingContext.getLeaderItem().getGen() != offsetMap.getCurrentItem().getGen()) {
            return;
        }

        //read the leader
        if (offsetMap.getRequestOffset() > offsetMap.getMaxOffset()) {
            //actually, we need do nothing, but keep the code structure here
            if (code == ResponseCode.PULL_OFFSET_MOVED) {
                offsetMap.setResponseCode(ResponseCode.PULL_OFFSET_MOVED);
                offsetMap.setNextBeginOffset(offsetMap.getMaxOffset());
            } else {
                //maybe current broker is the slave
                offsetMap.setResponseCode(code);
            }
        } else if (offsetMap.getRequestOffset() < offsetMap.getMinOffset()) {
            offsetMap.setNextBeginOffset(offsetMap.getMinOffset());
            offsetMap.setResponseCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
        } else {
            offsetMap.setResponseCode(code);
        }
    }

    private boolean calculateEarliestOffset(PullRewriteContext offsetMap, LogicQueueMappingItem earlistItem, TopicQueueMappingContext mappingContext, final int code) {
        if (earlistItem.getGen() != offsetMap.getCurrentItem().getGen()) {
            return false;
        }

        boolean isRevised = false;
        //note the currentItem maybe both the leader and  the earliest
        //read the earliest one
        if (offsetMap.getRequestOffset() < offsetMap.getMinOffset()) {
            if (code == ResponseCode.PULL_OFFSET_MOVED) {
                offsetMap.setResponseCode(ResponseCode.PULL_OFFSET_MOVED);
                offsetMap.setNextBeginOffset(offsetMap.getMinOffset());
            } else {
                //maybe read from slave, but we still set it to moved
                offsetMap.setResponseCode(ResponseCode.PULL_OFFSET_MOVED);
                offsetMap.setNextBeginOffset(offsetMap.getMinOffset());
            }
        } else if (offsetMap.getRequestOffset() >= offsetMap.getMaxOffset()) {
            //just move to another item
            LogicQueueMappingItem nextItem = TopicQueueMappingUtils.findNext(mappingContext.getMappingItemList(), offsetMap.getCurrentItem(), true);
            if (nextItem != null) {
                isRevised = true;
                offsetMap.setCurrentItem(nextItem);
                offsetMap.setNextBeginOffset(offsetMap.getCurrentItem().getStartOffset());
                offsetMap.setMinOffset(offsetMap.getCurrentItem().getStartOffset());
                offsetMap.setMaxOffset(offsetMap.getMinOffset());
                offsetMap.setResponseCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
            } else {
                //maybe the next one's logic offset is -1
                offsetMap.setResponseCode(ResponseCode.PULL_NOT_FOUND);
            }
        } else {
            //let it go
            offsetMap.setResponseCode(code);
        }

        return isRevised;
    }

    private void calculateMiddleOffset(PullRewriteContext offsetMap, LogicQueueMappingItem earlistItem, TopicQueueMappingContext mappingContext, final int code, boolean isRevised) {
        if (isRevised || mappingContext.getLeaderItem().getGen() == offsetMap.getCurrentItem().getGen()
                || earlistItem.getGen() == offsetMap.getCurrentItem().getGen()) {
            return;
        }

        //read from the middle item, ignore the PULL_OFFSET_MOVED
        if (offsetMap.getRequestOffset() < offsetMap.getMinOffset()) {
            offsetMap.setNextBeginOffset(offsetMap.getMinOffset());
            offsetMap.setResponseCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
        } else if (offsetMap.getRequestOffset() >= offsetMap.getMaxOffset()) {
            //just move to another item
            LogicQueueMappingItem nextItem = TopicQueueMappingUtils.findNext(mappingContext.getMappingItemList(), offsetMap.getCurrentItem(), true);
            if (nextItem != null) {
                offsetMap.setCurrentItem(nextItem);
                offsetMap.setNextBeginOffset(offsetMap.getCurrentItem().getStartOffset());
                offsetMap.setMinOffset(offsetMap.getCurrentItem().getStartOffset());
                offsetMap.setMaxOffset(offsetMap.getMinOffset());
                offsetMap.setResponseCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
            } else {
                //maybe the next one's logic offset is -1
                offsetMap.setResponseCode(ResponseCode.PULL_NOT_FOUND);
            }
        } else {
            offsetMap.setResponseCode(code);
        }
    }

    private PullRewriteContext calculateOffsetMap(PullMessageRequestHeader requestHeader, PullMessageResponseHeader responseHeader, TopicQueueMappingContext mappingContext, final int code) {
        LogicQueueMappingItem earlistItem = TopicQueueMappingUtils.findLogicQueueMappingItem(mappingContext.getMappingItemList(), 0L, true);

        PullRewriteContext offsetMap = initOffsetMap(requestHeader, responseHeader, code, mappingContext.getCurrentItem());

        assert mappingContext.getCurrentItem().getLogicOffset() >= 0;

        //consider the following situations
        // 1. read from slave, currently not supported
        // 2. the middle queue is truncated because of deleting commitlog
        if (code != ResponseCode.SUCCESS) {
            //note the currentItem maybe both the leader and  the earliest
            calculateLeaderOffset(offsetMap, mappingContext, code);
            boolean isRevised = calculateEarliestOffset(offsetMap, earlistItem, mappingContext, code);
            calculateMiddleOffset(offsetMap, earlistItem, mappingContext, code, isRevised);
        }

        //handle nextBeginOffset
        //the next begin offset should no more than the end offset
        if (offsetMap.getCurrentItem().checkIfEndOffsetDecided() && offsetMap.getNextBeginOffset() >= offsetMap.getCurrentItem().getEndOffset()) {
            offsetMap.setNextBeginOffset(offsetMap.getCurrentItem().getEndOffset());
        }

        return offsetMap;
    }

    private boolean allowAccess(RemotingCommand response, PullMessageResponseHeader responseHeader, Channel channel, RemotingCommand request, PullMessageRequestHeader requestHeader) {
        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            responseHeader.setForbiddenType(ForbiddenType.BROKER_FORBIDDEN);
            response.setCodeAndRemark(ResponseCode.NO_PERMISSION, String.format("the broker[%s] pulling message is forbidden",
                    this.brokerController.getBrokerConfig().getBrokerIP1()));
            return false;
        }

        if (request.getCode() == RequestCode.LITE_PULL_MESSAGE && !this.brokerController.getBrokerConfig().isLitePullMessageEnable()) {
            responseHeader.setForbiddenType(ForbiddenType.BROKER_FORBIDDEN);
            response.setCodeAndRemark(ResponseCode.NO_PERMISSION,
                    "the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1() + "] for lite pull consumer is forbidden");
            return false;
        }

        SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        if (null == subscriptionGroupConfig) {
            response.setCodeAndRemark(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST, String.format("subscription group [%s] does not exist, %s", requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return false;
        }

        if (!subscriptionGroupConfig.isConsumeEnable()) {
            responseHeader.setForbiddenType(ForbiddenType.GROUP_FORBIDDEN);
            response.setCodeAndRemark(ResponseCode.NO_PERMISSION, "subscription group no permission, " + requestHeader.getConsumerGroup());
            return false;
        }

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            LOGGER.error("the topic {} not exist, consumer: {}", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCodeAndRemark(ResponseCode.TOPIC_NOT_EXIST, String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return false;
        }

        if (!PermName.isReadable(topicConfig.getPerm())) {
            responseHeader.setForbiddenType(ForbiddenType.TOPIC_FORBIDDEN);
            response.setCodeAndRemark(ResponseCode.NO_PERMISSION, "the topic[" + requestHeader.getTopic() + "] pulling message is forbidden");
            return false;
        }

        if (requestHeader.getQueueId() < 0 || requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]", requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
            LOGGER.warn(errorInfo);
            response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, errorInfo);
            return false;
        }

        return true;
    }

    private void compensateBasicConsumerInfo(ConsumerManager consumerManager, PullMessageRequestHeader requestHeader) {
        switch (RequestSource.parseInteger(requestHeader.getRequestSource())) {
            case PROXY_FOR_BROADCAST:
                consumerManager.compensateBasicConsumerInfo(requestHeader.getConsumerGroup(), ConsumeType.CONSUME_PASSIVELY, MessageModel.BROADCASTING);
                break;
            case PROXY_FOR_STREAM:
                consumerManager.compensateBasicConsumerInfo(requestHeader.getConsumerGroup(), ConsumeType.CONSUME_ACTIVELY, MessageModel.CLUSTERING);
                break;
            default:
                consumerManager.compensateBasicConsumerInfo(requestHeader.getConsumerGroup(), ConsumeType.CONSUME_PASSIVELY, MessageModel.CLUSTERING);
                break;
        }
    }

    private MessageFilter initMessageFilter(SubscriptionData subscriptionData, ConsumerFilterData consumerFilterData) {
        MessageFilter messageFilter;
        if (this.brokerController.getBrokerConfig().isFilterSupportRetry()) {
            messageFilter = new ExpressionForRetryMessageFilter(subscriptionData, consumerFilterData, this.brokerController.getConsumerFilterManager());
        } else {
            messageFilter = new ExpressionMessageFilter(subscriptionData, consumerFilterData, this.brokerController.getConsumerFilterManager());
        }

        return messageFilter;
    }

    private boolean checkStoreRules(RemotingCommand request, PullMessageRequestHeader requestHeader, Channel channel, RemotingCommand response, boolean brokerAllowFlowCtrSuspend, SubscriptionData subscriptionData, MessageFilter messageFilter) {
        if (!(this.brokerController.getMessageStore() instanceof DefaultMessageStore)) {
            return true;
        }

        DefaultMessageStore defaultMessageStore = (DefaultMessageStore) this.brokerController.getMessageStore();
        boolean cgNeedColdDataFlowCtr = brokerController.getColdDataCgCtrService().isCgNeedColdDataFlowCtr(requestHeader.getConsumerGroup());
        if (!cgNeedColdDataFlowCtr) {
            return true;
        }

        boolean isMsgLogicCold = defaultMessageStore.getCommitLog().getColdDataCheckService().isMsgInColdArea(requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getQueueOffset());
        if (!isMsgLogicCold) {
            return true;
        }

        ConsumeType consumeType = this.brokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup()).getConsumeType();
        if (consumeType == ConsumeType.CONSUME_PASSIVELY) {
            response.setCodeAndRemark(ResponseCode.SYSTEM_BUSY, "This consumer group is reading cold data. It has been flow control");
            return false;
        }

        if (consumeType != ConsumeType.CONSUME_ACTIVELY) {
            return true;
        }

        if (brokerAllowFlowCtrSuspend) {  // second arrived, which will not be held
            PullRequest pullRequest = new PullRequest(request, channel, 1000, this.brokerController.getMessageStore().now(), requestHeader.getQueueOffset(), subscriptionData, messageFilter);
            this.brokerController.getColdDataPullRequestHoldService().suspendColdDataReadRequest(pullRequest);

            response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "second arrived, It has been flow control");
            return false;
        }

        requestHeader.setMaxMsgNums(1);
        return true;
    }

    private ConsumerGroupInfo getConsumerGroupInfo(SubscriptionGroupConfig subscriptionGroupConfig, PullMessageRequestHeader requestHeader, RemotingCommand response, PullMessageResponseHeader responseHeader) {
        ConsumerGroupInfo consumerGroupInfo = this.brokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup());
        if (null == consumerGroupInfo) {
            LOGGER.warn("the consumer's group info not exist, group: {}", requestHeader.getConsumerGroup());
            response.setCodeAndRemark(ResponseCode.SUBSCRIPTION_NOT_EXIST, "the consumer's group info not exist" + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
            return null;
        }

        if (!subscriptionGroupConfig.isConsumeBroadcastEnable() && consumerGroupInfo.getMessageModel() == MessageModel.BROADCASTING) {
            responseHeader.setForbiddenType(ForbiddenType.BROADCASTING_DISABLE_FORBIDDEN);
            response.setCodeAndRemark(ResponseCode.NO_PERMISSION, "the consumer group[" + requestHeader.getConsumerGroup() + "] can not consume by broadcast way");
            return null;
        }

        boolean readForbidden = this.brokerController.getSubscriptionGroupManager().getForbidden(subscriptionGroupConfig.getGroupName(), requestHeader.getTopic(), PermName.INDEX_PERM_READ);
        if (readForbidden) {
            responseHeader.setForbiddenType(ForbiddenType.SUBSCRIPTION_FORBIDDEN);
            response.setCodeAndRemark(ResponseCode.NO_PERMISSION, "the consumer group[" + requestHeader.getConsumerGroup() + "] is forbidden for topic[" + requestHeader.getTopic() + "]");
            return null;
        }

        return consumerGroupInfo;
    }

    private SubscriptionData buildSubscriptionData(SubscriptionGroupConfig subscriptionGroupConfig, PullMessageRequestHeader requestHeader, RemotingCommand response, PullMessageResponseHeader responseHeader) {
        if (PullSysFlag.hasSubscriptionFlag(requestHeader.getSysFlag())) {
            return getSubscriptionData(requestHeader, response);
        }

        return getSubscriptionData(subscriptionGroupConfig, requestHeader, response, responseHeader);
    }

    private SubscriptionData getSubscriptionData(SubscriptionGroupConfig subscriptionGroupConfig, PullMessageRequestHeader requestHeader, RemotingCommand response, PullMessageResponseHeader responseHeader) {
        ConsumerGroupInfo consumerGroupInfo = getConsumerGroupInfo(subscriptionGroupConfig, requestHeader, response, responseHeader);
        if (null == consumerGroupInfo) {
            return null;
        }

        SubscriptionData subscriptionData = consumerGroupInfo.findSubscriptionData(requestHeader.getTopic());
        if (null == subscriptionData) {
            LOGGER.warn("the consumer's subscription not exist, group: {}, topic:{}", requestHeader.getConsumerGroup(), requestHeader.getTopic());
            response.setCodeAndRemark(ResponseCode.SUBSCRIPTION_NOT_EXIST,"the consumer's subscription not exist" + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
            return null;
        }

        if (subscriptionData.getSubVersion() < requestHeader.getSubVersion()) {
            LOGGER.warn("The broker's subscription is not latest, group: {} {}", requestHeader.getConsumerGroup(), subscriptionData.getSubString());
            response.setCodeAndRemark(ResponseCode.SUBSCRIPTION_NOT_LATEST, "the consumer's subscription not latest");
            return null;
        }

        return subscriptionData;
    }

    private SubscriptionData getSubscriptionData(PullMessageRequestHeader requestHeader, RemotingCommand response) {
        try {
            SubscriptionData subscriptionData = FilterAPI.build(requestHeader.getTopic(), requestHeader.getSubscription(), requestHeader.getExpressionType());
            ConsumerManager consumerManager = brokerController.getConsumerManager();
            consumerManager.compensateSubscribeData(requestHeader.getConsumerGroup(), requestHeader.getTopic(), subscriptionData);

            return subscriptionData;
        } catch (Exception e) {
            LOGGER.warn("Parse the consumer's subscription[{}] failed, group: {}", requestHeader.getSubscription(), requestHeader.getConsumerGroup());
            response.setCodeAndRemark(ResponseCode.SUBSCRIPTION_PARSE_FAILED, "parse the consumer's subscription failed");
            return null;
        }
    }

    private ConsumerFilterData buildConsumerFilterData(PullMessageRequestHeader requestHeader, RemotingCommand response) {
        if (PullSysFlag.hasSubscriptionFlag(requestHeader.getSysFlag())) {
            return getConsumerFilterData(requestHeader);
        }
        return getConsumerFilterData(requestHeader, response);
    }

    private ConsumerFilterData getConsumerFilterData(PullMessageRequestHeader requestHeader) {
        return ConsumerFilterManager.build(
                requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getSubscription(),
                requestHeader.getExpressionType(), requestHeader.getSubVersion()
        );
    }

    private ConsumerFilterData getConsumerFilterData(PullMessageRequestHeader requestHeader, RemotingCommand response) {
        ConsumerFilterData consumerFilterData = this.brokerController.getConsumerFilterManager().get(requestHeader.getTopic(), requestHeader.getConsumerGroup());
        if (consumerFilterData == null) {
            response.setCode(ResponseCode.FILTER_DATA_NOT_EXIST);
            response.setRemark("The broker's consumer filter data is not exist!Your expression may be wrong!");
            return null;
        }
        if (consumerFilterData.getClientVersion() < requestHeader.getSubVersion()) {
            LOGGER.warn("The broker's consumer filter data is not latest, group: {}, topic: {}, serverV: {}, clientV: {}",
                    requestHeader.getConsumerGroup(), requestHeader.getTopic(), consumerFilterData.getClientVersion(), requestHeader.getSubVersion());
            response.setCode(ResponseCode.FILTER_DATA_NOT_LATEST);
            response.setRemark("the consumer's consumer filter data not latest");
            return null;
        }

        return consumerFilterData;
    }

    private RemotingCommand handleAsyncResult(RemotingCommand request, PullMessageRequestHeader requestHeader, Channel channel, SubscriptionData subscriptionData,
                                              SubscriptionGroupConfig subscriptionGroupConfig, boolean brokerAllowSuspend, MessageFilter messageFilter, RemotingCommand response, TopicQueueMappingContext mappingContext) {
        brokerController.getMessageStore().getMessageAsync(requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getQueueOffset(), requestHeader.getMaxMsgNums(), messageFilter)
        .thenApply(result -> {
            if (null == result) {
                return response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR,"store getMessage return null");
            }

            brokerController.getColdDataCgCtrService().coldAcc(requestHeader.getConsumerGroup(), result.getColdDataSum());
            return pullMessageResultHandler.handle(result, request, requestHeader, channel, subscriptionData, subscriptionGroupConfig, brokerAllowSuspend, messageFilter, response, mappingContext);
        })
        .thenAccept(result -> NettyRemotingAbstract.writeResponse(channel, request, result));

        return null;
    }

    private RemotingCommand handleBroadcastResult(Long broadcastInitOffset, RemotingCommand request, PullMessageRequestHeader requestHeader, Channel channel, SubscriptionData subscriptionData, SubscriptionGroupConfig subscriptionGroupConfig, boolean brokerAllowSuspend, MessageFilter messageFilter, RemotingCommand response, TopicQueueMappingContext mappingContext) {
        GetMessageResult getMessageResult = new GetMessageResult();
        getMessageResult.setStatus(GetMessageStatus.OFFSET_RESET);
        getMessageResult.setNextBeginOffset(broadcastInitOffset);

        return this.pullMessageResultHandler.handle(getMessageResult, request, requestHeader, channel, subscriptionData, subscriptionGroupConfig, brokerAllowSuspend, messageFilter, response, mappingContext);
    }

    private RemotingCommand handlePullResult(Long resetOffset, RemotingCommand request, PullMessageRequestHeader requestHeader, Channel channel, SubscriptionData subscriptionData, SubscriptionGroupConfig subscriptionGroupConfig, boolean brokerAllowSuspend, MessageFilter messageFilter, RemotingCommand response, TopicQueueMappingContext mappingContext) {
        GetMessageResult getMessageResult = new GetMessageResult();
        getMessageResult.setStatus(GetMessageStatus.OFFSET_RESET);
        getMessageResult.setNextBeginOffset(resetOffset);
        getMessageResult.setMinOffset(brokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId()));
        getMessageResult.setMaxOffset(brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId()));
        getMessageResult.setSuggestPullingFromSlave(false);

        return this.pullMessageResultHandler.handle(getMessageResult, request, requestHeader, channel, subscriptionData, subscriptionGroupConfig, brokerAllowSuspend, messageFilter, response, mappingContext);
    }

    protected boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }

    /**
     * Composes the header of the response message to be sent back to the client
     *
     * @param requestHeader           - the header of the request message
     * @param getMessageResult        - the result of the GetMessage request
     * @param topicSysFlag            - the system flag of the topic
     * @param subscriptionGroupConfig - configuration of the subscription group
     * @param response                - the response message to be sent back to the client
     * @param clientAddress           - the address of the client
     */
    protected void composeResponseHeader(PullMessageRequestHeader requestHeader, GetMessageResult getMessageResult, int topicSysFlag, SubscriptionGroupConfig subscriptionGroupConfig, RemotingCommand response, String clientAddress) {
        final PullMessageResponseHeader responseHeader = initPullMessageResponseHeader(getMessageResult, topicSysFlag, subscriptionGroupConfig, response);

        switch (getMessageResult.getStatus()) {
            case FOUND:
                response.setCode(ResponseCode.SUCCESS);
                break;
            case MESSAGE_WAS_REMOVING:
            case NO_MATCHED_MESSAGE:
                response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                break;
            case NO_MATCHED_LOGIC_QUEUE:
            case NO_MESSAGE_IN_QUEUE:
                response.setCode(getNoMessageInQueueCode(getMessageResult, requestHeader));
                break;
            case OFFSET_FOUND_NULL:
            case OFFSET_OVERFLOW_ONE:
                response.setCode(ResponseCode.PULL_NOT_FOUND);
                break;
            case OFFSET_OVERFLOW_BADLY:
                response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                // XXX: warn and notify me
                LOGGER.info("the request offset: {} over flow badly, fix to {}, broker max offset: {}, consumer: {}",
                        requestHeader.getQueueOffset(), getMessageResult.getNextBeginOffset(), getMessageResult.getMaxOffset(), clientAddress);
                break;
            case OFFSET_RESET:
                response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                LOGGER.info("The queue under pulling was previously reset to start from {}",
                        getMessageResult.getNextBeginOffset());
                break;
            case OFFSET_TOO_SMALL:
                response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                LOGGER.info("the request offset too small. group={}, topic={}, requestOffset={}, brokerMinOffset={}, clientIp={}",
                        requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueOffset(),
                        getMessageResult.getMinOffset(), clientAddress);
                break;
            default:
                assert false;
                break;
        }

        setSuggestWhichBrokerId(requestHeader, getMessageResult, responseHeader, response, subscriptionGroupConfig);
    }

    private int getNoMessageInQueueCode(GetMessageResult getMessageResult, PullMessageRequestHeader requestHeader) {
        if (0 == requestHeader.getQueueOffset()) {
            return ResponseCode.PULL_NOT_FOUND;
        }

        // XXX: warn and notify me
        LOGGER.info("the broker stores no queue data, fix the request offset {} to {}, Topic: {} QueueId: {} Consumer Group: {}",
                requestHeader.getQueueOffset(),
                getMessageResult.getNextBeginOffset(),
                requestHeader.getTopic(),
                requestHeader.getQueueId(),
                requestHeader.getConsumerGroup()
        );
        return ResponseCode.PULL_OFFSET_MOVED;
    }

    private PullMessageResponseHeader initPullMessageResponseHeader(GetMessageResult getMessageResult, int topicSysFlag, SubscriptionGroupConfig subscriptionGroupConfig, RemotingCommand response) {
        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();
        response.setRemark(getMessageResult.getStatus().name());
        responseHeader.setNextBeginOffset(getMessageResult.getNextBeginOffset());
        responseHeader.setMinOffset(getMessageResult.getMinOffset());
        // this does not need to be modified since it's not an accurate value under logical queue.
        responseHeader.setMaxOffset(getMessageResult.getMaxOffset());
        responseHeader.setTopicSysFlag(topicSysFlag);
        responseHeader.setGroupSysFlag(subscriptionGroupConfig.getGroupSysFlag());

        return responseHeader;
    }

    private void setSuggestWhichBrokerId(PullMessageRequestHeader requestHeader, GetMessageResult getMessageResult, PullMessageResponseHeader responseHeader, RemotingCommand response, SubscriptionGroupConfig subscriptionGroupConfig) {
        if (this.brokerController.getBrokerConfig().isSlaveReadEnable() && !this.brokerController.getBrokerConfig().isInBrokerContainer()) {
            // consume too slow ,redirect to another machine
            if (getMessageResult.isSuggestPullingFromSlave()) {
                responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly());
            }
            // consume ok
            else {
                responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
            }
        } else {
            responseHeader.setSuggestWhichBrokerId(MQConstants.MASTER_ID);
        }

        if (this.brokerController.getBrokerConfig().getBrokerId() == MQConstants.MASTER_ID || getMessageResult.isSuggestPullingFromSlave()) {
            return;
        }

        if (this.brokerController.getMinBrokerIdInGroup() != MQConstants.MASTER_ID) {
            return;
        }

        LOGGER.debug("slave redirect pullRequest to master, topic: {}, queueId: {}, consumer group: {}, next: {}, min: {}, max: {}",
                requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getConsumerGroup(), responseHeader.getNextBeginOffset(),
                responseHeader.getMinOffset(), responseHeader.getMaxOffset());

        responseHeader.setSuggestWhichBrokerId(MQConstants.MASTER_ID);
        if (!getMessageResult.getStatus().equals(GetMessageStatus.FOUND)) {
            response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
        }
    }

    protected void executeConsumeMessageHookBefore(RemotingCommand request, PullMessageRequestHeader requestHeader, GetMessageResult getMessageResult, boolean brokerAllowSuspend, int responseCode) {
        if (!this.hasConsumeMessageHook()) {
            return;
        }

        String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
        ConsumeMessageContext context = initConsumeMessageContext(request, requestHeader);

        setHookResponseContext(brokerAllowSuspend, responseCode, context, getMessageResult, owner);
        consumeMessageBefore(context);
    }

    private ConsumeMessageContext initConsumeMessageContext(RemotingCommand request, PullMessageRequestHeader requestHeader) {
        ConsumeMessageContext context = new ConsumeMessageContext();
        context.setConsumerGroup(requestHeader.getConsumerGroup());
        context.setTopic(requestHeader.getTopic());
        context.setQueueId(requestHeader.getQueueId());
        context.setAccountAuthType(request.getExtFields().get(BrokerStatsManager.ACCOUNT_AUTH_TYPE));
        context.setAccountOwnerParent(request.getExtFields().get(BrokerStatsManager.ACCOUNT_OWNER_PARENT));
        context.setAccountOwnerSelf(request.getExtFields().get(BrokerStatsManager.ACCOUNT_OWNER_SELF));
        context.setNamespace(NamespaceUtil.getNamespaceFromResource(requestHeader.getTopic()));

        return context;
    }

    private void setHookResponseContext(boolean brokerAllowSuspend, int responseCode, ConsumeMessageContext context, GetMessageResult getMessageResult, String owner) {
        switch (responseCode) {
            case ResponseCode.SUCCESS:
                setSuccessContext(context, getMessageResult, owner);
                break;
            case ResponseCode.PULL_NOT_FOUND:
                if (!brokerAllowSuspend) {
                    setNotFoundContext(context, owner);
                }
                break;
            case ResponseCode.PULL_RETRY_IMMEDIATELY:
            case ResponseCode.PULL_OFFSET_MOVED:
                setOffsetMovedContext(context, owner);
                break;
            default:
                assert false;
                break;
        }
    }

    private void setSuccessContext(ConsumeMessageContext context, GetMessageResult getMessageResult, String owner) {
        int commercialBaseCount = brokerController.getBrokerConfig().getCommercialBaseCount();
        int incValue = getMessageResult.getMsgCount4Commercial() * commercialBaseCount;

        context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_SUCCESS);
        context.setCommercialRcvTimes(incValue);
        context.setCommercialRcvSize(getMessageResult.getBufferTotalSize());
        context.setCommercialOwner(owner);

        context.setRcvStat(BrokerStatsManager.StatsType.RCV_SUCCESS);
        context.setRcvMsgNum(getMessageResult.getMessageCount());
        context.setRcvMsgSize(getMessageResult.getBufferTotalSize());
        context.setCommercialRcvMsgNum(getMessageResult.getMsgCount4Commercial());
    }

    private void setNotFoundContext(ConsumeMessageContext context, String owner) {
        context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_EPOLLS);
        context.setCommercialRcvTimes(1);
        context.setCommercialOwner(owner);

        context.setRcvStat(BrokerStatsManager.StatsType.RCV_EPOLLS);
        context.setRcvMsgNum(0);
        context.setRcvMsgSize(0);
        context.setCommercialRcvMsgNum(0);
    }

    private void setOffsetMovedContext(ConsumeMessageContext context, String owner) {
        context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_EPOLLS);
        context.setCommercialRcvTimes(1);
        context.setCommercialOwner(owner);

        context.setRcvStat(BrokerStatsManager.StatsType.RCV_EPOLLS);
        context.setRcvMsgNum(0);
        context.setRcvMsgSize(0);
        context.setCommercialRcvMsgNum(0);
    }

    private void consumeMessageBefore(ConsumeMessageContext context) {
        for (ConsumeMessageHook hook : this.consumeMessageHookList) {
            try {
                hook.consumeMessageBefore(context);
            } catch (Throwable ignored) {
            }
        }
    }

    protected void tryCommitOffset(boolean brokerAllowSuspend, PullMessageRequestHeader requestHeader, long nextOffset, String clientAddress) {
        this.brokerController.getConsumerOffsetManager().commitPullOffset(clientAddress, requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), nextOffset);

        boolean storeOffsetEnable = brokerAllowSuspend;
        final boolean hasCommitOffsetFlag = PullSysFlag.hasCommitOffsetFlag(requestHeader.getSysFlag());
        storeOffsetEnable = storeOffsetEnable && hasCommitOffsetFlag;
        if (storeOffsetEnable) {
            this.brokerController.getConsumerOffsetManager().commitOffset(clientAddress, requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());
        }
    }

    private boolean isBroadcast(boolean proxyPullBroadcast, ConsumerGroupInfo consumerGroupInfo) {
        return proxyPullBroadcast ||
                    consumerGroupInfo != null
                    && MessageModel.BROADCASTING.equals(consumerGroupInfo.getMessageModel())
                    && ConsumeType.CONSUME_PASSIVELY.equals(consumerGroupInfo.getConsumeType());
    }

    protected void updateBroadcastPulledOffset(String topic, String group, int queueId, PullMessageRequestHeader requestHeader, Channel channel, RemotingCommand response, long nextBeginOffset) {
        if (response == null || !this.brokerController.getBrokerConfig().isEnableBroadcastOffsetStore()) {
            return;
        }

        boolean proxyPullBroadcast = Objects.equals(RequestSource.PROXY_FOR_BROADCAST.getValue(), requestHeader.getRequestSource());
        ConsumerGroupInfo consumerGroupInfo = this.brokerController.getConsumerManager().getConsumerGroupInfo(group);

        if (!isBroadcast(proxyPullBroadcast, consumerGroupInfo)) {
            return;
        }

        long offset = requestHeader.getQueueOffset();
        if (ResponseCode.PULL_OFFSET_MOVED == response.getCode()) {
            offset = nextBeginOffset;
        }
        String clientId = getClientId(proxyPullBroadcast, requestHeader, channel, consumerGroupInfo);
        if (clientId == null) {
            return;
        }

        this.brokerController.getBroadcastOffsetManager().updateOffset(topic, group, queueId, offset, clientId, proxyPullBroadcast);
    }

    /**
     * When pull request is not broadcast or not return -1
     */
    protected long queryBroadcastPullInitOffset(String topic, String group, int queueId, PullMessageRequestHeader requestHeader, Channel channel) {
        if (!this.brokerController.getBrokerConfig().isEnableBroadcastOffsetStore()) {
            return -1L;
        }

        ConsumerGroupInfo consumerGroupInfo = this.brokerController.getConsumerManager().getConsumerGroupInfo(group);
        boolean proxyPullBroadcast = Objects.equals(RequestSource.PROXY_FOR_BROADCAST.getValue(), requestHeader.getRequestSource());

        if (!isBroadcast(proxyPullBroadcast, consumerGroupInfo)) {
            return -1L;
        }

        String clientId = getClientId(proxyPullBroadcast, requestHeader, channel, consumerGroupInfo);
        if (clientId == null) {
            return -1L;
        }

        return this.brokerController.getBroadcastOffsetManager().queryInitOffset(topic, group, queueId, clientId, requestHeader.getQueueOffset(), proxyPullBroadcast);
    }

    private String getClientId(boolean proxyPullBroadcast, PullMessageRequestHeader requestHeader, Channel channel, ConsumerGroupInfo consumerGroupInfo) {
        if (proxyPullBroadcast) {
            return requestHeader.getProxyFrowardClientId();
        }

        ClientChannelInfo clientChannelInfo = consumerGroupInfo.findChannel(channel);
        if (clientChannelInfo == null) {
            return null;
        }
        return clientChannelInfo.getClientId();
    }
}
