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
import io.netty.channel.FileRegion;
import io.opentelemetry.api.common.Attributes;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.connection.longpolling.PullRequest;
import org.apache.rocketmq.broker.server.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.infra.zerocopy.ManyMessageTransfer;
import org.apache.rocketmq.common.app.AbortProcessException;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.domain.topic.TopicFilterType;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.domain.message.MessageQueue;
import org.apache.rocketmq.common.domain.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.domain.sysflag.PullSysFlag;
import org.apache.rocketmq.common.domain.topic.TopicValidator;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.utils.NetworkUtils;
import org.apache.rocketmq.common.utils.TimeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.metrics.RemotingMetricsManager;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingContext;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.protocol.topic.OffsetMovedEvent;
import org.apache.rocketmq.store.api.dto.GetMessageResult;
import org.apache.rocketmq.store.api.filter.MessageFilter;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.server.config.BrokerRole;

import static org.apache.rocketmq.broker.server.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.server.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.server.metrics.BrokerMetricsConstant.LABEL_TOPIC;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_REQUEST_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESPONSE_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESULT;

public class DefaultPullMessageResultHandler implements PullMessageResultHandler {

    protected static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    protected final Broker broker;

    public DefaultPullMessageResultHandler(final Broker broker) {
        this.broker = broker;
    }

    @Override
    public RemotingCommand handle(final GetMessageResult getMessageResult, final RemotingCommand request, final PullMessageRequestHeader requestHeader, final Channel channel, final SubscriptionData subscriptionData, final SubscriptionGroupConfig subscriptionGroupConfig, final boolean brokerAllowSuspend, final MessageFilter messageFilter, RemotingCommand response, TopicQueueMappingContext mappingContext) {
        PullMessageProcessor processor = broker.getBrokerNettyServer().getPullMessageProcessor();
        final String clientAddress = RemotingHelper.parseChannelRemoteAddr(channel);
        TopicConfig topicConfig = this.broker.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        processor.composeResponseHeader(requestHeader, getMessageResult, topicConfig.getTopicSysFlag(), subscriptionGroupConfig, response, clientAddress);

        try {
            processor.executeConsumeMessageHookBefore(request, requestHeader, getMessageResult, brokerAllowSuspend, response.getCode());
        } catch (AbortProcessException e) {
            return response.setCodeAndRemark(e.getResponseCode(), e.getErrorMessage());
        }

        //rewrite the response for the static topic
        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();
        RemotingCommand rewriteResult = processor.rewriteResponseForStaticTopic(requestHeader, responseHeader, mappingContext, response.getCode());
        if (rewriteResult != null) {
            response = rewriteResult;
        }

        processor.updateBroadcastPulledOffset(requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueId(), requestHeader, channel, response, getMessageResult.getNextBeginOffset());
        processor.tryCommitOffset(brokerAllowSuspend, requestHeader, getMessageResult.getNextBeginOffset(), clientAddress);

        return formatResponseByCode(requestHeader, request, subscriptionGroupConfig, brokerAllowSuspend, channel, response, responseHeader, subscriptionData, messageFilter, getMessageResult);
    }

    private RemotingCommand formatResponseByCode(PullMessageRequestHeader requestHeader, RemotingCommand request, SubscriptionGroupConfig subscriptionGroupConfig, boolean brokerAllowSuspend, Channel channel, RemotingCommand response,  PullMessageResponseHeader responseHeader, SubscriptionData subscriptionData, MessageFilter messageFilter, GetMessageResult getMessageResult) {
        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                return handleSuccess(requestHeader, request, channel, response, getMessageResult);
            case ResponseCode.PULL_NOT_FOUND:
                if (!handlePullNotFound(requestHeader, brokerAllowSuspend, request, channel, subscriptionData, messageFilter)) {
                    return null;
                }
            case ResponseCode.PULL_RETRY_IMMEDIATELY:
                break;
            case ResponseCode.PULL_OFFSET_MOVED:
                handlePullOffsetMoved(requestHeader, responseHeader, response, subscriptionGroupConfig, getMessageResult);
                break;
            default:
                log.warn("[BUG] impossible result code of get message: {}", response.getCode());
                assert false;
        }

        return response;
    }

    private RemotingCommand handleSuccess(PullMessageRequestHeader requestHeader, RemotingCommand request, Channel channel, RemotingCommand response, GetMessageResult getMessageResult) {
        this.broker.getBrokerStatsManager().incGroupGetNums(requestHeader.getConsumerGroup(), requestHeader.getTopic(), getMessageResult.getMessageCount());
        this.broker.getBrokerStatsManager().incGroupGetSize(requestHeader.getConsumerGroup(), requestHeader.getTopic(), getMessageResult.getBufferTotalSize());
        this.broker.getBrokerStatsManager().incBrokerGetNums(requestHeader.getTopic(), getMessageResult.getMessageCount());

        if (!BrokerMetricsManager.isRetryOrDlqTopic(requestHeader.getTopic())) {
            Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                .put(LABEL_TOPIC, requestHeader.getTopic())
                .put(LABEL_CONSUMER_GROUP, requestHeader.getConsumerGroup())
                .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(requestHeader.getTopic()) || MQConstants.isSysConsumerGroup(requestHeader.getConsumerGroup()))
                .build();
            BrokerMetricsManager.messagesOutTotal.add(getMessageResult.getMessageCount(), attributes);
            BrokerMetricsManager.throughputOutTotal.add(getMessageResult.getBufferTotalSize(), attributes);
        }

        if (!channelIsWritable(channel, requestHeader)) {
            getMessageResult.release();
            //ignore pull request
            return null;
        }

        if (this.broker.getBrokerConfig().isTransferMsgByHeap()) {
            return handleTransferMsgByHeap(requestHeader, response, getMessageResult);
        } else {
            return handleSuccessMsg(request, channel, response, getMessageResult);
        }
    }

    private RemotingCommand handleSuccessMsg(RemotingCommand request, Channel channel, RemotingCommand response, GetMessageResult getMessageResult) {
        try {
            FileRegion fileRegion = new ManyMessageTransfer(response.encodeHeader(getMessageResult.getBufferTotalSize()), getMessageResult);
            channel.writeAndFlush(fileRegion).addListener((ChannelFutureListener) future -> {
                getMessageResult.release();
                Attributes attributes = RemotingMetricsManager.newAttributesBuilder()
                    .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode()))
                    .put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(response.getCode()))
                    .put(LABEL_RESULT, RemotingMetricsManager.getWriteAndFlushResult(future))
                    .build();

                RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributes);
                if (!future.isSuccess()) {
                    log.error("Fail to transfer messages from page cache to {}", channel.remoteAddress(), future.cause());
                }
            });
        } catch (Throwable e) {
            log.error("Error occurred when transferring messages from page cache", e);
            getMessageResult.release();
        }
        return null;
    }

    private RemotingCommand handleTransferMsgByHeap(PullMessageRequestHeader requestHeader, RemotingCommand response, GetMessageResult getMessageResult) {
        final long beginTimeMills = TimeUtils.now();
        final byte[] r = this.readGetMessageResult(getMessageResult, requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
        this.broker.getBrokerStatsManager().incGroupGetLatency(requestHeader.getConsumerGroup(),
            requestHeader.getTopic(), requestHeader.getQueueId(), (int) (TimeUtils.now() - beginTimeMills));
        response.setBody(r);
        return response;
    }

    private boolean handlePullNotFound(PullMessageRequestHeader requestHeader, boolean brokerAllowSuspend, RemotingCommand request, Channel channel, SubscriptionData subscriptionData, MessageFilter messageFilter) {
        final boolean hasSuspendFlag = PullSysFlag.hasSuspendFlag(requestHeader.getSysFlag());
        final long suspendTimeoutMillisLong = hasSuspendFlag ? requestHeader.getSuspendTimeoutMillis() : 0;

        if (!brokerAllowSuspend || !hasSuspendFlag) {
            return true;
        }

        long pollingTimeMills = suspendTimeoutMillisLong;
        if (!this.broker.getBrokerConfig().isLongPollingEnable()) {
            pollingTimeMills = this.broker.getBrokerConfig().getShortPollingTimeMills();
        }

        String topic = requestHeader.getTopic();
        long offset = requestHeader.getQueueOffset();
        int queueId = requestHeader.getQueueId();
        PullRequest pullRequest = new PullRequest(request, channel, pollingTimeMills, TimeUtils.now(), offset, subscriptionData, messageFilter);
        this.broker.getBrokerNettyServer().getPullRequestHoldService().suspendPullRequest(topic, queueId, pullRequest);
        return false;
    }

    private void handlePullOffsetMoved(PullMessageRequestHeader requestHeader, PullMessageResponseHeader responseHeader, RemotingCommand response, SubscriptionGroupConfig subscriptionGroupConfig, GetMessageResult getMessageResult) {
        if (this.broker.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE
            || this.broker.getMessageStoreConfig().isOffsetCheckInSlave()) {
            MessageQueue mq = new MessageQueue();
            mq.setTopic(requestHeader.getTopic());
            mq.setQueueId(requestHeader.getQueueId());
            mq.setBrokerName(this.broker.getBrokerConfig().getBrokerName());

            OffsetMovedEvent event = new OffsetMovedEvent();
            event.setConsumerGroup(requestHeader.getConsumerGroup());
            event.setMessageQueue(mq);
            event.setOffsetRequest(requestHeader.getQueueOffset());
            event.setOffsetNew(getMessageResult.getNextBeginOffset());
            log.warn(
                "PULL_OFFSET_MOVED:correction offset. topic={}, groupId={}, requestOffset={}, newOffset={}, suggestBrokerId={}",
                requestHeader.getTopic(), requestHeader.getConsumerGroup(), event.getOffsetRequest(), event.getOffsetNew(), responseHeader.getSuggestWhichBrokerId());
        } else {
            responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
            response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
            log.warn("PULL_OFFSET_MOVED:none correction. topic={}, groupId={}, requestOffset={}, suggestBrokerId={}",
                requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueOffset(), responseHeader.getSuggestWhichBrokerId());
        }
    }

    private boolean channelIsWritable(Channel channel, PullMessageRequestHeader requestHeader) {
        if (!this.broker.getBrokerConfig().isEnableNetWorkFlowControl()) {
            return true;
        }

        if (channel.isWritable()) {
            return true;
        }

        log.warn("channel {} not writable ,cid {}", channel.remoteAddress(), requestHeader.getConsumerGroup());
        return false;
    }

    protected byte[] readGetMessageResult(final GetMessageResult getMessageResult, final String group,
        final String topic,
        final int queueId) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());

        long storeTimestamp = 0;
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {

                byteBuffer.put(bb);
                int sysFlag = bb.getInt(MessageDecoder.SYSFLAG_POSITION);
//                bornhost has the IPv4 ip if the MessageSysFlag.BORNHOST_V6_FLAG bit of sysFlag is 0
//                IPv4 host = ip(4 byte) + port(4 byte); IPv6 host = ip(16 byte) + port(4 byte)
                int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
                int msgStoreTimePos = 4 // 1 TOTALSIZE
                    + 4 // 2 MAGICCODE
                    + 4 // 3 BODYCRC
                    + 4 // 4 QUEUEID
                    + 4 // 5 FLAG
                    + 8 // 6 QUEUEOFFSET
                    + 8 // 7 PHYSICALOFFSET
                    + 4 // 8 SYSFLAG
                    + 8 // 9 BORNTIMESTAMP
                    + bornhostLength; // 10 BORNHOST
                storeTimestamp = bb.getLong(msgStoreTimePos);
            }
        } finally {
            getMessageResult.release();
        }

        this.broker.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId, TimeUtils.now() - storeTimestamp);
        return byteBuffer.array();
    }

    protected void generateOffsetMovedEvent(final OffsetMovedEvent event) {
        try {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setTopic(TopicValidator.RMQ_SYS_OFFSET_MOVED_EVENT);
            msgInner.setTags(event.getConsumerGroup());
            msgInner.setDelayTimeLevel(0);
            msgInner.setKeys(event.getConsumerGroup());
            msgInner.setBody(event.encode());
            msgInner.setFlag(0);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(TopicFilterType.SINGLE_TAG, msgInner.getTags()));

            msgInner.setQueueId(0);
            msgInner.setSysFlag(0);
            msgInner.setBornTimestamp(System.currentTimeMillis());
            msgInner.setBornHost(NetworkUtils.string2SocketAddress(this.broker.getBrokerAddr()));
            msgInner.setStoreHost(msgInner.getBornHost());

            msgInner.setReconsumeTimes(0);

            PutMessageResult putMessageResult = this.broker.getMessageStore().putMessage(msgInner);
        } catch (Exception e) {
            log.warn(String.format("generateOffsetMovedEvent Exception, %s", event.toString()), e);
        }
    }
}
