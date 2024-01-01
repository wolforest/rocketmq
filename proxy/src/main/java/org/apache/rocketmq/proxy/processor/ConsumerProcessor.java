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

package org.apache.rocketmq.proxy.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.consumer.ReceiptHandle;
import org.apache.rocketmq.common.domain.message.MessageAccessor;
import org.apache.rocketmq.common.domain.message.MessageClientExt;
import org.apache.rocketmq.common.domain.message.MessageClientIDSetter;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageQueue;
import org.apache.rocketmq.common.utils.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.common.utils.FutureUtils;
import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

public class ConsumerProcessor extends AbstractProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final ExecutorService executor;

    public ConsumerProcessor(MessagingProcessor messagingProcessor, ServiceManager serviceManager,
        ExecutorService executor) {
        super(messagingProcessor, serviceManager);
        this.executor = executor;
    }

    /**
     * proxy method for MessageService.popMessage()
     *
     * @param ctx proxy context
     * @param queueSelector queue selector: by broker name for ReceiveMessageActivity
     * @param consumerGroup consumer group
     * @param topic topic
     * @param maxMsgNums batch size
     * @param invisibleTime invisibleTime:
     * @param pollTime pollingTimeout
     * @param initMode ConsumeInitMode.MAX for pop message mode
     * @param subscriptionData subscription data
     * @param fifo fifo setting
     * @param popMessageResultFilter filter
     * @param attemptId attemptId
     * @param timeoutMillis timeout
     * @return future
     */
    public CompletableFuture<PopResult> popMessage(
        ProxyContext ctx,
        QueueSelector queueSelector,
        String consumerGroup,
        String topic,
        int maxMsgNums,
        long invisibleTime,
        long pollTime,
        int initMode,
        SubscriptionData subscriptionData,
        boolean fifo,
        PopMessageResultFilter popMessageResultFilter,
        String attemptId,
        long timeoutMillis
    ) {
        CompletableFuture<PopResult> future = new CompletableFuture<>();
        try {
            AddressableMessageQueue messageQueue = queueSelector.select(ctx, this.serviceManager.getTopicRouteService().getCurrentMessageQueueView(ctx, topic));
            if (messageQueue == null) {
                throw new ProxyException(ProxyExceptionCode.FORBIDDEN, "no readable queue");
            }
            return popMessage(ctx, messageQueue, consumerGroup, topic, maxMsgNums, invisibleTime, pollTime, initMode,
                subscriptionData, fifo, popMessageResultFilter, attemptId, timeoutMillis);
        }  catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<PopResult> popMessage(
        ProxyContext ctx,
        AddressableMessageQueue messageQueue,
        String consumerGroup,
        String topic,
        int maxMsgNums,
        long invisibleTime,
        long pollTime,
        int initMode,
        SubscriptionData subscriptionData,
        boolean fifo,
        PopMessageResultFilter popMessageResultFilter,
        String attemptId,
        long timeoutMillis
    ) {
        CompletableFuture<PopResult> future = new CompletableFuture<>();
        try {
            PopMessageRequestHeader requestHeader = createPopMessageRequestHeader(
                messageQueue, consumerGroup, topic, maxMsgNums, invisibleTime, pollTime, initMode, subscriptionData, fifo, attemptId
            );

            future = this.serviceManager.getMessageService().popMessage(ctx, messageQueue, requestHeader, timeoutMillis)
                    .thenApplyAsync(popCallback(ctx, popMessageResultFilter, requestHeader, consumerGroup, topic, subscriptionData), this.executor);

        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    private PopMessageRequestHeader createPopMessageRequestHeader(
        AddressableMessageQueue messageQueue,
        String consumerGroup,
        String topic,
        int maxMsgNums,
        long invisibleTime,
        long pollTime,
        int initMode,
        SubscriptionData subscriptionData,
        boolean fifo,
        String attemptId
    ) {
        if (maxMsgNums > ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST) {
            log.warn("change maxNums from {} to {} for pop request, with info: topic:{}, group:{}",
                maxMsgNums, ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST, topic, consumerGroup);
            maxMsgNums = ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST;
        }

        PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(messageQueue.getQueueId());
        requestHeader.setMaxMsgNums(maxMsgNums);
        requestHeader.setInvisibleTime(invisibleTime);
        requestHeader.setPollTime(pollTime);
        requestHeader.setInitMode(initMode);
        requestHeader.setExpType(subscriptionData.getExpressionType());
        requestHeader.setExp(subscriptionData.getSubString());
        requestHeader.setOrder(fifo);
        requestHeader.setAttemptId(attemptId);
        requestHeader.setBornTime(System.currentTimeMillis());

        return requestHeader;
    }

    private Function<PopResult, PopResult> popCallback(ProxyContext ctx, PopMessageResultFilter filter, PopMessageRequestHeader requestHeader, String consumerGroup, String topic, SubscriptionData subscriptionData) {
        return popResult -> {
            if (!PopStatus.FOUND.equals(popResult.getPopStatus())) {
                return popResult;
            }

            if (popResult.getMsgFoundList() == null) {
                return popResult;
            }

            if (popResult.getMsgFoundList().isEmpty()) {
                return popResult;
            }

            if (null == filter) {
                return popResult;
            }

            List<MessageExt> messageExtList = new ArrayList<>();
            for (MessageExt messageExt : popResult.getMsgFoundList()) {
                parsePopMessage(ctx, filter, requestHeader, consumerGroup, topic, subscriptionData, messageExt, messageExtList);
            }
            popResult.setMsgFoundList(messageExtList);
            return popResult;
        };
    }

    private void parsePopMessage(ProxyContext ctx, PopMessageResultFilter filter, PopMessageRequestHeader requestHeader,
        String consumerGroup, String topic, SubscriptionData subscriptionData, MessageExt messageExt, List<MessageExt> messageExtList) {
        try {
            fillUniqIDIfNeed(messageExt);
            String handleString = createHandle(messageExt.getProperty(MessageConst.PROPERTY_POP_CK), messageExt.getCommitLogOffset());
            if (handleString == null) {
                log.error("[BUG] pop message from broker but handle is empty. requestHeader:{}, msg:{}", requestHeader, messageExt);
                messageExtList.add(messageExt);
                return;
            }

            MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_POP_CK, handleString);
            PopMessageResultFilter.FilterResult filterResult = filter.filterMessage(ctx, consumerGroup, subscriptionData, messageExt);

            switch (filterResult) {
                case NO_MATCH:
                    this.messagingProcessor.ackMessage(
                        ctx,
                        ReceiptHandle.decode(handleString),
                        messageExt.getMsgId(),
                        consumerGroup,
                        topic,
                        MessagingProcessor.DEFAULT_TIMEOUT_MILLS);
                    break;
                case TO_DLQ:
                    this.messagingProcessor.forwardMessageToDeadLetterQueue(
                        ctx,
                        ReceiptHandle.decode(handleString),
                        messageExt.getMsgId(),
                        consumerGroup,
                        topic,
                        MessagingProcessor.DEFAULT_TIMEOUT_MILLS);
                    break;
                case MATCH:
                default:
                    messageExtList.add(messageExt);
                    break;
            }
        } catch (Throwable t) {
            log.error("process filterMessage failed. requestHeader:{}, msg:{}", requestHeader, messageExt, t);
            messageExtList.add(messageExt);
        }
    }

    private void fillUniqIDIfNeed(MessageExt messageExt) {
        if (!StringUtils.isBlank(MessageClientIDSetter.getUniqID(messageExt))) {
            return;
        }

        if (!(messageExt instanceof MessageClientExt)) {
            return;
        }

        MessageClientExt clientExt = (MessageClientExt) messageExt;
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, clientExt.getOffsetMsgId());
    }

    public CompletableFuture<AckResult> ackMessage(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        String consumerGroup,
        String topic,
        long timeoutMillis
    ) {
        CompletableFuture<AckResult> future = new CompletableFuture<>();
        try {
            this.validateReceiptHandle(handle);
            AckMessageRequestHeader header = buildAckHeader(handle, consumerGroup, topic);

            future = this.serviceManager.getMessageService().ackMessage(
                ctx, handle, messageId, header, timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    private AckMessageRequestHeader buildAckHeader(ReceiptHandle handle, String consumerGroup, String topic) {
        AckMessageRequestHeader header = new AckMessageRequestHeader();
        header.setConsumerGroup(consumerGroup);
        header.setTopic(handle.getRealTopic(topic, consumerGroup));
        header.setQueueId(handle.getQueueId());
        header.setExtraInfo(handle.getReceiptHandle());
        header.setOffset(handle.getOffset());

        return header;
    }

    public CompletableFuture<List<BatchAckResult>> batchAckMessage(
        ProxyContext ctx,
        List<ReceiptHandleMessage> handleMessageList,
        String consumerGroup,
        String topic,
        long timeoutMillis
    ) {
        CompletableFuture<List<BatchAckResult>> future = new CompletableFuture<>();
        try {
            List<BatchAckResult> batchAckResultList = new ArrayList<>(handleMessageList.size());
            Map<String, List<ReceiptHandleMessage>> brokerHandleListMap = new HashMap<>();

            for (ReceiptHandleMessage handleMessage : handleMessageList) {
                if (handleMessage.getReceiptHandle().isExpired()) {
                    batchAckResultList.add(new BatchAckResult(handleMessage, EXPIRED_HANDLE_PROXY_EXCEPTION));
                    continue;
                }
                List<ReceiptHandleMessage> brokerHandleList = brokerHandleListMap.computeIfAbsent(handleMessage.getReceiptHandle().getBrokerName(), key -> new ArrayList<>());
                brokerHandleList.add(handleMessage);
            }

            if (brokerHandleListMap.isEmpty()) {
                return FutureUtils.addExecutor(CompletableFuture.completedFuture(batchAckResultList), this.executor);
            }
            Set<Map.Entry<String, List<ReceiptHandleMessage>>> brokerHandleListMapEntrySet = brokerHandleListMap.entrySet();
            CompletableFuture<List<BatchAckResult>>[] futures = new CompletableFuture[brokerHandleListMapEntrySet.size()];
            int futureIndex = 0;
            for (Map.Entry<String, List<ReceiptHandleMessage>> entry : brokerHandleListMapEntrySet) {
                futures[futureIndex++] = processBrokerHandle(ctx, consumerGroup, topic, entry.getValue(), timeoutMillis);
            }
            CompletableFuture.allOf(futures).whenComplete((val, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                }
                for (CompletableFuture<List<BatchAckResult>> resultFuture : futures) {
                    batchAckResultList.addAll(resultFuture.join());
                }
                future.complete(batchAckResultList);
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    protected CompletableFuture<List<BatchAckResult>> processBrokerHandle(ProxyContext ctx, String consumerGroup, String topic, List<ReceiptHandleMessage> handleMessageList, long timeoutMillis) {
        return this.serviceManager.getMessageService().batchAckMessage(ctx, handleMessageList, consumerGroup, topic, timeoutMillis)
            .thenApply(result -> {
                List<BatchAckResult> results = new ArrayList<>();
                for (ReceiptHandleMessage handleMessage : handleMessageList) {
                    results.add(new BatchAckResult(handleMessage, result));
                }
                return results;
            })
            .exceptionally(throwable -> {
                List<BatchAckResult> results = new ArrayList<>();
                for (ReceiptHandleMessage handleMessage : handleMessageList) {
                    results.add(new BatchAckResult(handleMessage, new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, throwable.getMessage(), throwable)));
                }
                return results;
            });
    }

    public CompletableFuture<AckResult> changeInvisibleTime(ProxyContext ctx, ReceiptHandle handle,
        String messageId, String groupName, String topicName, long invisibleTime, long timeoutMillis) {
        CompletableFuture<AckResult> future = new CompletableFuture<>();
        try {
            this.validateReceiptHandle(handle);

            ChangeInvisibleTimeRequestHeader requestHeader = new ChangeInvisibleTimeRequestHeader();
            requestHeader.setConsumerGroup(groupName);
            requestHeader.setTopic(handle.getRealTopic(topicName, groupName));
            requestHeader.setQueueId(handle.getQueueId());
            requestHeader.setExtraInfo(handle.getReceiptHandle());
            requestHeader.setOffset(handle.getOffset());
            requestHeader.setInvisibleTime(invisibleTime);
            long commitLogOffset = handle.getCommitLogOffset();

            future = this.serviceManager.getMessageService().changeInvisibleTime(
                    ctx,
                    handle,
                    messageId,
                    requestHeader,
                    timeoutMillis)
                .thenApplyAsync(ackResult -> {
                    if (StringUtils.isNotBlank(ackResult.getExtraInfo())) {
                        AckResult result = new AckResult();
                        result.setStatus(ackResult.getStatus());
                        result.setPopTime(result.getPopTime());
                        result.setExtraInfo(createHandle(ackResult.getExtraInfo(), commitLogOffset));
                        return result;
                    } else {
                        return ackResult;
                    }
                }, this.executor);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    protected String createHandle(String handleString, long commitLogOffset) {
        if (handleString == null) {
            return null;
        }
        return handleString + MessageConst.KEY_SEPARATOR + commitLogOffset;
    }

    public CompletableFuture<PullResult> pullMessage(ProxyContext ctx, MessageQueue messageQueue, String consumerGroup,
        long queueOffset, int maxMsgNums, int sysFlag, long commitOffset,
        long suspendTimeoutMillis, SubscriptionData subscriptionData, long timeoutMillis) {
        CompletableFuture<PullResult> future = new CompletableFuture<>();
        try {
            AddressableMessageQueue addressableMessageQueue = serviceManager.getTopicRouteService()
                .buildAddressableMessageQueue(ctx, messageQueue);
            PullMessageRequestHeader requestHeader = buildPullHeader(addressableMessageQueue, consumerGroup,
                queueOffset, maxMsgNums, sysFlag, commitOffset, suspendTimeoutMillis, subscriptionData);

            future = serviceManager.getMessageService().pullMessage(ctx, addressableMessageQueue, requestHeader, timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    private PullMessageRequestHeader buildPullHeader(AddressableMessageQueue addressableMessageQueue,
        String consumerGroup, long queueOffset, int maxMsgNums, int sysFlag, long commitOffset,
        long suspendTimeoutMillis, SubscriptionData subscriptionData) {

        PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setTopic(addressableMessageQueue.getTopic());
        requestHeader.setQueueId(addressableMessageQueue.getQueueId());
        requestHeader.setQueueOffset(queueOffset);
        requestHeader.setMaxMsgNums(maxMsgNums);
        requestHeader.setSysFlag(sysFlag);
        requestHeader.setCommitOffset(commitOffset);
        requestHeader.setSuspendTimeoutMillis(suspendTimeoutMillis);
        requestHeader.setSubscription(subscriptionData.getSubString());
        requestHeader.setExpressionType(subscriptionData.getExpressionType());

        return requestHeader;
    }

    public CompletableFuture<Void> updateConsumerOffset(ProxyContext ctx, MessageQueue messageQueue,
        String consumerGroup, long commitOffset, long timeoutMillis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            AddressableMessageQueue addressableMessageQueue = serviceManager.getTopicRouteService()
                .buildAddressableMessageQueue(ctx, messageQueue);
            UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setTopic(addressableMessageQueue.getTopic());
            requestHeader.setQueueId(addressableMessageQueue.getQueueId());
            requestHeader.setCommitOffset(commitOffset);
            future = serviceManager.getMessageService().updateConsumerOffset(ctx, addressableMessageQueue, requestHeader, timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    public CompletableFuture<Long> queryConsumerOffset(ProxyContext ctx, MessageQueue messageQueue,
        String consumerGroup, long timeoutMillis) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            AddressableMessageQueue addressableMessageQueue = serviceManager.getTopicRouteService()
                .buildAddressableMessageQueue(ctx, messageQueue);
            QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setTopic(addressableMessageQueue.getTopic());
            requestHeader.setQueueId(addressableMessageQueue.getQueueId());
            future = serviceManager.getMessageService().queryConsumerOffset(ctx, addressableMessageQueue, requestHeader, timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    public CompletableFuture<Set<MessageQueue>> lockBatchMQ(ProxyContext ctx, Set<MessageQueue> mqSet,
        String consumerGroup, String clientId, long timeoutMillis) {
        CompletableFuture<Set<MessageQueue>> future = new CompletableFuture<>();
        Set<MessageQueue> successSet = new CopyOnWriteArraySet<>();
        Set<AddressableMessageQueue> addressableMessageQueueSet = buildAddressableSet(ctx, mqSet);
        Map<String, List<AddressableMessageQueue>> messageQueueSetMap = buildAddressableMapByBrokerName(addressableMessageQueueSet);
        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        messageQueueSetMap.forEach((k, v) -> {
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setConsumerGroup(consumerGroup);
            requestBody.setClientId(clientId);
            requestBody.setMqSet(v.stream().map(AddressableMessageQueue::getMessageQueue).collect(Collectors.toSet()));
            CompletableFuture<Void> future0 = serviceManager.getMessageService()
                .lockBatchMQ(ctx, v.get(0), requestBody, timeoutMillis)
                .thenAccept(successSet::addAll);
            futureList.add(FutureUtils.addExecutor(future0, this.executor));
        });
        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).whenComplete((v, t) -> {
            if (t != null) {
                log.error("LockBatchMQ failed", t);
            }
            future.complete(successSet);
        });
        return FutureUtils.addExecutor(future, this.executor);
    }

    public CompletableFuture<Void> unlockBatchMQ(ProxyContext ctx, Set<MessageQueue> mqSet,
        String consumerGroup, String clientId, long timeoutMillis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Set<AddressableMessageQueue> addressableMessageQueueSet = buildAddressableSet(ctx, mqSet);
        Map<String, List<AddressableMessageQueue>> messageQueueSetMap = buildAddressableMapByBrokerName(addressableMessageQueueSet);
        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        messageQueueSetMap.forEach((k, v) -> {
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setConsumerGroup(consumerGroup);
            requestBody.setClientId(clientId);
            requestBody.setMqSet(v.stream().map(AddressableMessageQueue::getMessageQueue).collect(Collectors.toSet()));
            CompletableFuture<Void> future0 = serviceManager.getMessageService().unlockBatchMQ(ctx, v.get(0), requestBody, timeoutMillis);
            futureList.add(FutureUtils.addExecutor(future0, this.executor));
        });
        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).whenComplete((v, t) -> {
            if (t != null) {
                log.error("UnlockBatchMQ failed", t);
            }
            future.complete(null);
        });
        return FutureUtils.addExecutor(future, this.executor);
    }

    public CompletableFuture<Long> getMaxOffset(ProxyContext ctx, MessageQueue messageQueue, long timeoutMillis) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            AddressableMessageQueue addressableMessageQueue = serviceManager.getTopicRouteService()
                .buildAddressableMessageQueue(ctx, messageQueue);
            GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
            requestHeader.setTopic(addressableMessageQueue.getTopic());
            requestHeader.setQueueId(addressableMessageQueue.getQueueId());
            future = serviceManager.getMessageService().getMaxOffset(ctx, addressableMessageQueue, requestHeader, timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    public CompletableFuture<Long> getMinOffset(ProxyContext ctx, MessageQueue messageQueue, long timeoutMillis) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            AddressableMessageQueue addressableMessageQueue = serviceManager.getTopicRouteService()
                .buildAddressableMessageQueue(ctx, messageQueue);
            GetMinOffsetRequestHeader requestHeader = new GetMinOffsetRequestHeader();
            requestHeader.setTopic(addressableMessageQueue.getTopic());
            requestHeader.setQueueId(addressableMessageQueue.getQueueId());
            future = serviceManager.getMessageService().getMinOffset(ctx, addressableMessageQueue, requestHeader, timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    protected Set<AddressableMessageQueue> buildAddressableSet(ProxyContext ctx, Set<MessageQueue> mqSet) {
        Set<AddressableMessageQueue> addressableMessageQueueSet = new HashSet<>(mqSet.size());
        for (MessageQueue mq:mqSet) {
            try {
                addressableMessageQueueSet.add(serviceManager.getTopicRouteService().buildAddressableMessageQueue(ctx, mq)) ;
            } catch (Exception e) {
                log.error("build addressable message queue fail, messageQueue = {}", mq, e);
            }
        }
        return addressableMessageQueueSet;
    }

    protected HashMap<String, List<AddressableMessageQueue>> buildAddressableMapByBrokerName(
        final Set<AddressableMessageQueue> mqSet) {
        HashMap<String, List<AddressableMessageQueue>> result = new HashMap<>();
        for (AddressableMessageQueue mq : mqSet) {
            List<AddressableMessageQueue> mqs = result.computeIfAbsent(mq.getBrokerName(), k -> new ArrayList<>());
            mqs.add(mq);
        }
        return result;
    }
}
