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
package org.apache.rocketmq.proxy.service.message;

import io.netty.channel.ChannelHandlerContext;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.consumer.ReceiptHandle;
import org.apache.rocketmq.common.domain.message.Message;
import org.apache.rocketmq.common.domain.message.MessageBatch;
import org.apache.rocketmq.common.domain.message.MessageClientIDSetter;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageQueue;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.service.channel.ChannelManager;
import org.apache.rocketmq.proxy.service.channel.InvocationContext;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BatchAck;
import org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class LocalMessageService implements MessageService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final Broker broker;
    private final ChannelManager channelManager;

    public LocalMessageService(Broker broker, ChannelManager channelManager, RPCHook rpcHook) {
        this.broker = broker;
        this.channelManager = channelManager;
    }

    @Override
    public CompletableFuture<List<SendResult>> sendMessage(ProxyContext ctx, AddressableMessageQueue messageQueue,
        List<Message> msgList, SendMessageRequestHeader requestHeader, long timeoutMillis) {
        byte[] body;
        String messageId;
        if (msgList.size() > 1) {
            requestHeader.setBatch(true);
            MessageBatch msgBatch = MessageBatch.generateFromList(msgList);
            MessageClientIDSetter.setUniqID(msgBatch);
            body = msgBatch.encode();
            msgBatch.setBody(body);
            messageId = MessageClientIDSetter.getUniqID(msgBatch);
        } else {
            Message message = msgList.get(0);
            body = message.getBody();
            messageId = MessageClientIDSetter.getUniqID(message);
        }

        RemotingCommand request = LocalRemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader, ctx.getLanguage());
        request.setBody(body);
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();

        processSendRequest(ctx, request, future);

        return createSendResponse(future, messageId, requestHeader);
    }

    @Override
    public CompletableFuture<RemotingCommand> sendMessageBack(ProxyContext ctx, ReceiptHandle handle, String messageId,
        ConsumerSendMsgBackRequestHeader requestHeader, long timeoutMillis) {
        SimpleChannel channel = channelManager.createChannel(ctx);
        ChannelHandlerContext channelHandlerContext = channel.getChannelHandlerContext();
        RemotingCommand command = LocalRemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader, ctx.getLanguage());
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            RemotingCommand response = broker.getBrokerNettyServer().getSendMessageProcessor()
                .processRequest(channelHandlerContext, command);
            future.complete(response);
        } catch (Exception e) {
            log.error("Fail to process sendMessageBack command", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<Void> endTransactionOneway(ProxyContext ctx, String brokerName, EndTransactionRequestHeader requestHeader,
        long timeoutMillis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        SimpleChannel channel = channelManager.createChannel(ctx);
        ChannelHandlerContext channelHandlerContext = channel.getChannelHandlerContext();
        RemotingCommand command = LocalRemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, requestHeader, ctx.getLanguage());
        try {
            broker.getBrokerNettyServer().getEndTransactionProcessor()
                .processRequest(channelHandlerContext, command);
            future.complete(null);
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<PopResult> popMessage(ProxyContext ctx, AddressableMessageQueue messageQueue,
        PopMessageRequestHeader requestHeader, long timeoutMillis) {

        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        processPopRequest(ctx, requestHeader, future);

        return createPopResponse(future, messageQueue, requestHeader);
    }

    @Override
    public CompletableFuture<AckResult> changeInvisibleTime(ProxyContext ctx, ReceiptHandle handle, String messageId,
        ChangeInvisibleTimeRequestHeader requestHeader, long timeoutMillis) {
        SimpleChannel channel = channelManager.createChannel(ctx);
        ChannelHandlerContext channelHandlerContext = channel.getChannelHandlerContext();
        RemotingCommand command = LocalRemotingCommand.createRequestCommand(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, requestHeader, ctx.getLanguage());
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            RemotingCommand response = broker.getBrokerNettyServer().getChangeInvisibleTimeProcessor()
                .processRequest(channelHandlerContext, command);
            future.complete(response);
        } catch (Exception e) {
            log.error("Fail to process changeInvisibleTime command", e);
            future.completeExceptionally(e);
        }

        return createChangeInvisibleTimeResponse(future, handle);
    }

    @Override
    public CompletableFuture<AckResult> ackMessage(ProxyContext ctx, ReceiptHandle handle, String messageId,
        AckMessageRequestHeader requestHeader, long timeoutMillis) {
        SimpleChannel channel = channelManager.createChannel(ctx);
        ChannelHandlerContext channelHandlerContext = channel.getChannelHandlerContext();
        RemotingCommand command = LocalRemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, requestHeader, ctx.getLanguage());
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            RemotingCommand response = broker.getBrokerNettyServer().getAckMessageProcessor()
                .processRequest(channelHandlerContext, command);
            future.complete(response);
        } catch (Exception e) {
            log.error("Fail to process ackMessage command", e);
            future.completeExceptionally(e);
        }

        return createAckResponse(future);
    }

    @Override
    public CompletableFuture<AckResult> batchAckMessage(ProxyContext ctx, List<ReceiptHandleMessage> handleList, String consumerGroup, String topic, long timeoutMillis) {
        SimpleChannel channel = channelManager.createChannel(ctx);
        ChannelHandlerContext channelHandlerContext = channel.getChannelHandlerContext();
        RemotingCommand command = LocalRemotingCommand.createRequestCommand(RequestCode.BATCH_ACK_MESSAGE, null);

        BatchAckMessageRequestBody requestBody = createRequestBody(handleList, consumerGroup, topic);
        command.setBody(requestBody.encode());

        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            RemotingCommand response = broker.getBrokerNettyServer().getAckMessageProcessor()
                .processRequest(channelHandlerContext, command);
            future.complete(response);
        } catch (Exception e) {
            log.error("Fail to process batchAckMessage command", e);
            future.completeExceptionally(e);
        }
        return createAckResponse(future);
    }

    @Override
    public CompletableFuture<PullResult> pullMessage(ProxyContext ctx, AddressableMessageQueue messageQueue,
        PullMessageRequestHeader requestHeader, long timeoutMillis) {
        throw new NotImplementedException("pullMessage is not implemented in LocalMessageService");
    }

    @Override
    public CompletableFuture<Long> queryConsumerOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        QueryConsumerOffsetRequestHeader requestHeader, long timeoutMillis) {
        throw new NotImplementedException("queryConsumerOffset is not implemented in LocalMessageService");
    }

    @Override
    public CompletableFuture<Void> updateConsumerOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        UpdateConsumerOffsetRequestHeader requestHeader, long timeoutMillis) {
        throw new NotImplementedException("updateConsumerOffset is not implemented in LocalMessageService");
    }

    @Override
    public CompletableFuture<Set<MessageQueue>> lockBatchMQ(ProxyContext ctx, AddressableMessageQueue messageQueue,
        LockBatchRequestBody requestBody, long timeoutMillis) {
        throw new NotImplementedException("lockBatchMQ is not implemented in LocalMessageService");
    }

    @Override
    public CompletableFuture<Void> unlockBatchMQ(ProxyContext ctx, AddressableMessageQueue messageQueue,
        UnlockBatchRequestBody requestBody, long timeoutMillis) {
        throw new NotImplementedException("unlockBatchMQ is not implemented in LocalMessageService");
    }

    @Override
    public CompletableFuture<Long> getMaxOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        GetMaxOffsetRequestHeader requestHeader, long timeoutMillis) {
        throw new NotImplementedException("getMaxOffset is not implemented in LocalMessageService");
    }

    @Override
    public CompletableFuture<Long> getMinOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        GetMinOffsetRequestHeader requestHeader, long timeoutMillis) {
        throw new NotImplementedException("getMinOffset is not implemented in LocalMessageService");
    }

    @Override
    public CompletableFuture<RemotingCommand> request(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis) {
        throw new NotImplementedException("request is not implemented in LocalMessageService");
    }

    @Override
    public CompletableFuture<Void> requestOneway(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis) {
        throw new NotImplementedException("requestOneway is not implemented in LocalMessageService");
    }

    private void processSendRequest(ProxyContext ctx, RemotingCommand request, CompletableFuture<RemotingCommand> future) {
        SimpleChannel channel = channelManager.createInvocationChannel(ctx);
        InvocationContext invocationContext = new InvocationContext(future);
        channel.registerInvocationContext(request.getOpaque(), invocationContext);
        ChannelHandlerContext simpleChannelHandlerContext = channel.getChannelHandlerContext();

        try {
            RemotingCommand response = broker.getBrokerNettyServer().getSendMessageProcessor().processRequest(simpleChannelHandlerContext, request);
            if (response != null) {
                invocationContext.handle(response);
                channel.eraseInvocationContext(request.getOpaque());
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
            channel.eraseInvocationContext(request.getOpaque());
            log.error("Failed to process sendMessage command", e);
        }
    }

    private CompletableFuture<List<SendResult>> createSendResponse(CompletableFuture<RemotingCommand> future, String messageId, SendMessageRequestHeader requestHeader) {
        return future.thenApply(r -> {
            SendResult sendResult = new SendResult();
            SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) r.readCustomHeader();

            SendStatus sendStatus = codeToSendStatus(r);
            sendResult.setSendStatus(sendStatus);

            sendResult.setMsgId(messageId);
            sendResult.setMessageQueue(new MessageQueue(requestHeader.getTopic(), broker.getBrokerConfig().getBrokerName(), requestHeader.getQueueId()));
            sendResult.setQueueOffset(responseHeader.getQueueOffset());
            sendResult.setTransactionId(responseHeader.getTransactionId());
            sendResult.setOffsetMsgId(responseHeader.getMsgId());
            return Collections.singletonList(sendResult);
        });
    }

    private SendStatus codeToSendStatus(RemotingCommand r) {
        SendStatus sendStatus;
        switch (r.getCode()) {
            case ResponseCode.FLUSH_DISK_TIMEOUT: {
                sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
                break;
            }
            case ResponseCode.FLUSH_SLAVE_TIMEOUT: {
                sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
                break;
            }
            case ResponseCode.SLAVE_NOT_AVAILABLE: {
                sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
                break;
            }
            case ResponseCode.SUCCESS: {
                sendStatus = SendStatus.SEND_OK;
                break;
            }
            default: {
                throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, r.getRemark());
            }
        }

        return sendStatus;
    }

    private void processPopRequest(ProxyContext ctx, PopMessageRequestHeader requestHeader, CompletableFuture<RemotingCommand> future) {
        RemotingCommand request = LocalRemotingCommand.createRequestCommand(RequestCode.POP_MESSAGE, requestHeader, ctx.getLanguage());
        SimpleChannel channel = channelManager.createInvocationChannel(ctx);
        InvocationContext invocationContext = new InvocationContext(future);
        channel.registerInvocationContext(request.getOpaque(), invocationContext);
        ChannelHandlerContext simpleChannelHandlerContext = channel.getChannelHandlerContext();

        try {
            RemotingCommand response = broker.getBrokerNettyServer().getPopMessageProcessor().processRequest(simpleChannelHandlerContext, request);
            if (response != null) {
                invocationContext.handle(response);
                channel.eraseInvocationContext(request.getOpaque());
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
            channel.eraseInvocationContext(request.getOpaque());
            log.error("Failed to process popMessage command", e);
        }
    }

    private CompletableFuture<PopResult> createPopResponse(CompletableFuture<RemotingCommand> future, AddressableMessageQueue messageQueue, PopMessageRequestHeader requestHeader) {
        return future.thenApply(r -> {
            PopStatus popStatus = codeToPopStatus(r);
            List<MessageExt> messageExtList = initMessageExtList(r);

            PopResult popResult = new PopResult(popStatus, messageExtList);
            if (popStatus != PopStatus.FOUND) {
                return popResult;
            }

            PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) r.readCustomHeader();
            popResult.setInvisibleTime(responseHeader.getInvisibleTime());
            popResult.setPopTime(responseHeader.getPopTime());

            Map<String, Long> startOffsetInfo = ExtraInfoUtil.parseStartOffsetInfo(responseHeader.getStartOffsetInfo());
            Map<String, List<Long>> msgOffsetInfo = ExtraInfoUtil.parseMsgOffsetInfo(responseHeader.getMsgOffsetInfo());
            Map<String, Integer> orderCountInfo = ExtraInfoUtil.parseOrderCountInfo(responseHeader.getOrderCountInfo());

            // <topicMark@queueId, msg queueOffset>
            Map<String, List<Long>> sortMap = toSortMap(messageExtList);
            Map<String, String> map = new HashMap<>(5);
            for (MessageExt messageExt : messageExtList) {
                if (startOffsetInfo == null) {
                    handleNoStartOffsetInfo(messageExt, map, responseHeader, messageQueue);
                } else if (messageExt.getProperty(MessageConst.PROPERTY_POP_CK) == null) {
                    handleNoPopCK(messageExt, requestHeader, responseHeader, messageQueue, sortMap, msgOffsetInfo, startOffsetInfo, orderCountInfo);
                }
                messageExt.getProperties().computeIfAbsent(MessageConst.PROPERTY_FIRST_POP_TIME, k -> String.valueOf(responseHeader.getPopTime()));
                messageExt.setBrokerName(messageExt.getBrokerName());
                messageExt.setTopic(messageQueue.getTopic());
            }
            return popResult;
        });
    }

    private CompletableFuture<AckResult> createChangeInvisibleTimeResponse(CompletableFuture<RemotingCommand> future, ReceiptHandle handle) {
        return future.thenApply(r -> {
            ChangeInvisibleTimeResponseHeader responseHeader = (ChangeInvisibleTimeResponseHeader) r.readCustomHeader();
            AckResult ackResult = new AckResult();
            if (ResponseCode.SUCCESS == r.getCode()) {
                ackResult.setStatus(AckStatus.OK);
            } else {
                ackResult.setStatus(AckStatus.NO_EXIST);
            }
            ackResult.setPopTime(responseHeader.getPopTime());
            ackResult.setExtraInfo(ReceiptHandle.builder()
                .startOffset(handle.getStartOffset())
                .retrieveTime(responseHeader.getPopTime())
                .invisibleTime(responseHeader.getInvisibleTime())
                .reviveQueueId(responseHeader.getReviveQid())
                .topicType(handle.getTopicType())
                .brokerName(handle.getBrokerName())
                .queueId(handle.getQueueId())
                .offset(handle.getOffset())
                .build()
                .encode());
            return ackResult;
        });
    }

    private CompletableFuture<AckResult> createAckResponse(CompletableFuture<RemotingCommand> future) {
        return future.thenApply(r -> {
            AckResult ackResult = new AckResult();
            if (ResponseCode.SUCCESS == r.getCode()) {
                ackResult.setStatus(AckStatus.OK);
            } else {
                ackResult.setStatus(AckStatus.NO_EXIST);
            }
            return ackResult;
        });
    }

    private Map<String, BatchAck> createBatchAckMap(List<ReceiptHandleMessage> handleList, String consumerGroup, String topic) {
        Map<String, BatchAck> batchAckMap = new HashMap<>();
        for (ReceiptHandleMessage receiptHandleMessage : handleList) {
            String extraInfo = receiptHandleMessage.getReceiptHandle().getReceiptHandle();
            String[] extraInfoData = ExtraInfoUtil.split(extraInfo);
            String mergeKey = ExtraInfoUtil.getRetry(extraInfoData) + "@" +
                ExtraInfoUtil.getQueueId(extraInfoData) + "@" +
                ExtraInfoUtil.getCkQueueOffset(extraInfoData) + "@" +
                ExtraInfoUtil.getPopTime(extraInfoData);
            BatchAck bAck = batchAckMap.computeIfAbsent(mergeKey, k -> {
                BatchAck newBatchAck = new BatchAck();
                newBatchAck.setConsumerGroup(consumerGroup);
                newBatchAck.setTopic(topic);
                newBatchAck.setRetry(ExtraInfoUtil.getRetry(extraInfoData));
                newBatchAck.setStartOffset(ExtraInfoUtil.getCkQueueOffset(extraInfoData));
                newBatchAck.setQueueId(ExtraInfoUtil.getQueueId(extraInfoData));
                newBatchAck.setReviveQueueId(ExtraInfoUtil.getReviveQid(extraInfoData));
                newBatchAck.setPopTime(ExtraInfoUtil.getPopTime(extraInfoData));
                newBatchAck.setInvisibleTime(ExtraInfoUtil.getInvisibleTime(extraInfoData));
                newBatchAck.setBitSet(new BitSet());
                return newBatchAck;
            });
            bAck.getBitSet().set((int) (ExtraInfoUtil.getQueueOffset(extraInfoData) - ExtraInfoUtil.getCkQueueOffset(extraInfoData)));
        }

        return batchAckMap;
    }

    private BatchAckMessageRequestBody createRequestBody(List<ReceiptHandleMessage> handleList, String consumerGroup, String topic) {
        Map<String, BatchAck> batchAckMap = createBatchAckMap(handleList, consumerGroup, topic);
        BatchAckMessageRequestBody requestBody = new BatchAckMessageRequestBody();
        requestBody.setBrokerName(broker.getBrokerConfig().getBrokerName());
        requestBody.setAcks(new ArrayList<>(batchAckMap.values()));

        return requestBody;
    }

    private void handleNoPopCK(MessageExt messageExt, PopMessageRequestHeader requestHeader, PopMessageResponseHeader responseHeader, AddressableMessageQueue messageQueue,
        Map<String, List<Long>> sortMap, Map<String, List<Long>> msgOffsetInfo, Map<String, Long> startOffsetInfo,  Map<String, Integer> orderCountInfo) {
        String key = ExtraInfoUtil.getStartOffsetInfoMapKey(messageExt.getTopic(), messageExt.getQueueId());
        Long msgQueueOffset = getMsgQueueOffset(messageExt, sortMap, key, msgOffsetInfo);

        messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK,
            ExtraInfoUtil.buildExtraInfo(startOffsetInfo.get(key), responseHeader.getPopTime(), responseHeader.getInvisibleTime(),
                responseHeader.getReviveQid(), messageExt.getTopic(), messageQueue.getBrokerName(), messageExt.getQueueId(), msgQueueOffset)
        );

        setReconsumeTimes(requestHeader, orderCountInfo, key, messageExt);
    }

    private void handleNoStartOffsetInfo(MessageExt messageExt, Map<String, String> map, PopMessageResponseHeader responseHeader, AddressableMessageQueue messageQueue) {
        // we should set the check point info to extraInfo field , if the command is popMsg
        // find pop ck offset
        String key = messageExt.getTopic() + messageExt.getQueueId();
        if (!map.containsKey(messageExt.getTopic() + messageExt.getQueueId())) {
            map.put(key, ExtraInfoUtil.buildExtraInfo(messageExt.getQueueOffset(), responseHeader.getPopTime(), responseHeader.getInvisibleTime(), responseHeader.getReviveQid(),
                messageExt.getTopic(), messageQueue.getBrokerName(), messageExt.getQueueId()));
        }
        messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK, map.get(key) + MessageConst.KEY_SEPARATOR + messageExt.getQueueOffset());
    }

    private Long getMsgQueueOffset(MessageExt messageExt, Map<String, List<Long>> sortMap, String key, Map<String, List<Long>> msgOffsetInfo) {
        int index = sortMap.get(key).indexOf(messageExt.getQueueOffset());
        Long msgQueueOffset = msgOffsetInfo.get(key).get(index);
        if (msgQueueOffset != messageExt.getQueueOffset()) {
            log.warn("Queue offset [{}] of msg is strange, not equal to the stored in msg, {}", msgQueueOffset, messageExt);
        }

        return msgQueueOffset;
    }

    private PopStatus codeToPopStatus(RemotingCommand r) {
        PopStatus popStatus;

        switch (r.getCode()) {
            case ResponseCode.SUCCESS:
                popStatus = PopStatus.FOUND;
                break;
            case ResponseCode.POLLING_FULL:
                popStatus = PopStatus.POLLING_FULL;
                break;
            case ResponseCode.POLLING_TIMEOUT:
            case ResponseCode.PULL_NOT_FOUND:
                popStatus = PopStatus.POLLING_NOT_FOUND;
                break;
            default:
                throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, r.getRemark());
        }

        return popStatus;
    }

    private List<MessageExt> initMessageExtList(RemotingCommand r) {
        List<MessageExt> messageExtList = new ArrayList<>();
        if (ResponseCode.SUCCESS != r.getCode()) {
            return messageExtList;
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(r.getBody());
        messageExtList = MessageDecoder.decodesBatch(
            byteBuffer,
            true,
            false,
            true
        );

        return messageExtList;
    }

    private Map<String, List<Long>> toSortMap(List<MessageExt> messageExtList) {
        Map<String, List<Long>> sortMap = new HashMap<>(16);
        for (MessageExt messageExt : messageExtList) {
            // Value of POP_CK is used to determine whether it is a pop retry,
            // cause topic could be rewritten by broker.
            String key = ExtraInfoUtil.getStartOffsetInfoMapKey(messageExt.getTopic(),
                messageExt.getProperty(MessageConst.PROPERTY_POP_CK), messageExt.getQueueId());
            if (!sortMap.containsKey(key)) {
                sortMap.put(key, new ArrayList<>(4));
            }
            sortMap.get(key).add(messageExt.getQueueOffset());
        }

        return sortMap;
    }

    private void setReconsumeTimes(PopMessageRequestHeader requestHeader, Map<String, Integer> orderCountInfo, String key, MessageExt messageExt) {
        if (!requestHeader.isOrder() || orderCountInfo == null) {
            return;
        }

        Integer count = orderCountInfo.get(key);
        if (count != null && count > 0) {
            messageExt.setReconsumeTimes(count);
        }
    }

}
