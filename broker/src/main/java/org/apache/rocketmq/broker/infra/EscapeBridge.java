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

package org.apache.rocketmq.broker.infra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.domain.transaction.queue.TransactionalMessageUtil;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.lang.Pair;
import org.apache.rocketmq.common.lang.thread.ThreadFactoryImpl;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.domain.message.MessageQueue;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.utils.StringUtils;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.store.api.dto.GetMessageResult;
import org.apache.rocketmq.store.api.dto.GetMessageStatus;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;

public class EscapeBridge {
    protected static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long SEND_TIMEOUT = 3000L;
    private static final long DEFAULT_PULL_TIMEOUT_MILLIS = 1000 * 10L;
    private final String innerProducerGroupName;
    private final String innerConsumerGroupName;

    private final Broker broker;

    private ExecutorService defaultAsyncSenderExecutor;

    public EscapeBridge(Broker broker) {
        this.broker = broker;
        this.innerProducerGroupName = "InnerProducerGroup_" + broker.getBrokerConfig().getBrokerName() + "_" + broker.getBrokerConfig().getBrokerId();
        this.innerConsumerGroupName = "InnerConsumerGroup_" + broker.getBrokerConfig().getBrokerName() + "_" + broker.getBrokerConfig().getBrokerId();
    }

    public void start() throws Exception {
        initAsyncSenderExecutor();
    }

    public void shutdown() {
        if (null != this.defaultAsyncSenderExecutor) {
            this.defaultAsyncSenderExecutor.shutdown();
        }
    }

    public Pair<GetMessageStatus, MessageExt> getMessage(String topic, long offset, int queueId, String brokerName, boolean deCompressBody) {
        return getMessageAsync(topic, offset, queueId, brokerName, deCompressBody).join();
    }

    /**
     * called by PopReviveService
     *
     * @param topic topic
     * @param offset offset
     * @param queueId queueId
     * @param brokerName brokerName
     * @param deCompressBody doCompressBody
     * @return messageFuture
     */
    public CompletableFuture<Pair<GetMessageStatus, MessageExt>> getMessageAsync(String topic, long offset, int queueId, String brokerName, boolean deCompressBody) {
        MessageStore messageStore = broker.getBrokerMessageService().getMessageStoreByBrokerName(brokerName);
        if (messageStore == null) {
            return getMessageFromRemoteAsync(topic, offset, queueId, brokerName)
                .thenApply(msg -> {
                    if (msg == null) {
                        return new Pair<>(GetMessageStatus.MESSAGE_WAS_REMOVING, null);
                    }
                    return new Pair<>(GetMessageStatus.FOUND, msg);
                });
        }

        return messageStore.getMessageAsync(innerConsumerGroupName, topic, queueId, offset, 1, null)
            .thenApply(result -> {
                if (result == null) {
                    LOG.warn("getMessageResult is null , innerConsumerGroupName {}, topic {}, offset {}, queueId {}", innerConsumerGroupName, topic, offset, queueId);
                    return new Pair<>(GetMessageStatus.MESSAGE_WAS_REMOVING, null);
                }

                List<MessageExt> list = decodeMsgList(result, deCompressBody);
                if (list == null || list.isEmpty()) {
                    LOG.warn("Can not get msg , topic {}, offset {}, queueId {}, result is {}", topic, offset, queueId, result);
                    return new Pair<>(result.getStatus(), null);
                }

                return new Pair<>(result.getStatus(), list.get(0));
            });
    }

    /**
     * depends on config(!canNotEscape())
     *
     * called by
     *  - TransactionalMessageBridge
     *  - TimerMessageStore
     *  - PutResultProcess
     * @param messageExt message
     * @return putResult
     */
    public PutMessageResult putMessage(MessageExtBrokerInner messageExt) {
        Broker masterBroker = this.broker.peekMasterBroker();
        if (masterBroker != null) {
            return masterBroker.getMessageStore().putMessage(messageExt);
        }

        if (canNotEscape()) {
            LOG.warn("Put message failed, enableSlaveActingMaster={}, enableRemoteEscape={}.",
                this.broker.getBrokerConfig().isEnableSlaveActingMaster(), this.broker.getBrokerConfig().isEnableRemoteEscape());
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        try {
            messageExt.setWaitStoreMsgOK(false);
            final SendResult sendResult = putMessageToRemoteBroker(messageExt);
            return transformSendResult2PutResult(sendResult);
        } catch (Exception e) {
            LOG.error("sendMessageInFailover to remote failed", e);
            return new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true);
        }
    }

    /**
     * depends on config(!canNotEscape())
     *
     * called by DeliverDelayedMessageTimerTask
     * @param messageExt msg
     * @return future result
     */
    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner messageExt) {
        Broker masterBroker = this.broker.peekMasterBroker();
        if (masterBroker != null) {
            return masterBroker.getMessageStore().asyncPutMessage(messageExt);
        }

        if (canNotEscape()) {
            LOG.warn("Put message failed, enableSlaveActingMaster={}, enableRemoteEscape={}.",
                this.broker.getBrokerConfig().isEnableSlaveActingMaster(), this.broker.getBrokerConfig().isEnableRemoteEscape());
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null));
        }

        try {
            messageExt.setWaitStoreMsgOK(false);

            final TopicPublishInfo topicPublishInfo = this.broker.getTopicRouteInfoManager().tryToFindTopicPublishInfo(messageExt.getTopic());
            final String producerGroup = getProducerGroup(messageExt);

            final MessageQueue mqSelected = topicPublishInfo.selectOneMessageQueue();
            messageExt.setQueueId(mqSelected.getQueueId());

            final String brokerNameToSend = mqSelected.getBrokerName();
            final String brokerAddrToSend = this.broker.getTopicRouteInfoManager().findBrokerAddressInPublish(brokerNameToSend);
            final CompletableFuture<SendResult> future = this.broker.getClusterClient().sendMessageToSpecificBrokerAsync(brokerAddrToSend,
                brokerNameToSend, messageExt,
                producerGroup, SEND_TIMEOUT);

            return future.exceptionally(throwable -> null)
                .thenApplyAsync(this::transformSendResult2PutResult, this.defaultAsyncSenderExecutor)
                .exceptionally(throwable -> transformSendResult2PutResult(null));

        } catch (Exception e) {
            LOG.error("sendMessageInFailover to remote failed", e);
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true));
        }
    }

    /**
     * depends on config(!canNotEscape())
     *
     * called by
     *  - AckMessageProcessor
     *  - ChangeInvisibleTimeProcessor
     *  - PopReviveThread(PopReviveService)
     *  - PopBufferMergeThread(PopBufferMergeService)
     *
     * @param messageExt msg
     * @return put result
     */
    public PutMessageResult putMessageToSpecificQueue(MessageExtBrokerInner messageExt) {
        Broker masterBroker = this.broker.peekMasterBroker();
        if (masterBroker != null) {
            return masterBroker.getMessageStore().putMessage(messageExt);
        }

        if (canNotEscape()) {
            LOG.warn("Put message to specific queue failed, enableSlaveActingMaster={}, enableRemoteEscape={}.", this.broker.getBrokerConfig().isEnableSlaveActingMaster(), this.broker.getBrokerConfig().isEnableRemoteEscape());
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        try {
            messageExt.setWaitStoreMsgOK(false);

            final TopicPublishInfo topicPublishInfo = this.broker.getTopicRouteInfoManager().tryToFindTopicPublishInfo(messageExt.getTopic());
            List<MessageQueue> mqs = topicPublishInfo.getMessageQueueList();

            if (null == mqs || mqs.isEmpty()) {
                return new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true);
            }

            String id = messageExt.getTopic() + messageExt.getStoreHost();
            final int index = Math.floorMod(id.hashCode(), mqs.size());

            MessageQueue mq = mqs.get(index);
            messageExt.setQueueId(mq.getQueueId());

            String brokerNameToSend = mq.getBrokerName();
            String brokerAddrToSend = this.broker.getTopicRouteInfoManager().findBrokerAddressInPublish(brokerNameToSend);
            final SendResult sendResult = this.broker.getClusterClient().sendMessageToSpecificBroker(brokerAddrToSend, brokerNameToSend, messageExt, this.getProducerGroup(messageExt), SEND_TIMEOUT);

            return transformSendResult2PutResult(sendResult);
        } catch (Exception e) {
            LOG.error("sendMessageInFailover to remote failed", e);
            return new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true);
        }
    }

    private SendResult putMessageToRemoteBroker(MessageExtBrokerInner messageExt) {
        final boolean isTransHalfMessage = TransactionalMessageUtil.buildHalfTopic().equals(messageExt.getTopic());
        MessageExtBrokerInner messageToPut = messageExt;
        if (isTransHalfMessage) {
            messageToPut = TransactionalMessageUtil.buildTransactionalMessageFromHalfMessage(messageExt);
        }
        final TopicPublishInfo topicPublishInfo = this.broker.getTopicRouteInfoManager().tryToFindTopicPublishInfo(messageToPut.getTopic());
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            LOG.warn("putMessageToRemoteBroker: no route info of topic {} when escaping message, msgId={}",
                messageToPut.getTopic(), messageToPut.getMsgId());
            return null;
        }

        final MessageQueue mqSelected = topicPublishInfo.selectOneMessageQueue(this.broker.getBrokerConfig().getBrokerName());

        messageToPut.setQueueId(mqSelected.getQueueId());

        final String brokerNameToSend = mqSelected.getBrokerName();
        final String brokerAddrToSend = this.broker.getTopicRouteInfoManager().findBrokerAddressInPublish(brokerNameToSend);

        final long beginTimestamp = System.currentTimeMillis();
        try {
            final SendResult sendResult = this.broker.getClusterClient().sendMessageToSpecificBroker(
                brokerAddrToSend, brokerNameToSend,
                messageToPut, this.getProducerGroup(messageToPut), SEND_TIMEOUT);
            if (null != sendResult && SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
                return sendResult;
            } else {
                LOG.error("Escaping failed! cost {}ms, Topic: {}, MsgId: {}, Broker: {}",
                    System.currentTimeMillis() - beginTimestamp, messageExt.getTopic(),
                    messageExt.getMsgId(), brokerNameToSend);
            }
        } catch (RemotingException | MQBrokerException e) {
            LOG.error(String.format("putMessageToRemoteBroker exception, MsgId: %s, RT: %sms, Broker: %s",
                messageToPut.getMsgId(), System.currentTimeMillis() - beginTimestamp, mqSelected), e);
        } catch (InterruptedException e) {
            LOG.error(String.format("putMessageToRemoteBroker interrupted, MsgId: %s, RT: %sms, Broker: %s",
                messageToPut.getMsgId(), System.currentTimeMillis() - beginTimestamp, mqSelected), e);
            Thread.currentThread().interrupt();
        }

        return null;
    }

    private String getProducerGroup(MessageExtBrokerInner messageExt) {
        if (null == messageExt) {
            return this.innerProducerGroupName;
        }
        String producerGroup = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
        if (StringUtils.isEmpty(producerGroup)) {
            producerGroup = this.innerProducerGroupName;
        }
        return producerGroup;
    }

    private PutMessageResult transformSendResult2PutResult(SendResult sendResult) {
        if (sendResult == null) {
            return new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true);
        }
        switch (sendResult.getSendStatus()) {
            case SEND_OK:
                return new PutMessageResult(PutMessageStatus.PUT_OK, null, true);
            case SLAVE_NOT_AVAILABLE:
                return new PutMessageResult(PutMessageStatus.SLAVE_NOT_AVAILABLE, null, true);
            case FLUSH_DISK_TIMEOUT:
                return new PutMessageResult(PutMessageStatus.FLUSH_DISK_TIMEOUT, null, true);
            case FLUSH_SLAVE_TIMEOUT:
                return new PutMessageResult(PutMessageStatus.FLUSH_SLAVE_TIMEOUT, null, true);
            default:
                return new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true);
        }
    }

    protected List<MessageExt> decodeMsgList(GetMessageResult getMessageResult, boolean deCompressBody) {
        List<MessageExt> foundList = new ArrayList<>();
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            if (messageBufferList == null) {
                return foundList;
            }

            for (int i = 0; i < messageBufferList.size(); i++) {
                ByteBuffer bb = messageBufferList.get(i);
                if (bb == null) {
                    LOG.error("bb is null {}", getMessageResult);
                    continue;
                }
                MessageExt msgExt = MessageDecoder.decode(bb, true, deCompressBody);
                if (msgExt == null) {
                    LOG.error("decode msgExt is null {}", getMessageResult);
                    continue;
                }
                // use CQ offset, not offset in Message
                msgExt.setQueueOffset(getMessageResult.getMessageQueueOffset().get(i));
                foundList.add(msgExt);
            }
        } finally {
            getMessageResult.release();
        }

        return foundList;
    }

    protected MessageExt getMessageFromRemote(String topic, long offset, int queueId, String brokerName) {
        return getMessageFromRemoteAsync(topic, offset, queueId, brokerName).join();
    }

    private boolean canNotEscape() {
        if (!this.broker.getBrokerConfig().isEnableSlaveActingMaster()) {
            return true;
        }

        return !this.broker.getBrokerConfig().isEnableRemoteEscape();
    }

    private void initAsyncSenderExecutor() {
        if (canNotEscape()) {
            return;
        }

        final BlockingQueue<Runnable> asyncSenderThreadPoolQueue = new LinkedBlockingQueue<>(50000);
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        this.defaultAsyncSenderExecutor = ThreadUtils.newThreadPoolExecutor(
            availableProcessors,
            availableProcessors,
            1000 * 60,
            TimeUnit.MILLISECONDS,
            asyncSenderThreadPoolQueue,
            new ThreadFactoryImpl("AsyncEscapeBridgeExecutor_", this.broker.getBrokerIdentity())
        );
        LOG.info("init executor for escaping messages asynchronously success.");
    }

    protected CompletableFuture<MessageExt> getMessageFromRemoteAsync(String topic, long offset, int queueId, String brokerName) {
        try {
            String brokerAddr = this.broker.getTopicRouteInfoManager().findBrokerAddressInSubscribe(brokerName, MQConstants.MASTER_ID, false);
            if (null == brokerAddr) {
                this.broker.getTopicRouteInfoManager().updateTopicRouteInfoFromNameServer(topic, true, false);
                brokerAddr = this.broker.getTopicRouteInfoManager().findBrokerAddressInSubscribe(brokerName, MQConstants.MASTER_ID, false);

                if (null == brokerAddr) {
                    LOG.warn("can't find broker address for topic {}", topic);
                    return CompletableFuture.completedFuture(null);
                }
            }

            return this.broker.getClusterClient().pullMessageFromSpecificBrokerAsync(brokerName,
                brokerAddr, this.innerConsumerGroupName, topic, queueId, offset, 1, DEFAULT_PULL_TIMEOUT_MILLIS)
                .thenApply(pullResult -> {
                    if (pullResult.getPullStatus().equals(PullStatus.FOUND) && !pullResult.getMsgFoundList().isEmpty()) {
                        return pullResult.getMsgFoundList().get(0);
                    }
                    return null;
                });
        } catch (Exception e) {
            LOG.error("Get message from remote failed.", e);
        }

        return CompletableFuture.completedFuture(null);
    }
}
