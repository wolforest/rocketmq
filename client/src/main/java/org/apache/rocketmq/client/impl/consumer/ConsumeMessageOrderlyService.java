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
package org.apache.rocketmq.client.impl.consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.common.constant.MQConstants;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.body.CMResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConsumeMessageOrderlyService implements ConsumeMessageService {
    private static final Logger log = LoggerFactory.getLogger(ConsumeMessageOrderlyService.class);
    private final static long MAX_TIME_CONSUME_CONTINUOUSLY =
        Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "60000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerOrderly messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean stopped = false;

    public ConsumeMessageOrderlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerOrderly messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<>();

        String consumerGroupTag = (consumerGroup.length() > 100 ? consumerGroup.substring(0, 100) : consumerGroup) + "_";
        initConsumeExecutor(consumerGroupTag);

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_" + consumerGroupTag));
    }

    private void initConsumeExecutor(String consumerGroupTag) {
        this.consumeExecutor = new ThreadPoolExecutor(
            this.defaultMQPushConsumer.getConsumeThreadMin(),
            this.defaultMQPushConsumer.getConsumeThreadMax(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,
            new ThreadFactoryImpl("ConsumeMessageThread_" + consumerGroupTag));
    }

    public void start() {
        startLockMQService();
    }

    private void startLockMQService() {
        if (!MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())) {
            return;
        }

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    ConsumeMessageOrderlyService.this.lockMQPeriodically();
                } catch (Throwable e) {
                    log.error("scheduleAtFixedRate lockMQPeriodically exception", e);
                }
            }
        }, 1000 * 1, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
    }

    public void shutdown(long awaitTerminateMillis) {
        this.stopped = true;
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            this.unlockAllMQ();
        }
    }

    public synchronized void unlockAllMQ() {
        this.defaultMQPushConsumerImpl.getRebalanceImpl().unlockAll(false);
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
            && corePoolSize <= Short.MAX_VALUE
            && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
    }

    @Override
    public void decCorePoolSize() {
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        log.info("consumeMessageDirectly receive new message: {}", msg);

        ConsumeMessageDirectlyResult result = initConsumeMessageDirectlyResult();
        List<MessageExt> msgs = initMsgs(msg);
        MessageQueue mq = initMessageQueue(msg, brokerName);
        ConsumeOrderlyContext context = new ConsumeOrderlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);
        consumeMessageDirectly(result, msgs, mq, context);

        log.info("consumeMessageDirectly Result: {}", result);
        return result;
    }

    private ConsumeMessageDirectlyResult initConsumeMessageDirectlyResult() {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(true);

        return result;
    }

    private List<MessageExt> initMsgs(MessageExt msg) {
        List<MessageExt> msgs = new ArrayList<>();
        msgs.add(msg);

        return msgs;
    }

    private MessageQueue initMessageQueue(MessageExt msg, String brokerName) {
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        return mq;
    }

    private void consumeMessageDirectly(ConsumeMessageDirectlyResult result, List<MessageExt> msgs, MessageQueue mq, ConsumeOrderlyContext context) {
        final long beginTime = System.currentTimeMillis();

        try {
            ConsumeOrderlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                statusToConsumeMessageDirectlyResult(result, status);
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            handleConsumeMessageDirectlyException(result, msgs, mq, e);
        }

        result.setAutoCommit(context.isAutoCommit());
        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);
    }

    private void statusToConsumeMessageDirectlyResult(ConsumeMessageDirectlyResult result, ConsumeOrderlyStatus status) {
        switch (status) {
            case COMMIT:
                result.setConsumeResult(CMResult.CR_COMMIT);
                break;
            case ROLLBACK:
                result.setConsumeResult(CMResult.CR_ROLLBACK);
                break;
            case SUCCESS:
                result.setConsumeResult(CMResult.CR_SUCCESS);
                break;
            case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                result.setConsumeResult(CMResult.CR_LATER);
                break;
            default:
                break;
        }
    }

    private void handleConsumeMessageDirectlyException(ConsumeMessageDirectlyResult result, List<MessageExt> msgs, MessageQueue mq, Throwable e) {
        result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
        result.setRemark(IOUtils.exceptionSimpleDesc(e));

        log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
            IOUtils.exceptionSimpleDesc(e),
            ConsumeMessageOrderlyService.this.consumerGroup,
            msgs,
            mq), e);
    }

    @Override
    public void submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue, boolean dispatchToConsume) {
        if (!dispatchToConsume) {
            return;
        }

        ConsumeRequest consumeRequest = new ConsumeRequest(processQueue, messageQueue);
        this.consumeExecutor.submit(consumeRequest);
    }

    @Override
    public void submitPopConsumeRequest(final List<MessageExt> msgs,
                                        final PopProcessQueue processQueue,
                                        final MessageQueue messageQueue) {
        throw new UnsupportedOperationException();
    }

    public synchronized void lockMQPeriodically() {
        if (this.stopped) {
            return;
        }

        this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
    }

    public void tryLockLaterAndReconsume(final MessageQueue mq, final ProcessQueue processQueue,
        final long delayMills) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                boolean lockOK = ConsumeMessageOrderlyService.this.lockOneMQ(mq);
                if (lockOK) {
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 10);
                } else {
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 3000);
                }
            }
        }, delayMills, TimeUnit.MILLISECONDS);
    }

    public synchronized boolean lockOneMQ(final MessageQueue mq) {
        if (!this.stopped) {
            return this.defaultMQPushConsumerImpl.getRebalanceImpl().lock(mq);
        }

        return false;
    }

    private void submitConsumeRequestLater(
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final long suspendTimeMillis
    ) {
        long timeMillis = getSuspendTimeMillis(suspendTimeMillis);

        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                ConsumeMessageOrderlyService.this.submitConsumeRequest(null, processQueue, messageQueue, true);
            }
        }, timeMillis, TimeUnit.MILLISECONDS);
    }

    private long getSuspendTimeMillis(final long suspendTimeMillis) {
        long timeMillis = suspendTimeMillis;
        if (timeMillis == -1) {
            timeMillis = this.defaultMQPushConsumer.getSuspendCurrentQueueTimeMillis();
        }

        if (timeMillis < 10) {
            timeMillis = 10;
        } else if (timeMillis > 30000) {
            timeMillis = 30000;
        }

        return timeMillis;
    }

    public boolean processConsumeResult(final List<MessageExt> msgs, final ConsumeOrderlyStatus status, final ConsumeOrderlyContext context, final ConsumeRequest consumeRequest) {
        context.setContinuable(true);
        context.setCommitOffset(-1L);
        if (context.isAutoCommit()) {
            processAutoCommitResult(msgs, status, context, consumeRequest);
        } else {
            processManuallyCommitResult(msgs, status, context, consumeRequest);
        }

        if (context.getCommitOffset() >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), context.getCommitOffset(), false);
        }

        return context.isContinuable();
    }

    private void processAutoCommitResult(final List<MessageExt> msgs, final ConsumeOrderlyStatus status, final ConsumeOrderlyContext context, final ConsumeRequest consumeRequest) {
        switch (status) {
            case COMMIT:
            case ROLLBACK:
                log.warn("the message queue consume result is illegal, we think you want to ack these message {}",
                    consumeRequest.getMessageQueue());
            case SUCCESS:
                context.setCommitOffset(consumeRequest.getProcessQueue().commit());
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                break;
            case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                suspendAutoCommitResult(msgs, context, consumeRequest);
                break;
            default:
                break;
        }
    }

    private void suspendAutoCommitResult(final List<MessageExt> msgs, final ConsumeOrderlyContext context, final ConsumeRequest consumeRequest) {
        this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
        if (!checkReconsumeTimes(msgs)) {
            context.setCommitOffset(consumeRequest.getProcessQueue().commit());
            return;
        }

        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
        this.submitConsumeRequestLater(
            consumeRequest.getProcessQueue(),
            consumeRequest.getMessageQueue(),
            context.getSuspendCurrentQueueTimeMillis());
        context.setContinuable(false);
    }

    private void processManuallyCommitResult(final List<MessageExt> msgs, final ConsumeOrderlyStatus status, final ConsumeOrderlyContext context, final ConsumeRequest consumeRequest) {
        switch (status) {
            case SUCCESS:
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                break;
            case COMMIT:
                context.setCommitOffset(consumeRequest.getProcessQueue().commit());
                break;
            case ROLLBACK:
                rollbackManuallyCommitResult(context, consumeRequest);
                break;
            case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                suspendManuallyCommitResult(msgs, context, consumeRequest);
                break;
            default:
                break;
        }
    }

    private void rollbackManuallyCommitResult(final ConsumeOrderlyContext context, final ConsumeRequest consumeRequest) {
        consumeRequest.getProcessQueue().rollback();
        this.submitConsumeRequestLater(
            consumeRequest.getProcessQueue(),
            consumeRequest.getMessageQueue(),
            context.getSuspendCurrentQueueTimeMillis());
        context.setContinuable(false);
    }

    private void suspendManuallyCommitResult(final List<MessageExt> msgs, final ConsumeOrderlyContext context, final ConsumeRequest consumeRequest) {
        this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
        if (!checkReconsumeTimes(msgs)) {
            return;
        }

        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
        this.submitConsumeRequestLater(
            consumeRequest.getProcessQueue(),
            consumeRequest.getMessageQueue(),
            context.getSuspendCurrentQueueTimeMillis());
        context.setContinuable(false);
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    private int getMaxReconsumeTimes() {
        // default reconsume times: Integer.MAX_VALUE
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return Integer.MAX_VALUE;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    private boolean checkReconsumeTimes(List<MessageExt> msgs) {
        boolean suspend = false;
        if (msgs == null || msgs.isEmpty()) {
            return false;
        }

        for (MessageExt msg : msgs) {
            if (msg.getReconsumeTimes() < getMaxReconsumeTimes()) {
                suspend = true;
                msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                continue;
            }

            MessageAccessor.setReconsumeTime(msg, String.valueOf(msg.getReconsumeTimes()));
            if (sendMessageBack(msg)) {
                continue;
            }

            suspend = true;
            msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);

        }
        return suspend;
    }

    public boolean sendMessageBack(final MessageExt msg) {
        try {
            // max reconsume times exceeded then send to dead letter queue.
            Message newMsg = new Message(MQConstants.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, org.apache.rocketmq.common.utils.StringUtils.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
            newMsg.setFlag(msg.getFlag());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

            this.defaultMQPushConsumerImpl.getmQClientFactory().getDefaultMQProducer().send(newMsg);
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    public void resetNamespace(final List<MessageExt> msgs) {
        for (MessageExt msg : msgs) {
            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    class ConsumeRequest implements Runnable {
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;

        public ConsumeRequest(ProcessQueue processQueue, MessageQueue messageQueue) {
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        @Override
        public void run() {
            if (isDropped()) {
                return;
            }

            final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);
            synchronized (objLock) {
                MessageModel model = ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel();
                if (MessageModel.BROADCASTING.equals(model) || this.processQueue.isLocked() && !this.processQueue.isLockExpired()) {
                    runWithLock();
                } else {
                    tryLockLater();
                }
            }
        }

        private void tryLockLater() {
            if (isDropped()) {
                return;
            }

            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
        }

        private void runWithLock() {
            final long beginTime = System.currentTimeMillis();
            for (boolean continueConsume = true; continueConsume; ) {
                if (!isRunnable(beginTime)) break;

                List<MessageExt> msgs = takeMessages();
                if (msgs.isEmpty()) {
                    continueConsume = false;
                    continue;
                }

                final ConsumeOrderlyContext context = new ConsumeOrderlyContext(this.messageQueue);
                ConsumeOrderlyStatus status = null;
                boolean hasException = false;
                long beginTimestamp = System.currentTimeMillis();

                try {
                    this.processQueue.getConsumeLock().lock();
                    if (isDropped()) break;

                    status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
                } catch (Throwable e) {
                    logConsumeException(e, msgs);
                    hasException = true;
                } finally {
                    this.processQueue.getConsumeLock().unlock();
                }

                ConsumeMessageContext consumeMessageContext = executeHookBefore(msgs);
                status = runAfterConsume(consumeMessageContext, status, msgs, hasException, beginTimestamp);
                continueConsume = ConsumeMessageOrderlyService.this.processConsumeResult(msgs, status, context, this);
            }

        }

        private boolean isRunnable(long beginTime) {
            if (isDropped()) return false;
            if (isLocked()) return false;
            if (isLockExpired()) return false;
            if (isTimeout(beginTime)) return false;

            return true;
        }

        private boolean isDropped() {
            if (this.processQueue.isDropped()) {
                log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                return true;
            }

            return false;
        }

        private boolean isLocked() {
            if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                && !this.processQueue.isLocked()) {
                log.warn("the message queue not locked, so consume later, {}", this.messageQueue);
                ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                return true;
            }
            return false;
        }

        private boolean isLockExpired() {
            if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                && this.processQueue.isLockExpired()) {
                log.warn("the message queue lock expired, so consume later, {}", this.messageQueue);
                ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                return true;
            }
            return false;
        }

        private boolean isTimeout(long beginTime) {
            long interval = System.currentTimeMillis() - beginTime;
            if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, messageQueue, 10);
                return true;
            }

            return false;
        }

        private ConsumeMessageContext executeHookBefore(List<MessageExt> msgs) {
            ConsumeMessageContext consumeMessageContext = null;
            if (!ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                return null;
            }

            consumeMessageContext = new ConsumeMessageContext();
            consumeMessageContext.setConsumerGroup(ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumerGroup());
            consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
            consumeMessageContext.setMq(messageQueue);
            consumeMessageContext.setMsgList(msgs);
            consumeMessageContext.setSuccess(false);
            // init consume context type
            consumeMessageContext.setProps(new HashMap<>());
            ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);

            return consumeMessageContext;
        }

        private List<MessageExt> takeMessages() {
            final int consumeBatchSize = ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
            List<MessageExt> msgs = this.processQueue.takeMessages(consumeBatchSize);
            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

            return msgs;
        }

        private void logConsumeException(Throwable e, List<MessageExt> msgs) {
            log.warn(String.format("consumeMessage exception: %s Group: %s Msgs: %s MQ: %s",
                IOUtils.exceptionSimpleDesc(e),
                ConsumeMessageOrderlyService.this.consumerGroup,
                msgs,
                messageQueue), e);
        }

        private ConsumeOrderlyStatus runAfterConsume(ConsumeMessageContext consumeMessageContext, ConsumeOrderlyStatus status, List<MessageExt> msgs, boolean hasException, long beginTimestamp) {

            logErrorStatus(status, msgs);

            long consumeRT = System.currentTimeMillis() - beginTimestamp;
            ConsumeReturnType returnType = getConsumeReturnType(consumeRT, hasException, status);
            if (null == status) {
                status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }

            executeHookAfter(consumeMessageContext, status, returnType);
            ConsumeMessageOrderlyService.this.getConsumerStatsManager().incConsumeRT(ConsumeMessageOrderlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            return status;
        }

        private void logErrorStatus(ConsumeOrderlyStatus status, List<MessageExt> msgs) {
            if (null == status
                || ConsumeOrderlyStatus.ROLLBACK == status
                || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                log.warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
                    ConsumeMessageOrderlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
            }
        }

        private ConsumeReturnType getConsumeReturnType(long consumeRT, boolean hasException, ConsumeOrderlyStatus status) {
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            if (null == status) {
                if (hasException) {
                    returnType = ConsumeReturnType.EXCEPTION;
                } else {
                    returnType = ConsumeReturnType.RETURNNULL;
                }
            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                returnType = ConsumeReturnType.TIME_OUT;
            } else if (ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                returnType = ConsumeReturnType.FAILED;
            } else if (ConsumeOrderlyStatus.SUCCESS == status) {
                returnType = ConsumeReturnType.SUCCESS;
            }

            return returnType;
        }

        private void executeHookAfter(ConsumeMessageContext consumeMessageContext, ConsumeOrderlyStatus status, ConsumeReturnType returnType) {
            if (!ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                return;
            }

            consumeMessageContext.getProps().put(MQConstants.CONSUME_CONTEXT_TYPE, returnType.name());

            consumeMessageContext.setStatus(status.toString());
            consumeMessageContext.setSuccess(ConsumeOrderlyStatus.SUCCESS == status || ConsumeOrderlyStatus.COMMIT == status);
            consumeMessageContext.setAccessChannel(defaultMQPushConsumer.getAccessChannel());
            ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
        }


    }

}
