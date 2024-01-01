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

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.lang.thread.ThreadFactoryImpl;
import org.apache.rocketmq.common.domain.message.MessageAccessor;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageQueue;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.CMResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
    private static final Logger log = LoggerFactory.getLogger(ConsumeMessageConcurrentlyService.class);
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerConcurrently messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;

    private final ScheduledExecutorService scheduledExecutorService;
    private final ScheduledExecutorService cleanExpireMsgExecutors;

    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<>();

        String consumerGroupTag = (consumerGroup.length() > 100 ? consumerGroup.substring(0, 100) : consumerGroup) + "_";

        initConsumerExecutor(consumerGroupTag);
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_" + consumerGroupTag));
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_" + consumerGroupTag));
    }

    private void initConsumerExecutor(String consumerGroupTag) {
        this.consumeExecutor = new ThreadPoolExecutor(
            this.defaultMQPushConsumer.getConsumeThreadMin(),
            this.defaultMQPushConsumer.getConsumeThreadMax(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,
            new ThreadFactoryImpl("ConsumeMessageThread_" + consumerGroupTag));
    }

    public void start() {
        startCleanExpiredMsgService();
    }

    /**
     * resend the expired msg back to MQ periodically
     */
    private void startCleanExpiredMsgService() {
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    cleanExpireMsg();
                } catch (Throwable e) {
                    log.error("scheduleAtFixedRate cleanExpireMsg exception", e);
                }
            }

        }, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
    }

    public void shutdown(long awaitTerminateMillis) {
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        this.cleanExpireMsgExecutors.shutdown();
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
        ConsumeMessageDirectlyResult result = initConsumeMessageDirectlyResult();
        List<MessageExt> msgs = initMsgs(msg, brokerName);
        MessageQueue mq = initMessageQueue(msg, brokerName);

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);
        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);
        final long beginTime = System.currentTimeMillis();
        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                statusToConsumeMessageDirectlyResult(result, status);
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(IOUtils.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                IOUtils.exceptionSimpleDesc(e),
                ConsumeMessageConcurrentlyService.this.consumerGroup,
                msgs,
                mq), e);
        }

        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);
        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    private ConsumeMessageDirectlyResult initConsumeMessageDirectlyResult() {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        return result;
    }

    private List<MessageExt> initMsgs(MessageExt msg, String brokerName) {
        msg.setBrokerName(brokerName);

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

    private void statusToConsumeMessageDirectlyResult(ConsumeMessageDirectlyResult result, ConsumeConcurrentlyStatus status) {
        switch (status) {
            case CONSUME_SUCCESS:
                result.setConsumeResult(CMResult.CR_SUCCESS);
                break;
            case RECONSUME_LATER:
                result.setConsumeResult(CMResult.CR_LATER);
                break;
            default:
                break;
        }
    }

    @Override
    public void submitConsumeRequest(final List<MessageExt> msgs, final ProcessQueue processQueue, final MessageQueue messageQueue, final boolean dispatchToConsume) {
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
        if (msgs.size() <= consumeBatchSize) {
            submitLessBatchRequest(msgs, processQueue, messageQueue);
        } else {
            submitMoreBatchRequest(msgs, processQueue, messageQueue, consumeBatchSize);
        }
    }

    private void submitLessBatchRequest(final List<MessageExt> msgs, final ProcessQueue processQueue, final MessageQueue messageQueue) {
        ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
        try {
            this.consumeExecutor.submit(consumeRequest);
        } catch (RejectedExecutionException e) {
            this.submitConsumeRequestLater(consumeRequest);
        }
    }

    private void submitMoreBatchRequest(final List<MessageExt> msgs, final ProcessQueue processQueue, final MessageQueue messageQueue, int consumeBatchSize) {
        for (int total = 0; total < msgs.size(); ) {
            List<MessageExt> msgThis = initMessageList(msgs, total, consumeBatchSize);
            submitConsumeRequestLater(msgs, processQueue, messageQueue, msgThis, total);
        }
    }

    private List<MessageExt> initMessageList(final List<MessageExt> msgs, int total, int consumeBatchSize) {
        List<MessageExt> msgThis = new ArrayList<>(consumeBatchSize);
        for (int i = 0; i < consumeBatchSize; i++, total++) {
            if (total < msgs.size()) {
                msgThis.add(msgs.get(total));
            } else {
                break;
            }
        }

        return msgThis;
    }

    private void submitConsumeRequestLater(final List<MessageExt> msgs, final ProcessQueue processQueue, final MessageQueue messageQueue, List<MessageExt> msgThis, int total) {
        ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
        try {
            this.consumeExecutor.submit(consumeRequest);
        } catch (RejectedExecutionException e) {
            for (; total < msgs.size(); total++) {
                msgThis.add(msgs.get(total));
            }

            this.submitConsumeRequestLater(consumeRequest);
        }
    }

    @Override
    public void submitPopConsumeRequest(final List<MessageExt> msgs,
        final PopProcessQueue processQueue,
        final MessageQueue messageQueue) {
        throw new UnsupportedOperationException();
    }

    private void cleanExpireMsg() {
        for (Map.Entry<MessageQueue, ProcessQueue> next : this.defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet()) {
            ProcessQueue pq = next.getValue();
            pq.cleanExpiredMsg(this.defaultMQPushConsumer);
        }
    }

    public void processConsumeResult(final ConsumeConcurrentlyStatus status, final ConsumeConcurrentlyContext context, final ConsumeRequest consumeRequest) {
        int ackIndex = context.getAckIndex();

        if (consumeRequest.getMsgs().isEmpty())
            return;

        switch (status) {
            case CONSUME_SUCCESS:
                ackIndex = processConsumeSuccess(consumeRequest, ackIndex);
                break;
            case RECONSUME_LATER:
                ackIndex = processConsumeLater(consumeRequest);
                break;
            default:
                break;
        }

        processConsumeResultByMode(context, consumeRequest, ackIndex);
        updateOffsetAfterProcessConsume(consumeRequest);
    }

    private int processConsumeSuccess(final ConsumeRequest consumeRequest, int ackIndex) {
        if (ackIndex >= consumeRequest.getMsgs().size()) {
            ackIndex = consumeRequest.getMsgs().size() - 1;
        }
        int ok = ackIndex + 1;
        int failed = consumeRequest.getMsgs().size() - ok;
        this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
        this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);

        return ackIndex;
    }

    private int processConsumeLater(final ConsumeRequest consumeRequest) {
        this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
            consumeRequest.getMsgs().size());
        return -1;
    }

    private void processConsumeResultByMode(final ConsumeConcurrentlyContext context, final ConsumeRequest consumeRequest, int ackIndex) {
        switch (this.defaultMQPushConsumer.getMessageModel()) {
            case BROADCASTING:
                processBroadCastingConsume(consumeRequest, ackIndex);
                break;
            case CLUSTERING:
                processClusteringConsume(context, consumeRequest, ackIndex);
                break;
            default:
                break;
        }
    }

    public void processBroadCastingConsume(final ConsumeRequest consumeRequest, int ackIndex) {
        for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
            MessageExt msg = consumeRequest.getMsgs().get(i);
            log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
        }
    }

    public void processClusteringConsume(final ConsumeConcurrentlyContext context, final ConsumeRequest consumeRequest, int ackIndex) {
        List<MessageExt> msgBackFailed = new ArrayList<>(consumeRequest.getMsgs().size());
        for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
            MessageExt msg = consumeRequest.getMsgs().get(i);
            // Maybe message is expired and cleaned, just ignore it.
            if (!consumeRequest.getProcessQueue().containsMessage(msg)) {
                log.info("Message is not found in its process queue; skip send-back-procedure, topic={}, "
                        + "brokerName={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getBrokerName(),
                    msg.getQueueId(), msg.getQueueOffset());
                continue;
            }
            boolean result = this.sendMessageBack(msg, context);
            if (!result) {
                msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                msgBackFailed.add(msg);
            }
        }

        if (!msgBackFailed.isEmpty()) {
            consumeRequest.getMsgs().removeAll(msgBackFailed);

            this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
        }
    }

    private void updateOffsetAfterProcessConsume(ConsumeRequest consumeRequest) {
        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
        if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
        }
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
        int delayLevel = context.getDelayLevelWhenNextConsume();

        // Wrap topic with namespace before sending back message.
        msg.setTopic(this.defaultMQPushConsumer.withNamespace(msg.getTopic()));
        try {
            this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, this.defaultMQPushConsumer.queueWithNamespace(context.getMessageQueue()));
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg, e);
        }

        return false;
    }

    private void submitConsumeRequestLater(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue, true);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    class ConsumeRequest implements Runnable {
        private final List<MessageExt> msgs;
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;

        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }


        @Override
        public void run() {
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped. group={} {}", ConsumeMessageConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }

            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            ConsumeConcurrentlyStatus status = null;
            defaultMQPushConsumerImpl.tryResetPopRetryTopic(msgs, consumerGroup);
            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

            ConsumeMessageContext consumeMessageContext = null;
            consumeMessageContext = executeHookBefore();

            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;

            try {
                initMsgs(msgs);
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
            } catch (Throwable e) {
                logConsumeException(e, msgs);
                hasException = true;
            }

            long consumeRT = System.currentTimeMillis() - beginTimestamp;
            ConsumeReturnType returnType = getConsumeReturnType(consumeRT, hasException, status);
            status = logErrorStatus(status, msgs);

            executeHookAfter(consumeMessageContext, returnType, status);
            incConsumeRT(consumeRT);
            processConsumeResult(context, status);
        }

        public List<MessageExt> getMsgs() {
            return msgs;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        private ConsumeMessageContext executeHookBefore() {
            if (!ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                return null;
            }

            ConsumeMessageContext consumeMessageContext = new ConsumeMessageContext();
            consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
            consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
            consumeMessageContext.setProps(new HashMap<>());
            consumeMessageContext.setMq(messageQueue);
            consumeMessageContext.setMsgList(msgs);
            consumeMessageContext.setSuccess(false);
            ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);

            return consumeMessageContext;
        }

        private ConsumeReturnType getConsumeReturnType(long consumeRT, boolean hasException, ConsumeConcurrentlyStatus status) {
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            if (null == status) {
                if (hasException) {
                    returnType = ConsumeReturnType.EXCEPTION;
                } else {
                    returnType = ConsumeReturnType.RETURNNULL;
                }
            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                returnType = ConsumeReturnType.TIME_OUT;
            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                returnType = ConsumeReturnType.FAILED;
            } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                returnType = ConsumeReturnType.SUCCESS;
            }
            return returnType;
        }

        private void initMsgs(List<MessageExt> msgs) {
            if (msgs == null || msgs.isEmpty()) {
                return;
            }

            for (MessageExt msg : msgs) {
                MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
            }
        }

        private void logConsumeException(Throwable e, List<MessageExt> msgs) {
            log.warn(String.format("consumeMessage exception: %s Group: %s Msgs: %s MQ: %s",
                IOUtils.exceptionSimpleDesc(e),
                ConsumeMessageConcurrentlyService.this.consumerGroup,
                msgs,
                messageQueue), e);
        }

        private ConsumeConcurrentlyStatus logErrorStatus(ConsumeConcurrentlyStatus status, List<MessageExt> msgs) {
            if (null != status) {
                return status;
            }

            log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                ConsumeMessageConcurrentlyService.this.consumerGroup,
                msgs,
                messageQueue);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }

        private void executeHookAfter(ConsumeMessageContext consumeMessageContext, ConsumeReturnType returnType, ConsumeConcurrentlyStatus status) {
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MQConstants.CONSUME_CONTEXT_TYPE, returnType.name());

                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                consumeMessageContext.setAccessChannel(defaultMQPushConsumer.getAccessChannel());
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }
        }

        private void incConsumeRT(long consumeRT) {
            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager()
                .incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);
        }

        private void processConsumeResult(ConsumeConcurrentlyContext context, ConsumeConcurrentlyStatus status) {
            if (!processQueue.isDropped()) {
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            } else {
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
            }
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

    }
}
