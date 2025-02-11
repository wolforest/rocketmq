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
package org.apache.rocketmq.broker.domain.transaction;

import io.netty.channel.Channel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.common.lang.thread.ThreadFactoryImpl;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;

/**
 * async send check messages to client, working process:
 *  resolveHalfMsg -> executorService.run() -> sendCheckMessage
 */
public abstract class AbstractTransactionalMessageCheckListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private Broker broker;

    //queue nums of topic TRANS_CHECK_MAX_TIME_TOPIC
    protected final static int TCMT_QUEUE_NUMS = 1;

    private static volatile ExecutorService executorService;

    public AbstractTransactionalMessageCheckListener() {
    }

    public AbstractTransactionalMessageCheckListener(Broker broker) {
        this.broker = broker;
    }

    public void resolveHalfMsg(final MessageExt msgExt) {
        if (executorService == null) {
            LOGGER.error("TransactionalMessageCheckListener not init");
            return;
        }

        executorService.execute(() -> {
            try {
                sendCheckMessage(msgExt);
            } catch (Exception e) {
                LOGGER.error("Send check message error!", e);
            }
        });
    }

    /**
     * public only for test env, called by this.resolveHalfMsg
     * should be private
     *
     * @param msgExt msg
     * @throws Exception e
     */
    public void sendCheckMessage(MessageExt msgExt) throws Exception {
        CheckTransactionStateRequestHeader header = new CheckTransactionStateRequestHeader();
        header.setTopic(msgExt.getTopic());
        header.setCommitLogOffset(msgExt.getCommitLogOffset());
        header.setOffsetMsgId(msgExt.getMsgId());
        header.setMsgId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        header.setTransactionId(header.getMsgId());
        header.setTranStateTableOffset(msgExt.getQueueOffset());
        header.setBrokerName(broker.getBrokerConfig().getBrokerName());

        msgExt.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
        msgExt.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
        msgExt.setStoreSize(0);

        String groupId = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
        Channel channel = broker.getProducerManager().getAvailableChannel(groupId);
        if (channel != null) {
            broker.getBroker2Client().checkProducerTransactionState(groupId, channel, header, msgExt);
        } else {
            LOGGER.warn("Check transaction failed, channel is null. groupId={}", groupId);
        }
    }

    public Broker getBrokerController() {
        return broker;
    }

    public void shutDown() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    public synchronized void initExecutorService() {
        if (executorService == null) {
            executorService = ThreadUtils.newThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2000),
                new ThreadFactoryImpl("Transaction-msg-check-thread", broker.getBrokerIdentity()), new CallerRunsPolicy());
        }
    }

    /**
     * Inject brokerController for this listener
     *
     * @param broker brokerController
     */
    public void setBrokerController(Broker broker) {
        this.broker = broker;
        initExecutorService();
    }

    /**
     * In order to avoid check back unlimited, we will discard the message that have been checked more than a certain
     * number of times.
     *
     * @param msgExt Message to be discarded.
     */
    public abstract void resolveDiscardMsg(MessageExt msgExt);
}
