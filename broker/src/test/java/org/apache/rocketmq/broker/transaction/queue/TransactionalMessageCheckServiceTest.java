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
package org.apache.rocketmq.broker.transaction.queue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.server.BrokerController;
import org.apache.rocketmq.broker.domain.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.domain.transaction.TransactionalMessageCheckService;
import org.apache.rocketmq.broker.domain.transaction.queue.TransactionalMessageBridge;
import org.apache.rocketmq.broker.domain.transaction.queue.TransactionalMessageUtil;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.domain.message.MessageQueue;
import org.apache.rocketmq.common.domain.topic.TopicValidator;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.api.dto.AppendMessageResult;
import org.apache.rocketmq.store.api.dto.AppendMessageStatus;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TransactionalMessageCheckServiceTest {
    private TransactionalMessageCheckService transactionalMessageCheckService;
    @Mock
    private TransactionalMessageBridge bridge;
    @Mock
    private AbstractTransactionalMessageCheckListener listener;

    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(),
        new NettyClientConfig(), new MessageStoreConfig());

    @Before
    public void init() {
        when(bridge.getBrokerController()).thenReturn(brokerController);
        listener.setBrokerController(brokerController);
        transactionalMessageCheckService = new TransactionalMessageCheckService(brokerController, bridge, listener);
    }

    @Test
    public void testCheck_withDiscard() {
        when(bridge.fetchMessageQueues(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC)).thenReturn(createMessageQueueSet(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC));
        when(bridge.getHalfMessage(0, 0, 1)).thenReturn(createDiscardPullResult(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC, 5, "hellp", 1));
        //when(bridge.getHalfMessage(0, 1, 1)).thenReturn(createPullResult(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC, 6, "hellp", 0));
        when(bridge.getOpMessage(anyInt(), anyLong(), anyInt())).thenReturn(createOpPulResult(TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC, 1, "10", 1));
        when(bridge.getBrokerController()).thenReturn(this.brokerController);
        long timeOut = this.brokerController.getBrokerConfig().getTransactionTimeOut();
        int checkMax = this.brokerController.getBrokerConfig().getTransactionCheckMax();
        final AtomicInteger checkMessage = new AtomicInteger(0);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                checkMessage.addAndGet(1);
                return null;
            }
        }).when(listener).resolveDiscardMsg(any(MessageExt.class));
        transactionalMessageCheckService.check(timeOut, checkMax, listener);
        assertThat(checkMessage.get()).isEqualTo(1);
    }

    @Test
    public void testCheck_withCheck() {
        when(bridge.fetchMessageQueues(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC)).thenReturn(createMessageQueueSet(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC));
        when(bridge.getHalfMessage(0, 0, 1)).thenReturn(createPullResult(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC, 5, "hello", 1));
        when(bridge.getHalfMessage(0, 1, 1)).thenReturn(createPullResult(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC, 6, "hellp", 0));
        when(bridge.getOpMessage(anyInt(), anyLong(), anyInt())).thenReturn(createPullResult(TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC, 1, "5", 0));
        when(bridge.getBrokerController()).thenReturn(this.brokerController);
        when(bridge.renewHalfMessageInner(any(MessageExtBrokerInner.class))).thenReturn(createMessageBrokerInner());
        when(bridge.putMessageReturnResult(any(MessageExtBrokerInner.class)))
            .thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        long timeOut = this.brokerController.getBrokerConfig().getTransactionTimeOut();
        final int checkMax = this.brokerController.getBrokerConfig().getTransactionCheckMax();
        final AtomicInteger checkMessage = new AtomicInteger(0);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                checkMessage.addAndGet(1);
                return checkMessage;
            }
        }).when(listener).resolveHalfMsg(any(MessageExt.class));
        transactionalMessageCheckService.check(timeOut, checkMax, listener);
        assertThat(checkMessage.get()).isEqualTo(1);
    }

    private PullResult createDiscardPullResult(String topic, long queueOffset, String body, int size) {
        PullResult result = createPullResult(topic, queueOffset, body, size);
        List<MessageExt> msgs = result.getMsgFoundList();
        for (MessageExt msg : msgs) {
            msg.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, "100000");
        }
        return result;
    }

    private PullResult createOpPulResult(String topic, long queueOffset, String body, int size) {
        PullResult result = createPullResult(topic, queueOffset, body, size);
        List<MessageExt> msgs = result.getMsgFoundList();
        for (MessageExt msg : msgs) {
            msg.setTags(TransactionalMessageUtil.REMOVE_TAG);
        }
        return result;
    }

    private PullResult createImmunityPulResult(String topic, long queueOffset, String body, int size) {
        PullResult result = createPullResult(topic, queueOffset, body, size);
        List<MessageExt> msgs = result.getMsgFoundList();
        for (MessageExt msg : msgs) {
            msg.putUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS, "0");
        }
        return result;
    }

    private PullResult createPullResult(String topic, long queueOffset, String body, int size) {
        PullResult result = null;
        if (0 == size) {
            result = new PullResult(PullStatus.NO_NEW_MSG, 1, 0, 1,
                null);
        } else {
            result = new PullResult(PullStatus.FOUND, 1, 0, 1,
                getMessageList(queueOffset, topic, body, 1));
            return result;
        }
        return result;
    }

    private List<MessageExt> getMessageList(long queueOffset, String topic, String body, int size) {
        List<MessageExt> msgs = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            MessageExt messageExt = createMessageBrokerInner(queueOffset, topic, body);
            msgs.add(messageExt);
        }
        return msgs;
    }

    private Set<MessageQueue> createMessageQueueSet(String topic) {
        Set<MessageQueue> messageQueues = new HashSet<>();
        MessageQueue messageQueue = new MessageQueue(topic, "DefaultCluster", 0);
        messageQueues.add(messageQueue);
        return messageQueues;
    }

    private MessageExtBrokerInner createMessageBrokerInner(long queueOffset, String topic, String body) {
        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        inner.setBornTimestamp(System.currentTimeMillis() - 80000);
        inner.setTransactionId("123456123");
        inner.setTopic(topic);
        inner.setQueueOffset(queueOffset);
        inner.setBody(body.getBytes());
        inner.setMsgId("123456123");
        inner.setQueueId(0);
        inner.setTopic("hello");
        return inner;
    }

    private MessageExtBrokerInner createMessageBrokerInner() {
        return createMessageBrokerInner(1, "testTopic", "hello world");
    }
}
