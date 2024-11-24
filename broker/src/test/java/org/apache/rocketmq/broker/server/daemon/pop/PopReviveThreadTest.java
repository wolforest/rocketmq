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
package org.apache.rocketmq.broker.server.daemon.pop;

import com.alibaba.fastjson.JSON;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.domain.queue.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.domain.metadata.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.domain.metadata.topic.TopicConfigManager;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.constant.PopConstants;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.failover.EscapeBridge;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.common.utils.NetworkUtils;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.api.broker.pop.AckMsg;
import org.apache.rocketmq.store.api.broker.pop.PopCheckPoint;
import org.apache.rocketmq.store.api.broker.pop.PopKeyBuilder;
import org.apache.rocketmq.store.domain.timer.TimerMessageStore;
import org.apache.rocketmq.store.domain.timer.TimerState;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;

@RunWith(MockitoJUnitRunner.Silent.class)
public class PopReviveThreadTest {

    private static final String CLUSTER_NAME = "test";
    private static final String REVIVE_TOPIC = PopAckConstants.buildClusterReviveTopic(CLUSTER_NAME);
    private static final int REVIVE_QUEUE_ID = 0;
    private static final String GROUP = "group";
    private static final String TOPIC = "topic";
    private static final SocketAddress STORE_HOST = NetworkUtil.string2SocketAddress("127.0.0.1:8080");
    private static final Long INVISIBLE_TIME = 1000L;

    @Mock
    private MessageStore messageStore;
    @Mock
    private ConsumerOffsetManager consumerOffsetManager;
    @Mock
    private TopicConfigManager topicConfigManager;
    @Mock
    private TimerMessageStore timerMessageStore;
    @Mock
    private SubscriptionGroupManager subscriptionGroupManager;
    @Mock
    private Broker broker;
    @Mock
    private TimerState timerState;
    @Mock
    private EscapeBridge escapeBridge;
    private PopMessageProcessor popMessageProcessor;

    private BrokerConfig brokerConfig;
    private PopReviveThread popReviveThread;

    @Before
    public void before() {
        brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerClusterName(CLUSTER_NAME);
        when(broker.getBrokerConfig()).thenReturn(brokerConfig);
        when(broker.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(broker.getMessageStore()).thenReturn(messageStore);
        when(broker.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(broker.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        when(broker.getEscapeBridge()).thenReturn(escapeBridge);
        when(messageStore.getTimerMessageStore()).thenReturn(timerMessageStore);
        when(timerMessageStore.getTimerState()).thenReturn(timerState);
        when(timerMessageStore.getTimerState().getDequeueBehind()).thenReturn(0L);
        when(timerMessageStore.getEnqueueBehind()).thenReturn(0L);

        when(topicConfigManager.selectTopicConfig(anyString())).thenReturn(new TopicConfig());
        when(subscriptionGroupManager.findSubscriptionGroupConfig(anyString())).thenReturn(new SubscriptionGroupConfig());

        popMessageProcessor = new PopMessageProcessor(broker); // a real one, not mock
        when(broker.getBrokerNettyServer().getPopMessageProcessor()).thenReturn(popMessageProcessor);

        popReviveThread = spy(new PopReviveThread(broker, REVIVE_TOPIC, REVIVE_QUEUE_ID));
        popReviveThread.setShouldRunPopRevive(true);
    }

    @Test
    public void testWhenAckMoreThanCk() throws Throwable {
        brokerConfig.setEnableSkipLongAwaitingAck(true);
        long maxReviveOffset = 4;

        when(consumerOffsetManager.queryOffset(PopConstants.REVIVE_GROUP, REVIVE_TOPIC, REVIVE_QUEUE_ID))
            .thenReturn(0L);
        List<MessageExt> reviveMessageExtList = new ArrayList<>();
        long basePopTime = System.currentTimeMillis();
        {
            // put a pair of ck and ack
            PopCheckPoint ck = buildPopCheckPoint(1, basePopTime, 1);
            reviveMessageExtList.add(buildCkMsg(ck));
            reviveMessageExtList.add(buildAckMsg(buildAckMsg(1, basePopTime), ck.getReviveTime(), 1, basePopTime));
        }
        {
            for (int i = 2; i <= maxReviveOffset; i++) {
                long popTime = basePopTime + i;
                PopCheckPoint ck = buildPopCheckPoint(i, popTime, i);
                reviveMessageExtList.add(buildAckMsg(buildAckMsg(i, popTime), ck.getReviveTime(), i, popTime));
            }
        }
        doReturn(reviveMessageExtList, new ArrayList<>()).when(popReviveThread).getReviveMessage(anyLong(), anyInt());

        ConsumeReviveObj consumeReviveObj = popReviveThread.consumeReviveMessage();

        assertEquals(1, consumeReviveObj.getMap().size());

        ArgumentCaptor<Long> commitOffsetCaptor = ArgumentCaptor.forClass(Long.class);
        doNothing().when(consumerOffsetManager).commitOffset(anyString(), anyString(), anyString(), anyInt(), commitOffsetCaptor.capture());
        popReviveThread.mergeAndRevive(consumeReviveObj);

        assertEquals(1, commitOffsetCaptor.getValue().longValue());
    }

    @Test
    public void testSkipLongWaiteAck() throws Throwable {
        brokerConfig.setEnableSkipLongAwaitingAck(true);
        brokerConfig.setReviveAckWaitMs(TimeUnit.SECONDS.toMillis(2));
        long maxReviveOffset = 4;

        when(consumerOffsetManager.queryOffset(PopConstants.REVIVE_GROUP, REVIVE_TOPIC, REVIVE_QUEUE_ID))
            .thenReturn(0L);
        List<MessageExt> reviveMessageExtList = new ArrayList<>();
        long basePopTime = System.currentTimeMillis() - brokerConfig.getReviveAckWaitMs() * 2;
        {
            // put a pair of ck and ack
            PopCheckPoint ck = buildPopCheckPoint(1, basePopTime, 1);
            reviveMessageExtList.add(buildCkMsg(ck));
            reviveMessageExtList.add(buildAckMsg(buildAckMsg(1, basePopTime), ck.getReviveTime(), 1, basePopTime));
        }
        {
            for (int i = 2; i <= maxReviveOffset; i++) {
                long popTime = basePopTime + i;
                PopCheckPoint ck = buildPopCheckPoint(i, popTime, i);
                reviveMessageExtList.add(buildAckMsg(buildAckMsg(i, popTime), ck.getReviveTime(), i, popTime));
            }
        }
        doReturn(reviveMessageExtList, new ArrayList<>()).when(popReviveThread).getReviveMessage(anyLong(), anyInt());

        ConsumeReviveObj consumeReviveObj = popReviveThread.consumeReviveMessage();

        assertEquals(4, consumeReviveObj.getMap().size());

        ArgumentCaptor<Long> commitOffsetCaptor = ArgumentCaptor.forClass(Long.class);
        doNothing().when(consumerOffsetManager).commitOffset(anyString(), anyString(), anyString(), anyInt(), commitOffsetCaptor.capture());
        popReviveThread.mergeAndRevive(consumeReviveObj);

        assertEquals(maxReviveOffset, commitOffsetCaptor.getValue().longValue());
    }

    @Test
    public void testSkipLongWaiteAckWithSameAck() throws Throwable {
        brokerConfig.setEnableSkipLongAwaitingAck(true);
        brokerConfig.setReviveAckWaitMs(TimeUnit.SECONDS.toMillis(2));
        long maxReviveOffset = 4;

        when(consumerOffsetManager.queryOffset(PopConstants.REVIVE_GROUP, REVIVE_TOPIC, REVIVE_QUEUE_ID))
            .thenReturn(0L);
        List<MessageExt> reviveMessageExtList = new ArrayList<>();
        long basePopTime = System.currentTimeMillis() - brokerConfig.getReviveAckWaitMs() * 2;
        {
            for (int i = 2; i <= maxReviveOffset; i++) {
                long popTime = basePopTime + i;
                PopCheckPoint ck = buildPopCheckPoint(0, basePopTime, i);
                reviveMessageExtList.add(buildAckMsg(buildAckMsg(0, basePopTime), ck.getReviveTime(), i, popTime));
            }
        }
        doReturn(reviveMessageExtList, new ArrayList<>()).when(popReviveThread).getReviveMessage(anyLong(), anyInt());

        ConsumeReviveObj consumeReviveObj = popReviveThread.consumeReviveMessage();

        assertEquals(1, consumeReviveObj.getMap().size());

        ArgumentCaptor<Long> commitOffsetCaptor = ArgumentCaptor.forClass(Long.class);
        doNothing().when(consumerOffsetManager).commitOffset(anyString(), anyString(), anyString(), anyInt(), commitOffsetCaptor.capture());
        popReviveThread.mergeAndRevive(consumeReviveObj);

        assertEquals(maxReviveOffset, commitOffsetCaptor.getValue().longValue());
    }

    @Test
    public void testReviveMsgFromCk_messageFound_writeRetryOK() throws Throwable {
        PopCheckPoint ck = buildPopCheckPoint(0, 0, 0);
        PopReviveThread.ConsumeReviveObj reviveObj = new PopReviveThread.ConsumeReviveObj();
        reviveObj.map.put("", ck);
        reviveObj.endTime = System.currentTimeMillis();
        StringBuilder actualRetryTopic = new StringBuilder();

        when(escapeBridge.getMessageAsync(anyString(), anyLong(), anyInt(), anyString(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(Triple.of(new MessageExt(), "", false)));
        when(escapeBridge.putMessageToSpecificQueue(any(MessageExtBrokerInner.class))).thenAnswer(invocation -> {
            MessageExtBrokerInner msg = invocation.getArgument(0);
            actualRetryTopic.append(msg.getTopic());
            return new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK));
        });

        popReviveThread.mergeAndRevive(reviveObj);
        Assert.assertEquals(KeyBuilder.buildPopRetryTopic(TOPIC, GROUP, false), actualRetryTopic.toString());
        verify(escapeBridge, times(1)).putMessageToSpecificQueue(any(MessageExtBrokerInner.class)); // write retry
        verify(messageStore, times(0)).putMessage(any(MessageExtBrokerInner.class)); // rewrite CK
    }

    @Test
    public void testReviveMsgFromCk_messageFound_writeRetryFailed_rewriteCK() throws Throwable {
        PopCheckPoint ck = buildPopCheckPoint(0, 0, 0);
        PopReviveThread.ConsumeReviveObj reviveObj = new PopReviveThread.ConsumeReviveObj();
        reviveObj.map.put("", ck);
        reviveObj.endTime = System.currentTimeMillis();
        StringBuilder actualRetryTopic = new StringBuilder();
        StringBuilder actualReviveTopic = new StringBuilder();
        AtomicLong actualInvisibleTime = new AtomicLong(0L);

        when(escapeBridge.getMessageAsync(anyString(), anyLong(), anyInt(), anyString(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(Triple.of(new MessageExt(), "", false)));
        when(escapeBridge.putMessageToSpecificQueue(any(MessageExtBrokerInner.class))).thenAnswer(invocation -> {
            MessageExtBrokerInner msg = invocation.getArgument(0);
            actualRetryTopic.append(msg.getTopic());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED));
        });
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenAnswer(invocation -> {
            MessageExtBrokerInner msg = invocation.getArgument(0);
            actualReviveTopic.append(msg.getTopic());
            PopCheckPoint rewriteCK = JSON.parseObject(msg.getBody(), PopCheckPoint.class);
            actualInvisibleTime.set(rewriteCK.getReviveTime());
            return new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK));
        });

        popReviveThread.mergeAndRevive(reviveObj);
        Assert.assertEquals(KeyBuilder.buildPopRetryTopic(TOPIC, GROUP, false), actualRetryTopic.toString());
        Assert.assertEquals(REVIVE_TOPIC, actualReviveTopic.toString());
        Assert.assertEquals(INVISIBLE_TIME + 10 * 1000L, actualInvisibleTime.get()); // first interval is 10s
        verify(escapeBridge, times(1)).putMessageToSpecificQueue(any(MessageExtBrokerInner.class)); // write retry
        verify(messageStore, times(1)).putMessage(any(MessageExtBrokerInner.class)); // rewrite CK
    }

    @Test
    public void testReviveMsgFromCk_messageFound_writeRetryFailed_rewriteCK_end() throws Throwable {
        brokerConfig.setSkipWhenCKRePutReachMaxTimes(true);
        PopCheckPoint ck = buildPopCheckPoint(0, 0, 0);
        ck.setRePutTimes("17");
        PopReviveThread.ConsumeReviveObj reviveObj = new PopReviveThread.ConsumeReviveObj();
        reviveObj.map.put("", ck);
        reviveObj.endTime = System.currentTimeMillis();
        StringBuilder actualRetryTopic = new StringBuilder();

        when(escapeBridge.getMessageAsync(anyString(), anyLong(), anyInt(), anyString(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(Triple.of(new MessageExt(), "", false)));
        when(escapeBridge.putMessageToSpecificQueue(any(MessageExtBrokerInner.class))).thenAnswer(invocation -> {
            MessageExtBrokerInner msg = invocation.getArgument(0);
            actualRetryTopic.append(msg.getTopic());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED));
        });

        popReviveThread.mergeAndRevive(reviveObj);
        Assert.assertEquals(KeyBuilder.buildPopRetryTopic(TOPIC, GROUP, false), actualRetryTopic.toString());
        verify(escapeBridge, times(1)).putMessageToSpecificQueue(any(MessageExtBrokerInner.class)); // write retry
        verify(messageStore, times(0)).putMessage(any(MessageExtBrokerInner.class)); // rewrite CK
    }

    @Test
    public void testReviveMsgFromCk_messageFound_writeRetryFailed_rewriteCK_noEnd() throws Throwable {
        brokerConfig.setSkipWhenCKRePutReachMaxTimes(false);
        PopCheckPoint ck = buildPopCheckPoint(0, 0, 0);
        ck.setRePutTimes(Byte.MAX_VALUE + "");
        PopReviveThread.ConsumeReviveObj reviveObj = new PopReviveThread.ConsumeReviveObj();
        reviveObj.map.put("", ck);
        reviveObj.endTime = System.currentTimeMillis();
        StringBuilder actualRetryTopic = new StringBuilder();

        when(escapeBridge.getMessageAsync(anyString(), anyLong(), anyInt(), anyString(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(Triple.of(new MessageExt(), "", false)));
        when(escapeBridge.putMessageToSpecificQueue(any(MessageExtBrokerInner.class))).thenAnswer(invocation -> {
            MessageExtBrokerInner msg = invocation.getArgument(0);
            actualRetryTopic.append(msg.getTopic());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED));
        });

        popReviveThread.mergeAndRevive(reviveObj);
        Assert.assertEquals(KeyBuilder.buildPopRetryTopic(TOPIC, GROUP, false), actualRetryTopic.toString());
        verify(escapeBridge, times(1)).putMessageToSpecificQueue(any(MessageExtBrokerInner.class)); // write retry
        verify(messageStore, times(1)).putMessage(any(MessageExtBrokerInner.class)); // rewrite CK
    }

    @Test
    public void testReviveMsgFromCk_messageNotFound_noRetry() throws Throwable {
        PopCheckPoint ck = buildPopCheckPoint(0, 0, 0);
        PopReviveThread.ConsumeReviveObj reviveObj = new PopReviveThread.ConsumeReviveObj();
        reviveObj.map.put("", ck);
        reviveObj.endTime = System.currentTimeMillis();

        when(escapeBridge.getMessageAsync(anyString(), anyLong(), anyInt(), anyString(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(Triple.of(null, "", false)));

        popReviveThread.mergeAndRevive(reviveObj);
        verify(escapeBridge, times(0)).putMessageToSpecificQueue(any(MessageExtBrokerInner.class)); // write retry
        verify(messageStore, times(0)).putMessage(any(MessageExtBrokerInner.class)); // rewrite CK
    }

    @Test
    public void testReviveMsgFromCk_messageNotFound_needRetry() throws Throwable {
        PopCheckPoint ck = buildPopCheckPoint(0, 0, 0);
        PopReviveThread.ConsumeReviveObj reviveObj = new PopReviveThread.ConsumeReviveObj();
        reviveObj.map.put("", ck);
        reviveObj.endTime = System.currentTimeMillis();
        StringBuilder actualReviveTopic = new StringBuilder();
        AtomicLong actualInvisibleTime = new AtomicLong(0L);

        when(escapeBridge.getMessageAsync(anyString(), anyLong(), anyInt(), anyString(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(Triple.of(null, "", true)));
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenAnswer(invocation -> {
            MessageExtBrokerInner msg = invocation.getArgument(0);
            actualReviveTopic.append(msg.getTopic());
            PopCheckPoint rewriteCK = JSON.parseObject(msg.getBody(), PopCheckPoint.class);
            actualInvisibleTime.set(rewriteCK.getReviveTime());
            return new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK));
        });

        popReviveThread.mergeAndRevive(reviveObj);
        Assert.assertEquals(REVIVE_TOPIC, actualReviveTopic.toString());
        Assert.assertEquals(INVISIBLE_TIME + 10 * 1000L, actualInvisibleTime.get()); // first interval is 10s
        verify(escapeBridge, times(0)).putMessageToSpecificQueue(any(MessageExtBrokerInner.class)); // write retry
        verify(messageStore, times(1)).putMessage(any(MessageExtBrokerInner.class)); // rewrite CK
    }

    @Test
    public void testReviveMsgFromCk_messageNotFound_needRetry_end() throws Throwable {
        brokerConfig.setSkipWhenCKRePutReachMaxTimes(true);
        PopCheckPoint ck = buildPopCheckPoint(0, 0, 0);
        ck.setRePutTimes("17");
        PopReviveThread.ConsumeReviveObj reviveObj = new PopReviveThread.ConsumeReviveObj();
        reviveObj.map.put("", ck);
        reviveObj.endTime = System.currentTimeMillis();

        when(escapeBridge.getMessageAsync(anyString(), anyLong(), anyInt(), anyString(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(Triple.of(null, "", true)));

        popReviveThread.mergeAndRevive(reviveObj);
        verify(escapeBridge, times(0)).putMessageToSpecificQueue(any(MessageExtBrokerInner.class)); // write retry
        verify(messageStore, times(0)).putMessage(any(MessageExtBrokerInner.class)); // rewrite CK
    }

    @Test
    public void testReviveMsgFromCk_messageNotFound_needRetry_noEnd() throws Throwable {
        brokerConfig.setSkipWhenCKRePutReachMaxTimes(false);
        PopCheckPoint ck = buildPopCheckPoint(0, 0, 0);
        ck.setRePutTimes(Byte.MAX_VALUE + "");
        PopReviveThread.ConsumeReviveObj reviveObj = new PopReviveThread.ConsumeReviveObj();
        reviveObj.map.put("", ck);
        reviveObj.endTime = System.currentTimeMillis();

        when(escapeBridge.getMessageAsync(anyString(), anyLong(), anyInt(), anyString(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(Triple.of(null, "", true)));

        popReviveThread.mergeAndRevive(reviveObj);
        verify(escapeBridge, times(0)).putMessageToSpecificQueue(any(MessageExtBrokerInner.class)); // write retry
        verify(messageStore, times(1)).putMessage(any(MessageExtBrokerInner.class)); // rewrite CK
    }

    public static PopCheckPoint buildPopCheckPoint(long startOffset, long popTime, long reviveOffset) {
        PopCheckPoint ck = new PopCheckPoint();
        ck.setStartOffset(startOffset);
        ck.setPopTime(popTime);
        ck.setQueueId(0);
        ck.setCId(GROUP);
        ck.setTopic(TOPIC);
        ck.setNum((byte) 1);
        ck.setBitMap(0);
        ck.setReviveOffset(reviveOffset);
        ck.setInvisibleTime(INVISIBLE_TIME);
        ck.setBrokerName("broker-a");
        return ck;
    }

    public static AckMsg buildAckMsg(long offset, long popTime) {
        AckMsg ackMsg = new AckMsg();
        ackMsg.setAckOffset(offset);
        ackMsg.setStartOffset(offset);
        ackMsg.setConsumerGroup(GROUP);
        ackMsg.setTopic(TOPIC);
        ackMsg.setQueueId(0);
        ackMsg.setPopTime(popTime);
        ackMsg.setBrokerName("broker-a");

        return ackMsg;
    }

    public static MessageExtBrokerInner buildCkMsg(PopCheckPoint ck) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();

        msgInner.setTopic(REVIVE_TOPIC);
        msgInner.setBody(JSON.toJSONString(ck).getBytes(DataConverter.CHARSET_UTF8));
        msgInner.setQueueId(REVIVE_QUEUE_ID);
        msgInner.setTags(PopConstants.CK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(STORE_HOST);
        msgInner.setStoreHost(STORE_HOST);
        msgInner.setDeliverTimeMs(ck.getReviveTime() - PopConstants.ackTimeInterval);
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopKeyBuilder.genCkUniqueId(ck));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        msgInner.setQueueOffset(ck.getReviveOffset());

        return msgInner;
    }

    public static MessageExtBrokerInner buildAckMsg(AckMsg ackMsg, long deliverMs, long reviveOffset,
        long deliverTime) {
        MessageExtBrokerInner messageExtBrokerInner = buildAckInnerMessage(
            REVIVE_TOPIC,
            ackMsg,
            REVIVE_QUEUE_ID,
            STORE_HOST,
            deliverMs,
            PopKeyBuilder.genAckUniqueId(ackMsg)
        );
        messageExtBrokerInner.setQueueOffset(reviveOffset);
        messageExtBrokerInner.setDeliverTimeMs(deliverMs);
        messageExtBrokerInner.setStoreTimestamp(deliverTime);
        return messageExtBrokerInner;
    }

    public static MessageExtBrokerInner buildAckInnerMessage(String reviveTopic, AckMsg ackMsg, int reviveQid,
        SocketAddress host, long deliverMs, String ackUniqueId) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(reviveTopic);
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(DataConverter.CHARSET_UTF8));
        msgInner.setQueueId(reviveQid);
        msgInner.setTags(PopConstants.ACK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(host);
        msgInner.setStoreHost(host);
        msgInner.setDeliverTimeMs(deliverMs);
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, ackUniqueId);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        return msgInner;
    }
}
