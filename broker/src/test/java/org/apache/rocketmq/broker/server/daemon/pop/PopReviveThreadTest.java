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
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.common.utils.NetworkUtils;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.api.broker.pop.AckMsg;
import org.apache.rocketmq.store.api.broker.pop.PopCheckPoint;
import org.apache.rocketmq.store.api.broker.pop.PopKeyBuilder;
import org.apache.rocketmq.store.domain.timer.TimerMessageStore;
import org.apache.rocketmq.store.domain.timer.model.TimerState;
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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class PopReviveThreadTest {

    private static final String REVIVE_TOPIC = PopConstants.REVIVE_TOPIC + "test";
    private static final int REVIVE_QUEUE_ID = 0;
    private static final String GROUP = "group";
    private static final String TOPIC = "topic";
    private static final SocketAddress STORE_HOST = NetworkUtils.string2SocketAddress("127.0.0.1:8080");

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

    private BrokerConfig brokerConfig;
    private PopReviveThread popReviveThread;

    @Before
    public void before() {
        brokerConfig = new BrokerConfig();

        when(broker.getBrokerConfig()).thenReturn(brokerConfig);
        when(broker.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(broker.getMessageStore()).thenReturn(messageStore);
        when(broker.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(broker.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        when(messageStore.getTimerMessageStore()).thenReturn(timerMessageStore);
        when(timerMessageStore.getTimerState()).thenReturn(timerState);
        when(timerMessageStore.getTimerState().getDequeueBehind()).thenReturn(0L);
        when(timerMessageStore.getEnqueueBehind()).thenReturn(0L);

        when(topicConfigManager.selectTopicConfig(anyString())).thenReturn(new TopicConfig());
        when(subscriptionGroupManager.findSubscriptionGroupConfig(anyString())).thenReturn(new SubscriptionGroupConfig());

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
        ck.setInvisibleTime(1000);
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
