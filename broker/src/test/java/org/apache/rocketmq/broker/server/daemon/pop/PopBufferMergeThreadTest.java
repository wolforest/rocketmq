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

import io.netty.channel.Channel;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.connection.ClientChannelInfo;
import org.apache.rocketmq.broker.server.daemon.schedule.ScheduleMessageService;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.utils.ReflectUtils;
import org.apache.rocketmq.common.utils.SystemUtils;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.store.api.broker.pop.AckMsg;
import org.apache.rocketmq.store.api.broker.pop.PopCheckPoint;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.apache.rocketmq.broker.api.controller.PullMessageProcessorTest.createConsumerData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.Silent.class)
public class PopBufferMergeThreadTest {
    @Spy
    private Broker broker = new Broker(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private DefaultMessageStore messageStore;
    private ScheduleMessageService scheduleMessageService;
    private ClientChannelInfo clientChannelInfo;
    private String group = "FooBarGroup";
    private String topic = "FooBar";

    @Before
    public void init() throws Exception {
        ReflectUtils.writeField(broker.getBrokerConfig(), "enablePopBufferMerge", true, true);
        broker.setMessageStore(messageStore);
        scheduleMessageService = new ScheduleMessageService(broker);
        scheduleMessageService.parseDelayLevel();
        Channel mockChannel = mock(Channel.class);
        broker.getTopicConfigManager().getTopicConfigTable().put(topic, new TopicConfig());
        clientChannelInfo = new ClientChannelInfo(mockChannel);
        ConsumerData consumerData = createConsumerData(group, topic);
        broker.getConsumerManager().registerConsumer(
            consumerData.getGroupName(),
            clientChannelInfo,
            consumerData.getConsumeType(),
            consumerData.getMessageModel(),
            consumerData.getConsumeFromWhere(),
            consumerData.getSubscriptionDataSet(),
            false);
    }

    @Test(timeout = 10_000)
    public void testBasic() throws Exception {
        // This test case fails on Windows in CI pipeline
        // Disable it for later fix
        Assume.assumeFalse(SystemUtils.isWindows());
        PopBufferMergeThread popBufferMergeThread = new PopBufferMergeThread(broker);
        popBufferMergeThread.start();
        PopCheckPoint ck = new PopCheckPoint();
        ck.setBitMap(0);
        int msgCnt = 1;
        ck.setNum((byte) msgCnt);
        long popTime = System.currentTimeMillis() - 1000;
        ck.setPopTime(popTime);
        int invisibleTime = 30_000;
        ck.setInvisibleTime(invisibleTime);
        int offset = 100;
        ck.setStartOffset(offset);
        ck.setCId(group);
        ck.setTopic(topic);
        int queueId = 0;
        ck.setQueueId(queueId);

        int reviveQid = 0;
        long nextBeginOffset = 101L;
        long ackOffset = offset;
        AckMsg ackMsg = new AckMsg();
        ackMsg.setAckOffset(ackOffset);
        ackMsg.setStartOffset(offset);
        ackMsg.setConsumerGroup(group);
        ackMsg.setTopic(topic);
        ackMsg.setQueueId(queueId);
        ackMsg.setPopTime(popTime);
        try {
            assertThat(popBufferMergeThread.cacheCheckPoint(ck, reviveQid, ackOffset, nextBeginOffset)).isTrue();
            assertThat(popBufferMergeThread.getLatestOffset(topic, group, queueId)).isEqualTo(nextBeginOffset);
            Thread.sleep(1000); // wait background threads of PopBufferMergeService run for some time
            assertThat(popBufferMergeThread.addAckMsg(reviveQid, ackMsg)).isTrue();
            assertThat(popBufferMergeThread.getLatestOffset(topic, group, queueId)).isEqualTo(nextBeginOffset);
        } finally {
            popBufferMergeThread.shutdown(true);
        }
    }
}
