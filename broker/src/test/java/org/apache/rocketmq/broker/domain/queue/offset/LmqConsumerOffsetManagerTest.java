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

package org.apache.rocketmq.broker.domain.queue.offset;

import java.io.File;
import java.util.Map;
import org.apache.rocketmq.broker.domain.metadata.subscription.LmqSubscriptionGroupManager;
import org.apache.rocketmq.broker.domain.metadata.topic.LmqTopicConfigManager;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Test;
import org.mockito.Spy;

import static org.assertj.core.api.Assertions.assertThat;

public class LmqConsumerOffsetManagerTest {

    @Spy
    private Broker broker = new Broker(new BrokerConfig(), new NettyServerConfig(),
        new NettyClientConfig(), new MessageStoreConfig());

    @Test
    public void testOffsetManage() {
        LmqConsumerOffsetManager lmqConsumerOffsetManager = new LmqConsumerOffsetManager(broker);
        LmqTopicConfigManager lmqTopicConfigManager = new LmqTopicConfigManager(broker);
        LmqSubscriptionGroupManager lmqSubscriptionGroupManager = new LmqSubscriptionGroupManager(broker);

        String lmqTopicName = "%LMQ%1111";
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(lmqTopicName);
        lmqTopicConfigManager.updateTopicConfig(topicConfig);
        TopicConfig topicConfig1 = lmqTopicConfigManager.selectTopicConfig(lmqTopicName);
        assertThat(topicConfig1.getTopicName()).isEqualTo(topicConfig.getTopicName());

        String lmqGroupName = "%LMQ%GID_test";
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(lmqGroupName);
        lmqSubscriptionGroupManager.updateSubscriptionGroupConfig(subscriptionGroupConfig);
        SubscriptionGroupConfig subscriptionGroupConfig1 = lmqSubscriptionGroupManager.findSubscriptionGroupConfig(
            lmqGroupName);
        assertThat(subscriptionGroupConfig1.getGroupName()).isEqualTo(subscriptionGroupConfig.getGroupName());

        lmqConsumerOffsetManager.commitOffset("127.0.0.1", lmqGroupName, lmqTopicName, 0, 10L);
        Map<Integer, Long> integerLongMap = lmqConsumerOffsetManager.queryOffset(lmqGroupName, lmqTopicName);
        assertThat(integerLongMap.get(0)).isEqualTo(10L);
        long offset = lmqConsumerOffsetManager.queryOffset(lmqGroupName, lmqTopicName, 0);
        assertThat(offset).isEqualTo(10L);

        long offset1 = lmqConsumerOffsetManager.queryOffset(lmqGroupName, lmqTopicName + "test", 0);
        assertThat(offset1).isEqualTo(-1L);
    }

    @Test
    public void testOffsetManage1() {
        LmqConsumerOffsetManager lmqConsumerOffsetManager = new LmqConsumerOffsetManager(broker);

        String lmqTopicName = "%LMQ%1111";

        String lmqGroupName = "%LMQ%GID_test";

        lmqConsumerOffsetManager.commitOffset("127.0.0.1", lmqGroupName, lmqTopicName, 0, 10L);

        lmqTopicName = "%LMQ%1222";

        lmqGroupName = "%LMQ%GID_test222";

        lmqConsumerOffsetManager.commitOffset("127.0.0.1", lmqGroupName, lmqTopicName, 0, 10L);
        lmqConsumerOffsetManager.commitOffset("127.0.0.1","GID_test1", "MqttTest",0, 10L);

        String json = lmqConsumerOffsetManager.encode(true);

        LmqConsumerOffsetManager lmqConsumerOffsetManager1 = new LmqConsumerOffsetManager(broker);

        lmqConsumerOffsetManager1.decode(json);

        assertThat(lmqConsumerOffsetManager1.getOffsetTable().size()).isEqualTo(1);
        assertThat(lmqConsumerOffsetManager1.getLmqOffsetTable().size()).isEqualTo(2);
    }

    @After
    public void destroy() {
        IOUtils.deleteFile(new File(new MessageStoreConfig().getStorePathRootDir()));
    }

}
