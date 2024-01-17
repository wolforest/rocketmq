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

package org.apache.rocketmq.broker.server.daemon.longpolling;

import io.netty.channel.Channel;
import java.util.HashMap;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.daemon.BrokerNettyServer;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.utils.StringUtils;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.api.filter.DefaultMessageFilter;
import org.apache.rocketmq.store.server.DefaultMessageStore;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PullRequestHoldServiceTest {

    @Mock
    private Broker broker;

    private PullRequestHoldService pullRequestHoldService;

    @Mock
    private PullRequest pullRequest;

    private BrokerConfig brokerConfig = new BrokerConfig();

    @Mock
    private DefaultMessageStore defaultMessageStore;

    @Mock
    private DefaultMessageFilter defaultMessageFilter;

    @Mock
    private RemotingCommand remotingCommand;

    @Mock
    private Channel channel;

    @Mock
    private BrokerNettyServer brokerNettyServer;

    private SubscriptionData subscriptionData;

    private static final String TEST_TOPIC = "TEST_TOPIC";

    private static final int DEFAULT_QUEUE_ID = 0;

    private static final long MAX_OFFSET = 100L;

    @Before
    public void before() {
        when(broker.getBrokerConfig()).thenReturn(brokerConfig);
        when(broker.getBrokerNettyServer()).thenReturn(brokerNettyServer);
        pullRequestHoldService = new PullRequestHoldService(broker);
        subscriptionData = new SubscriptionData(TEST_TOPIC, "*");
        pullRequest = new PullRequest(remotingCommand, channel, 3000, 3000, 0L, subscriptionData, defaultMessageFilter);
        pullRequestHoldService.start();
    }

    @After
    public void after() {
        pullRequestHoldService.shutdown();
    }

    @Test
    public void suspendPullRequestTest() {
        Assertions.assertThatCode(() -> pullRequestHoldService.suspendPullRequest(TEST_TOPIC, DEFAULT_QUEUE_ID, pullRequest)).doesNotThrowAnyException();
    }

    @Test
    public void getServiceNameTest() {
        final String name = pullRequestHoldService.getServiceName();
        assert StringUtils.isNotEmpty(name);
    }

    @Test
    public void checkHoldRequestTest() {
        Assertions.assertThatCode(() -> pullRequestHoldService.checkHoldRequest()).doesNotThrowAnyException();
    }

    @Test
    public void notifyMessageArrivingTest() {
        Assertions.assertThatCode(() -> pullRequestHoldService.notifyMessageArriving(TEST_TOPIC, DEFAULT_QUEUE_ID, MAX_OFFSET)).doesNotThrowAnyException();

        Assertions.assertThatCode(() -> pullRequestHoldService.suspendPullRequest(TEST_TOPIC, DEFAULT_QUEUE_ID, pullRequest)).doesNotThrowAnyException();

        Assertions.assertThatCode(() -> pullRequestHoldService.notifyMessageArriving(TEST_TOPIC, DEFAULT_QUEUE_ID, MAX_OFFSET,
            1L, System.currentTimeMillis(), new byte[10], new HashMap<>())).doesNotThrowAnyException();
    }

    @Test
    public void notifyMasterOnlineTest() {
        Assertions.assertThatCode(() -> pullRequestHoldService.suspendPullRequest(TEST_TOPIC, DEFAULT_QUEUE_ID, pullRequest)).doesNotThrowAnyException();

        Assertions.assertThatCode(() -> pullRequestHoldService.notifyMasterOnline()).doesNotThrowAnyException();
    }

}
