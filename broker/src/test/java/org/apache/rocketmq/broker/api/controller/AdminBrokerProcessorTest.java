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
package org.apache.rocketmq.broker.api.controller;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.domain.consumer.ConsumerGroupInfo;
import org.apache.rocketmq.broker.domain.consumer.ConsumerManager;
import org.apache.rocketmq.broker.domain.queue.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.server.daemon.schedule.ScheduleMessageService;
import org.apache.rocketmq.broker.server.bootstrap.BrokerServiceManager;
import org.apache.rocketmq.broker.domain.metadata.subscription.RocksDBSubscriptionGroupManager;
import org.apache.rocketmq.broker.domain.metadata.topic.RocksDBTopicConfigManager;
import org.apache.rocketmq.broker.domain.metadata.topic.TopicConfigManager;
import org.apache.rocketmq.common.lang.BoundaryType;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.topic.KeyBuilder;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.domain.topic.TopicFilterType;
import org.apache.rocketmq.common.domain.topic.TopicQueueId;
import org.apache.rocketmq.common.domain.constant.PermName;
import org.apache.rocketmq.common.domain.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.domain.message.MessageAccessor;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.topic.TopicValidator;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.utils.SystemUtils;
import org.apache.rocketmq.common.utils.BeanUtils;
import org.apache.rocketmq.auth.authentication.enums.UserType;
import org.apache.rocketmq.auth.authentication.manager.AuthenticationMetadataManager;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.enums.Decision;
import org.apache.rocketmq.auth.authorization.manager.AuthorizationMetadataManager;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.authorization.model.Environment;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.common.domain.action.Action;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.AclInfo;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UserInfo;
import org.apache.rocketmq.remoting.protocol.header.CreateAclRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateUserRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteAclRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteUserRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetAclRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetAllTopicConfigResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetUserRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ListAclsRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ListUsersRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResumeCheckHalfMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateAclRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateUserRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.infra.mappedfile.DefaultMappedFile;
import org.apache.rocketmq.store.infra.mappedfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.api.broker.stats.BrokerStats;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AdminBrokerProcessorTest {

    private AdminBrokerProcessor adminBrokerProcessor;

    @Mock
    private ChannelHandlerContext handlerContext;

    @Mock
    private Channel channel;

    @Spy
    private Broker
        broker = new Broker(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(),
            new MessageStoreConfig(), null);

    @Mock
    private MessageStore messageStore;

    @Mock
    private SendMessageProcessor sendMessageProcessor;

    @Mock
    private ConcurrentMap<TopicQueueId, LongAdder> inFlyWritingCounterMap;

    private Set<String> systemTopicSet;
    private String topic;

    @Mock
    private SocketAddress socketAddress;
    @Mock
    private BrokerStats brokerStats;
    @Mock
    private TopicConfigManager topicConfigManager;
    @Mock
    private ConsumerManager consumerManager;
    @Mock
    private ConsumerOffsetManager consumerOffsetManager;
    @Mock
    private DefaultMessageStore defaultMessageStore;
    @Mock
    private ScheduleMessageService scheduleMessageService;
    @Mock
    private AuthenticationMetadataManager authenticationMetadataManager;
    @Mock
    private AuthorizationMetadataManager authorizationMetadataManager;
    @Mock
    private BrokerServiceManager brokerServiceManager;

    @Before
    public void init() throws Exception {
        broker.setMessageStore(messageStore);
        broker.getBrokerAuthService().setAuthenticationMetadataManager(authenticationMetadataManager);
        broker.getBrokerAuthService().setAuthorizationMetadataManager(authorizationMetadataManager);

        //doReturn(sendMessageProcessor).when(brokerController).getSendMessageProcessor();

        adminBrokerProcessor = new AdminBrokerProcessor(broker);

        systemTopicSet = Sets.newHashSet(
            TopicValidator.RMQ_SYS_SELF_TEST_TOPIC,
            TopicValidator.RMQ_SYS_BENCHMARK_TOPIC,
            TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
            TopicValidator.RMQ_SYS_OFFSET_MOVED_EVENT,
            TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC,
            this.broker.getBrokerConfig().getBrokerClusterName(),
            this.broker.getBrokerConfig().getBrokerClusterName() + "_" + MQConstants.REPLY_TOPIC_POSTFIX);
        if (this.broker.getBrokerConfig().isTraceTopicEnable()) {
            systemTopicSet.add(this.broker.getBrokerConfig().getMsgTraceTopicName());
        }
        when(handlerContext.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 12345));

        topic = "FooBar" + System.nanoTime();

        broker.getTopicConfigManager().getTopicConfigTable().put(topic, new TopicConfig(topic));
        broker.getMessageStoreConfig().setTimerWheelEnable(false);
    }

    @After
    public void destroy() {
        if (notToBeExecuted()) {
            return;
        }
        if (broker.getSubscriptionGroupManager() != null) {
            broker.getSubscriptionGroupManager().stop();
        }
        if (broker.getTopicConfigManager() != null) {
            broker.getTopicConfigManager().stop();
        }
        if (broker.getConsumerOffsetManager() != null) {
            broker.getConsumerOffsetManager().stop();
        }
    }

    private void initRocksdbTopicManager() {
        if (notToBeExecuted()) {
            return;
        }
        RocksDBTopicConfigManager rocksDBTopicConfigManager = new RocksDBTopicConfigManager(broker);
        broker.setTopicConfigManager(rocksDBTopicConfigManager);
        rocksDBTopicConfigManager.load();
    }

    private void initRocksdbSubscriptionManager() {
        if (notToBeExecuted()) {
            return;
        }
        RocksDBSubscriptionGroupManager rocksDBSubscriptionGroupManager = new RocksDBSubscriptionGroupManager(broker);
        broker.setSubscriptionGroupManager(rocksDBSubscriptionGroupManager);
        rocksDBSubscriptionGroupManager.load();
    }

    @Test
    public void testProcessRequest_success() throws RemotingCommandException, UnknownHostException {
        RemotingCommand request = createUpdateBrokerConfigCommand();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testProcessRequest_fail() throws RemotingCommandException, UnknownHostException {
        RemotingCommand request = createResumeCheckHalfMessageCommand();
        when(messageStore.selectOneMessageByOffset(any(Long.class))).thenReturn(createSelectMappedBufferResult());
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
    }

    @Test
    public void testUpdateAndCreateTopicInRocksdb() throws Exception {
        if (notToBeExecuted()) {
            return;
        }
        initRocksdbTopicManager();
        testUpdateAndCreateTopic();
    }

    @Test
    public void testUpdateAndCreateTopic() throws Exception {
        //test system topic
        for (String topic : systemTopicSet) {
            RemotingCommand request = buildCreateTopicRequest(topic);
            RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
            assertThat(response.getRemark()).isEqualTo("The topic[" + topic + "] is conflict with system topic.");
        }

        //test validate error topic
        String topic = "";
        RemotingCommand request = buildCreateTopicRequest(topic);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);

        topic = "TEST_CREATE_TOPIC";
        request = buildCreateTopicRequest(topic);
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testDeleteTopicInRocksdb() throws Exception {
        if (notToBeExecuted()) {
            return;
        }
        initRocksdbTopicManager();
        testDeleteTopic();
    }

    @Test
    public void testDeleteTopic() throws Exception {
        //test system topic
        for (String topic : systemTopicSet) {
            RemotingCommand request = buildDeleteTopicRequest(topic);
            RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
            assertThat(response.getRemark()).isEqualTo("The topic[" + topic + "] is conflict with system topic.");
        }

        String topic = "TEST_DELETE_TOPIC";
        RemotingCommand request = buildDeleteTopicRequest(topic);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testDeleteWithPopRetryTopic() throws Exception {
        String topic = "topicA";
        String anotherTopic = "another_topicA";
        BrokerConfig brokerConfig = new BrokerConfig();

        topicConfigManager = mock(TopicConfigManager.class);
        when(broker.getTopicConfigManager()).thenReturn(topicConfigManager);
        final ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
        topicConfigTable.put(topic, new TopicConfig());
        topicConfigTable.put(KeyBuilder.buildPopRetryTopic(topic, "cid1", brokerConfig.isEnableRetryTopicV2()), new TopicConfig());

        topicConfigTable.put(anotherTopic, new TopicConfig());
        topicConfigTable.put(KeyBuilder.buildPopRetryTopic(anotherTopic, "cid2", brokerConfig.isEnableRetryTopicV2()), new TopicConfig());
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicConfigTable);
        when(topicConfigManager.selectTopicConfig(anyString())).thenAnswer(invocation -> {
            final String selectTopic = invocation.getArgument(0);
            return topicConfigManager.getTopicConfigTable().get(selectTopic);
        });

        when(broker.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.whichGroupByTopic(topic)).thenReturn(Sets.newHashSet("cid1"));

        RemotingCommand request = buildDeleteTopicRequest(topic);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        verify(topicConfigManager).deleteTopicConfig(topic);
        verify(topicConfigManager).deleteTopicConfig(KeyBuilder.buildPopRetryTopic(topic, "cid1", brokerConfig.isEnableRetryTopicV2()));
        verify(messageStore, times(2)).deleteTopics(anySet());
    }

    @Test
    public void testGetAllTopicConfigInRocksdb() throws Exception {
        if (notToBeExecuted()) {
            return;
        }
        initRocksdbTopicManager();
        testGetAllTopicConfig();
    }

    @Test
    public void testGetAllTopicConfig() throws Exception {
        GetAllTopicConfigResponseHeader getAllTopicConfigResponseHeader = new GetAllTopicConfigResponseHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, getAllTopicConfigResponseHeader);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testUpdateBrokerConfig() throws Exception {
        handlerContext = mock(ChannelHandlerContext.class);
        channel = mock(Channel.class);
        when(handlerContext.channel()).thenReturn(channel);
        socketAddress = mock(SocketAddress.class);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);
        Map<String, String> bodyMap = new HashMap<>();
        bodyMap.put("key", "value");
        request.setBody(bodyMap.toString().getBytes());
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetBrokerConfig() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONFIG, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testProcessRequest_UpdateConfigPath() throws RemotingCommandException {
        final RemotingCommand updateConfigRequest = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);
        Properties properties = new Properties();

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);

        // Update allowed value
        properties.setProperty("allAckInSyncStateSet", "true");
        updateConfigRequest.setBody(BeanUtils.properties2String(properties).getBytes(StandardCharsets.UTF_8));

        RemotingCommand response = adminBrokerProcessor.processRequest(ctx, updateConfigRequest);

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        //update disallowed value
        properties.clear();
        properties.setProperty("brokerConfigPath", "test/path");
        updateConfigRequest.setBody(BeanUtils.properties2String(properties).getBytes(StandardCharsets.UTF_8));

        response = adminBrokerProcessor.processRequest(ctx, updateConfigRequest);

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
        assertThat(response.getRemark()).contains("Can not update config in black list.");

        //update disallowed value
        properties.clear();
        properties.setProperty("configBlackList", "test;path");
        updateConfigRequest.setBody(BeanUtils.properties2String(properties).getBytes(StandardCharsets.UTF_8));

        response = adminBrokerProcessor.processRequest(ctx, updateConfigRequest);

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
        assertThat(response.getRemark()).contains("Can not update config in black list.");
    }

    @Test
    public void testSearchOffsetByTimestamp() throws Exception {
        messageStore = mock(MessageStore.class);
        when(messageStore.getOffsetInQueueByTime(anyString(), anyInt(), anyLong(), any(BoundaryType.class))).thenReturn(Long.MIN_VALUE);
        when(broker.getMessageStore()).thenReturn(messageStore);
        SearchOffsetRequestHeader searchOffsetRequestHeader = new SearchOffsetRequestHeader();
        searchOffsetRequestHeader.setTopic("topic");
        searchOffsetRequestHeader.setQueueId(0);
        searchOffsetRequestHeader.setTimestamp(System.currentTimeMillis());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, searchOffsetRequestHeader);
        request.addExtField("topic", "topic");
        request.addExtField("queueId", "0");
        request.addExtField("timestamp", System.currentTimeMillis() + "");
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetMaxOffset() throws Exception {
        messageStore = mock(MessageStore.class);
        when(messageStore.getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(Long.MIN_VALUE);
        when(broker.getMessageStore()).thenReturn(messageStore);
        GetMaxOffsetRequestHeader getMaxOffsetRequestHeader = new GetMaxOffsetRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, getMaxOffsetRequestHeader);
        request.addExtField("topic", "topic");
        request.addExtField("queueId", "0");
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetMinOffset() throws Exception {
        messageStore = mock(MessageStore.class);
        when(messageStore.getMinOffsetInQueue(anyString(), anyInt())).thenReturn(Long.MIN_VALUE);
        when(broker.getMessageStore()).thenReturn(messageStore);
        GetMinOffsetRequestHeader getMinOffsetRequestHeader = new GetMinOffsetRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MIN_OFFSET, getMinOffsetRequestHeader);
        request.addExtField("topic", "topic");
        request.addExtField("queueId", "0");
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetEarliestMsgStoretime() throws Exception {
        messageStore = mock(MessageStore.class);
        when(broker.getMessageStore()).thenReturn(messageStore);
        GetEarliestMsgStoretimeRequestHeader getEarliestMsgStoretimeRequestHeader = new GetEarliestMsgStoretimeRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_EARLIEST_MSG_STORETIME, getEarliestMsgStoretimeRequestHeader);
        request.addExtField("topic", "topic");
        request.addExtField("queueId", "0");
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetBrokerRuntimeInfo() throws Exception {
        brokerStats = mock(BrokerStats.class);
        when(broker.getBrokerServiceManager()).thenReturn(brokerServiceManager);
        when(broker.getBrokerServiceManager().getBrokerStats()).thenReturn(brokerStats);
        when(brokerStats.getMsgPutTotalYesterdayMorning()).thenReturn(Long.MIN_VALUE);
        when(brokerStats.getMsgPutTotalTodayMorning()).thenReturn(Long.MIN_VALUE);
        when(brokerStats.getMsgPutTotalTodayNow()).thenReturn(Long.MIN_VALUE);
        when(brokerStats.getMsgGetTotalTodayMorning()).thenReturn(Long.MIN_VALUE);
        when(brokerStats.getMsgGetTotalTodayNow()).thenReturn(Long.MIN_VALUE);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_RUNTIME_INFO, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testLockBatchMQ() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);
        LockBatchRequestBody lockBatchRequestBody = new LockBatchRequestBody();
        lockBatchRequestBody.setClientId("1111");
        lockBatchRequestBody.setConsumerGroup("group");
        request.setBody(JSON.toJSON(lockBatchRequestBody).toString().getBytes());
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testUnlockBatchMQ() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, null);
        UnlockBatchRequestBody unlockBatchRequestBody = new UnlockBatchRequestBody();
        unlockBatchRequestBody.setClientId("11111");
        unlockBatchRequestBody.setConsumerGroup("group");
        request.setBody(JSON.toJSON(unlockBatchRequestBody).toString().getBytes());
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testUpdateAndCreateSubscriptionGroupInRocksdb() throws Exception {
        initRocksdbSubscriptionManager();
        testUpdateAndCreateSubscriptionGroup();
    }

    @Test
    public void testUpdateAndCreateSubscriptionGroup() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, null);
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setBrokerId(1);
        subscriptionGroupConfig.setGroupName("groupId");
        subscriptionGroupConfig.setConsumeEnable(Boolean.TRUE);
        subscriptionGroupConfig.setConsumeBroadcastEnable(Boolean.TRUE);
        subscriptionGroupConfig.setRetryMaxTimes(111);
        subscriptionGroupConfig.setConsumeFromMinEnable(Boolean.TRUE);
        request.setBody(JSON.toJSON(subscriptionGroupConfig).toString().getBytes());
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetAllSubscriptionGroupInRocksdb() throws Exception {
        initRocksdbSubscriptionManager();
        testGetAllSubscriptionGroup();
    }

    @Test
    public void testGetAllSubscriptionGroup() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testDeleteSubscriptionGroupInRocksdb() throws Exception {
        initRocksdbSubscriptionManager();
        testDeleteSubscriptionGroup();
    }

    @Test
    public void testDeleteSubscriptionGroup() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_SUBSCRIPTIONGROUP, null);
        request.addExtField("groupName", "GID-Group-Name");
        request.addExtField("removeOffset", "true");
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetTopicStatsInfo() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_STATS_INFO, null);
        request.addExtField("topic", "topicTest");
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);
        topicConfigManager = mock(TopicConfigManager.class);
        when(broker.getTopicConfigManager()).thenReturn(topicConfigManager);
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName("topicTest");
        when(topicConfigManager.selectTopicConfig(anyString())).thenReturn(topicConfig);
        RemotingCommand responseSuccess = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(responseSuccess.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetConsumerConnectionList() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_CONNECTION_LIST, null);
        request.addExtField("consumerGroup", "GID-group-test");
        consumerManager = mock(ConsumerManager.class);
        when(broker.getConsumerManager()).thenReturn(consumerManager);
        ConsumerGroupInfo consumerGroupInfo = new ConsumerGroupInfo("GID-group-test", ConsumeType.CONSUME_ACTIVELY, MessageModel.CLUSTERING, ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        when(consumerManager.getConsumerGroupInfo(anyString())).thenReturn(consumerGroupInfo);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetProducerConnectionList() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_PRODUCER_CONNECTION_LIST, null);
        request.addExtField("producerGroup", "ProducerGroupId");
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
    }

    @Test
    public void testGetAllProducerInfo() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_PRODUCER_INFO, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetConsumeStats() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUME_STATS, null);
        request.addExtField("topic", "topicTest");
        request.addExtField("consumerGroup", "GID-test");
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetAllConsumerOffset() throws RemotingCommandException {
        consumerOffsetManager = mock(ConsumerOffsetManager.class);
        when(broker.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        ConsumerOffsetManager consumerOffset = new ConsumerOffsetManager();
        when(consumerOffsetManager.encode()).thenReturn(JSON.toJSONString(consumerOffset, false));
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_CONSUMER_OFFSET, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetAllDelayOffset() throws Exception {
        defaultMessageStore = mock(DefaultMessageStore.class);
        scheduleMessageService = mock(ScheduleMessageService.class);
//        when(brokerController.getMessageStore()).thenReturn(defaultMessageStore);
        when(broker.getScheduleMessageService()).thenReturn(scheduleMessageService);
        when(scheduleMessageService.encode()).thenReturn("content");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_DELAY_OFFSET, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetTopicConfigInRocksdb() throws Exception {
        if (notToBeExecuted()) {
            return;
        }
        initRocksdbTopicManager();
        testGetTopicConfig();
    }

    @Test
    public void testGetTopicConfig() throws Exception {
        String topic = "foobar";

        broker.getTopicConfigManager().getTopicConfigTable().put(topic, new TopicConfig(topic));

        {
            GetTopicConfigRequestHeader requestHeader = new GetTopicConfigRequestHeader();
            requestHeader.setTopic(topic);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_CONFIG, requestHeader);
            request.makeCustomHeaderToNet();
            RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            assertThat(response.getBody()).isNotEmpty();
        }
        {
            GetTopicConfigRequestHeader requestHeader = new GetTopicConfigRequestHeader();
            requestHeader.setTopic("aaaaaaa");
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_CONFIG, requestHeader);
            request.makeCustomHeaderToNet();
            RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);
            assertThat(response.getRemark()).contains("No topic in this broker.");
        }
    }

    @Test
    public void testCreateUser() throws RemotingCommandException {
        when(authenticationMetadataManager.createUser(any(User.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        CreateUserRequestHeader createUserRequestHeader = new CreateUserRequestHeader();
        createUserRequestHeader.setUsername("abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_CREATE_USER, createUserRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        UserInfo userInfo = UserInfo.of("abc", "123", UserType.NORMAL.getName());
        request.setBody(JSON.toJSONBytes(userInfo));
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        when(authenticationMetadataManager.isSuperUser(eq("rocketmq"))).thenReturn(CompletableFuture.completedFuture(true));
        createUserRequestHeader = new CreateUserRequestHeader();
        createUserRequestHeader.setUsername("super");
        request = RemotingCommand.createRequestCommand(RequestCode.AUTH_CREATE_USER, createUserRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        userInfo = UserInfo.of("super", "123", UserType.SUPER.getName());
        request.setBody(JSON.toJSONBytes(userInfo));
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        when(authenticationMetadataManager.isSuperUser(eq("rocketmq"))).thenReturn(CompletableFuture.completedFuture(false));
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
    }

    @Test
    public void testUpdateUser() throws RemotingCommandException {
        when(authenticationMetadataManager.updateUser(any(User.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(authenticationMetadataManager.getUser(eq("abc"))).thenReturn(CompletableFuture.completedFuture(User.of("abc", "123", UserType.NORMAL)));
        when(authenticationMetadataManager.getUser(eq("super"))).thenReturn(CompletableFuture.completedFuture(User.of("super", "123", UserType.SUPER)));

        UpdateUserRequestHeader updateUserRequestHeader = new UpdateUserRequestHeader();
        updateUserRequestHeader.setUsername("abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_UPDATE_USER, updateUserRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        UserInfo userInfo = UserInfo.of("abc", "123", UserType.NORMAL.getName());
        request.setBody(JSON.toJSONBytes(userInfo));
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        when(authenticationMetadataManager.isSuperUser(eq("rocketmq"))).thenReturn(CompletableFuture.completedFuture(true));
        updateUserRequestHeader = new UpdateUserRequestHeader();
        updateUserRequestHeader.setUsername("super");
        request = RemotingCommand.createRequestCommand(RequestCode.AUTH_UPDATE_USER, updateUserRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        userInfo = UserInfo.of("super", "123", UserType.SUPER.getName());
        request.setBody(JSON.toJSONBytes(userInfo));
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        when(authenticationMetadataManager.isSuperUser(eq("rocketmq"))).thenReturn(CompletableFuture.completedFuture(false));
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
    }

    @Test
    public void testDeleteUser() throws RemotingCommandException {
        when(authenticationMetadataManager.deleteUser(any(String.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(authenticationMetadataManager.getUser(eq("abc"))).thenReturn(CompletableFuture.completedFuture(User.of("abc", "123", UserType.NORMAL)));
        when(authenticationMetadataManager.getUser(eq("super"))).thenReturn(CompletableFuture.completedFuture(User.of("super", "123", UserType.SUPER)));

        DeleteUserRequestHeader deleteUserRequestHeader = new DeleteUserRequestHeader();
        deleteUserRequestHeader.setUsername("abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_DELETE_USER, deleteUserRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        when(authenticationMetadataManager.isSuperUser(eq("rocketmq"))).thenReturn(CompletableFuture.completedFuture(true));
        deleteUserRequestHeader = new DeleteUserRequestHeader();
        deleteUserRequestHeader.setUsername("super");
        request = RemotingCommand.createRequestCommand(RequestCode.AUTH_DELETE_USER, deleteUserRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        when(authenticationMetadataManager.isSuperUser(eq("rocketmq"))).thenReturn(CompletableFuture.completedFuture(false));
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
    }

    @Test
    public void testGetUser() throws RemotingCommandException {
        when(authenticationMetadataManager.getUser(eq("abc"))).thenReturn(CompletableFuture.completedFuture(User.of("abc", "123", UserType.NORMAL)));

        GetUserRequestHeader getUserRequestHeader = new GetUserRequestHeader();
        getUserRequestHeader.setUsername("abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_GET_USER, getUserRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        UserInfo userInfo = JSON.parseObject(new String(response.getBody()), UserInfo.class);
        assertThat(userInfo.getUsername()).isEqualTo("abc");
        assertThat(userInfo.getPassword()).isEqualTo("123");
        assertThat(userInfo.getUserType()).isEqualTo("Normal");
    }

    @Test
    public void testListUser() throws RemotingCommandException {
        when(authenticationMetadataManager.listUser(eq("abc"))).thenReturn(CompletableFuture.completedFuture(Arrays.asList(User.of("abc", "123", UserType.NORMAL))));

        ListUsersRequestHeader listUserRequestHeader = new ListUsersRequestHeader();
        listUserRequestHeader.setFilter("abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_LIST_USER, listUserRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        List<UserInfo> userInfo = JSON.parseArray(new String(response.getBody()), UserInfo.class);
        assertThat(userInfo.get(0).getUsername()).isEqualTo("abc");
        assertThat(userInfo.get(0).getPassword()).isEqualTo("123");
        assertThat(userInfo.get(0).getUserType()).isEqualTo("Normal");
    }

    @Test
    public void testCreateAcl() throws RemotingCommandException {
        when(authorizationMetadataManager.createAcl(any(Acl.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        CreateAclRequestHeader createAclRequestHeader = new CreateAclRequestHeader();
        createAclRequestHeader.setSubject("User:abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_CREATE_ACL, createAclRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        AclInfo aclInfo = AclInfo.of("User:abc", Arrays.asList("Topic:*"), Arrays.asList("PUB"), Arrays.asList("192.168.0.1"), "Grant");
        request.setBody(JSON.toJSONBytes(aclInfo));
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

    }

    @Test
    public void testUpdateAcl() throws RemotingCommandException {
        when(authorizationMetadataManager.updateAcl(any(Acl.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        UpdateAclRequestHeader updateAclRequestHeader = new UpdateAclRequestHeader();
        updateAclRequestHeader.setSubject("User:abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_UPDATE_ACL, updateAclRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        AclInfo aclInfo = AclInfo.of("User:abc", Arrays.asList("Topic:*"), Arrays.asList("PUB"), Arrays.asList("192.168.0.1"), "Grant");
        request.setBody(JSON.toJSONBytes(aclInfo));
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

    }

    @Test
    public void testDeleteAcl() throws RemotingCommandException {
        when(authorizationMetadataManager.deleteAcl(any(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        DeleteAclRequestHeader deleteAclRequestHeader = new DeleteAclRequestHeader();
        deleteAclRequestHeader.setSubject("User:abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_DELETE_ACL, deleteAclRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetAcl() throws RemotingCommandException {
        Acl aclInfo = Acl.of(User.of("abc"), Arrays.asList(Resource.of("Topic:*")), Arrays.asList(Action.PUB), Environment.of("192.168.0.1"), Decision.ALLOW);
        when(authorizationMetadataManager.getAcl(any(Subject.class))).thenReturn(CompletableFuture.completedFuture(aclInfo));

        GetAclRequestHeader getAclRequestHeader = new GetAclRequestHeader();
        getAclRequestHeader.setSubject("User:abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_GET_ACL, getAclRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        AclInfo aclInfoData = JSON.parseObject(new String(response.getBody()), AclInfo.class);
        assertThat(aclInfoData.getSubject()).isEqualTo("User:abc");
        assertThat(aclInfoData.getPolicies().get(0).getEntries().get(0).getResource()).isEqualTo("Topic:*");
        assertThat(aclInfoData.getPolicies().get(0).getEntries().get(0).getActions()).containsAll(Arrays.asList(Action.PUB.getName()));
        assertThat(aclInfoData.getPolicies().get(0).getEntries().get(0).getSourceIps()).containsAll(Arrays.asList("192.168.0.1"));
        assertThat(aclInfoData.getPolicies().get(0).getEntries().get(0).getDecision()).isEqualTo("Allow");
    }

    @Test
    public void testListAcl() throws RemotingCommandException {
        Acl aclInfo = Acl.of(User.of("abc"), Arrays.asList(Resource.of("Topic:*")), Arrays.asList(Action.PUB), Environment.of("192.168.0.1"), Decision.ALLOW);
        when(authorizationMetadataManager.listAcl(any(), any())).thenReturn(CompletableFuture.completedFuture(Arrays.asList(aclInfo)));

        ListAclsRequestHeader listAclRequestHeader = new ListAclsRequestHeader();
        listAclRequestHeader.setSubjectFilter("User:abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_LIST_ACL, listAclRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        List<AclInfo> aclInfoData = JSON.parseArray(new String(response.getBody()), AclInfo.class);
        assertThat(aclInfoData.get(0).getSubject()).isEqualTo("User:abc");
        assertThat(aclInfoData.get(0).getPolicies().get(0).getEntries().get(0).getResource()).isEqualTo("Topic:*");
        assertThat(aclInfoData.get(0).getPolicies().get(0).getEntries().get(0).getActions()).containsAll(Arrays.asList(Action.PUB.getName()));
        assertThat(aclInfoData.get(0).getPolicies().get(0).getEntries().get(0).getSourceIps()).containsAll(Arrays.asList("192.168.0.1"));
        assertThat(aclInfoData.get(0).getPolicies().get(0).getEntries().get(0).getDecision()).isEqualTo("Allow");
    }

    private RemotingCommand buildCreateTopicRequest(String topic) {
        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setTopicFilterType(TopicFilterType.SINGLE_TAG.name());
        requestHeader.setReadQueueNums(8);
        requestHeader.setWriteQueueNums(8);
        requestHeader.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }

    private RemotingCommand buildDeleteTopicRequest(String topic) {
        DeleteTopicRequestHeader requestHeader = new DeleteTopicRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_BROKER, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }

    private MessageExt createDefaultMessageExt() {
        MessageExt messageExt = new MessageExt();
        messageExt.setMsgId("12345678");
        messageExt.setQueueId(0);
        messageExt.setCommitLogOffset(123456789L);
        messageExt.setQueueOffset(1234);
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_REAL_QUEUE_ID, "0");
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_REAL_TOPIC, "testTopic");
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, "15");
        return messageExt;
    }

    private SelectMappedBufferResult createSelectMappedBufferResult() {
        SelectMappedBufferResult result = new SelectMappedBufferResult(0, ByteBuffer.allocate(1024), 0, new DefaultMappedFile());
        return result;
    }

    private ResumeCheckHalfMessageRequestHeader createResumeCheckHalfMessageRequestHeader() {
        ResumeCheckHalfMessageRequestHeader header = new ResumeCheckHalfMessageRequestHeader();
        header.setTopic("topic");
        header.setMsgId("C0A803CA00002A9F0000000000031367");
        return header;
    }

    private RemotingCommand createResumeCheckHalfMessageCommand() {
        ResumeCheckHalfMessageRequestHeader header = createResumeCheckHalfMessageRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.RESUME_CHECK_HALF_MESSAGE, header);
        request.makeCustomHeaderToNet();
        return request;
    }

    private RemotingCommand createUpdateBrokerConfigCommand() {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);
        request.makeCustomHeaderToNet();
        return request;
    }

    private boolean notToBeExecuted() {
        return SystemUtils.isMac();
    }
}
