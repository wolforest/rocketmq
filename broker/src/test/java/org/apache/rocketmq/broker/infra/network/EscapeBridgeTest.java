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

package org.apache.rocketmq.broker.infra.network;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.domain.metadata.topic.TopicRouteInfoManager;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.daemon.BrokerMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.api.dto.GetMessageResult;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;
import org.apache.rocketmq.store.infra.mappedfile.DefaultMappedFile;
import org.apache.rocketmq.store.infra.mappedfile.MappedFile;
import org.apache.rocketmq.store.infra.mappedfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EscapeBridgeTest {

    private EscapeBridge escapeBridge;

    @Mock
    private Broker broker;

    @Mock
    private MessageExtBrokerInner messageExtBrokerInner;

    private BrokerConfig brokerConfig;

    @Mock
    private DefaultMessageStore defaultMessageStore;

    private GetMessageResult getMessageResult;

    @Mock
    private DefaultMQProducer defaultMQProducer;

    @Mock
    private BrokerMessageService brokerMessageService;

    private static final String BROKER_NAME = "broker_a";

    private static final String TEST_TOPIC = "TEST_TOPIC";

    private static final int DEFAULT_QUEUE_ID = 0;


    @Before
    public void before() throws Exception {
        brokerConfig = new BrokerConfig();
        getMessageResult = new GetMessageResult();
        brokerConfig.setBrokerName(BROKER_NAME);
        when(broker.getBrokerConfig()).thenReturn(brokerConfig);

        escapeBridge = new EscapeBridge(broker);
        messageExtBrokerInner = new MessageExtBrokerInner();
        when(broker.getMessageStore()).thenReturn(defaultMessageStore);
        when(defaultMessageStore.getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any())).thenReturn(CompletableFuture.completedFuture(getMessageResult));

        TopicRouteInfoManager topicRouteInfoManager = mock(TopicRouteInfoManager.class);
        when(broker.getTopicRouteInfoManager()).thenReturn(topicRouteInfoManager);
        when(topicRouteInfoManager.findBrokerAddressInSubscribe(anyString(), anyLong(), anyBoolean())).thenReturn("");

        ClusterClient clusterClient = mock(ClusterClient.class);
        when(broker.getClusterClient()).thenReturn(clusterClient);
        when(clusterClient.pullMessageFromSpecificBrokerAsync(anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(new PullResult(PullStatus.FOUND, -1, -1, -1, new ArrayList<>())));

        brokerConfig.setEnableSlaveActingMaster(true);
        brokerConfig.setEnableRemoteEscape(true);
        escapeBridge.start();
        defaultMQProducer.start();
    }

    @After
    public void after() {
        escapeBridge.shutdown();
        broker.shutdown();
        defaultMQProducer.shutdown();
    }

    @Test
    public void putMessageTest() {
        messageExtBrokerInner.setTopic(TEST_TOPIC);
        messageExtBrokerInner.setQueueId(DEFAULT_QUEUE_ID);
        messageExtBrokerInner.setBody("Hello World".getBytes(StandardCharsets.UTF_8));
        // masterBroker is null
        final PutMessageResult result1 = escapeBridge.putMessage(messageExtBrokerInner);
        assert result1 != null;
        assert PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL.equals(result1.getPutMessageStatus());

        // masterBroker is not null
        messageExtBrokerInner.setBody("Hello World2".getBytes(StandardCharsets.UTF_8));
        when(broker.peekMasterBroker()).thenReturn(broker);
        Assertions.assertThatCode(() -> escapeBridge.putMessage(messageExtBrokerInner)).doesNotThrowAnyException();

        when(broker.peekMasterBroker()).thenReturn(null);
        final PutMessageResult result3 = escapeBridge.putMessage(messageExtBrokerInner);
        assert result3 != null;
        assert PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL.equals(result3.getPutMessageStatus());
    }

    @Test
    public void asyncPutMessageTest() {

        // masterBroker is null
        Assertions.assertThatCode(() -> escapeBridge.asyncPutMessage(messageExtBrokerInner)).doesNotThrowAnyException();

        // masterBroker is not null
        when(broker.peekMasterBroker()).thenReturn(broker);
        Assertions.assertThatCode(() -> escapeBridge.asyncPutMessage(messageExtBrokerInner)).doesNotThrowAnyException();

        when(broker.peekMasterBroker()).thenReturn(null);
        Assertions.assertThatCode(() -> escapeBridge.asyncPutMessage(messageExtBrokerInner)).doesNotThrowAnyException();
    }

    @Test
    public void putMessageToSpecificQueueTest() {
        // masterBroker is null
        final PutMessageResult result1 = escapeBridge.putMessageToSpecificQueue(messageExtBrokerInner);
        assert result1 != null;
        assert PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL.equals(result1.getPutMessageStatus());

        // masterBroker is not null
        when(broker.peekMasterBroker()).thenReturn(broker);
        Assertions.assertThatCode(() -> escapeBridge.putMessageToSpecificQueue(messageExtBrokerInner)).doesNotThrowAnyException();
    }

    @Test
    public void getMessageTest() {
        when(broker.peekMasterBroker()).thenReturn(broker);
        when(broker.getBrokerMessageService()).thenReturn(brokerMessageService);
        when(broker.getBrokerMessageService().getMessageStoreByBrokerName(any())).thenReturn(defaultMessageStore);
        Assertions.assertThatCode(() -> escapeBridge.putMessage(messageExtBrokerInner)).doesNotThrowAnyException();

        Assertions.assertThatCode(() -> escapeBridge.getMessage(TEST_TOPIC, 0, DEFAULT_QUEUE_ID, BROKER_NAME, false)).doesNotThrowAnyException();
    }

    @Test
    public void getMessageAsyncTest() {
        when(broker.peekMasterBroker()).thenReturn(broker);
        when(broker.getBrokerMessageService()).thenReturn(brokerMessageService);
        when(broker.getBrokerMessageService().getMessageStoreByBrokerName(any())).thenReturn(defaultMessageStore);
        Assertions.assertThatCode(() -> escapeBridge.putMessage(messageExtBrokerInner)).doesNotThrowAnyException();

        Assertions.assertThatCode(() -> escapeBridge.getMessageAsync(TEST_TOPIC, 0, DEFAULT_QUEUE_ID, BROKER_NAME, false)).doesNotThrowAnyException();
    }

    @Test
    public void getMessageFromRemoteTest() {
        Assertions.assertThatCode(() -> escapeBridge.getMessageFromRemote(TEST_TOPIC, 1, DEFAULT_QUEUE_ID, BROKER_NAME)).doesNotThrowAnyException();
    }

    @Test
    public void getMessageFromRemoteAsyncTest() {
        Assertions.assertThatCode(() -> escapeBridge.getMessageFromRemoteAsync(TEST_TOPIC, 1, DEFAULT_QUEUE_ID, BROKER_NAME)).doesNotThrowAnyException();
    }

    @Test
    public void decodeMsgListTest() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(10);
        MappedFile mappedFile = new DefaultMappedFile();
        SelectMappedBufferResult result = new SelectMappedBufferResult(0, byteBuffer, 10, mappedFile);

        getMessageResult.addMessage(result);
        Assertions.assertThatCode(() -> escapeBridge.decodeMsgList(getMessageResult, false)).doesNotThrowAnyException();
    }

}
