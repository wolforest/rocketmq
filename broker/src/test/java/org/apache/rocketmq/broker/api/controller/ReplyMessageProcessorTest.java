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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.client.ClientChannelInfo;
import org.apache.rocketmq.broker.infra.network.Broker2Client;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.domain.topic.TopicValidator;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.store.api.dto.AppendMessageResult;
import org.apache.rocketmq.store.api.dto.AppendMessageStatus;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReplyMessageProcessorTest {
    private ReplyMessageProcessor replyMessageProcessor;
    @Spy
    private Broker broker = new Broker(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private ChannelHandlerContext handlerContext;
    @Mock
    private MessageStore messageStore;
    @Mock
    private Channel channel;

    private String topic = "FooBar";
    private String group = "FooBarGroup";
    private ClientChannelInfo clientInfo;
    @Mock
    private Broker2Client broker2Client;

    @Before
    public void init() throws IllegalAccessException, NoSuchFieldException {
        clientInfo = new ClientChannelInfo(channel, "127.0.0.1", LanguageCode.JAVA, 0);
        broker.setMessageStore(messageStore);
        when(broker.getBroker2Client()).thenReturn(broker2Client);
        //when(messageStore.now()).thenReturn(System.currentTimeMillis());
        Channel mockChannel = mock(Channel.class);
        when(mockChannel.remoteAddress()).thenReturn(new InetSocketAddress(1024));
        when(handlerContext.channel()).thenReturn(mockChannel);
        replyMessageProcessor = new ReplyMessageProcessor(broker);
    }

    @Test
    public void testProcessRequest_Success() throws RemotingCommandException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException {
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        broker.getProducerManager().registerProducer(group, clientInfo);
        final RemotingCommand request = createSendMessageRequestHeaderCommand(RequestCode.SEND_REPLY_MESSAGE);
        when(broker.getBroker2Client().callClient(any(), any(RemotingCommand.class))).thenReturn(createResponse(ResponseCode.SUCCESS, request));
        RemotingCommand responseToReturn = replyMessageProcessor.processRequest(handlerContext, request);
        assertThat(responseToReturn.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(responseToReturn.getOpaque()).isEqualTo(request.getOpaque());
    }

    private RemotingCommand createSendMessageRequestHeaderCommand(int requestCode) {
        SendMessageRequestHeader requestHeader = createSendMessageRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, requestHeader);
        request.setBody(new byte[] {'a'});
        request.makeCustomHeaderToNet();
        return request;
    }

    private SendMessageRequestHeader createSendMessageRequestHeader() {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setProducerGroup(group);
        requestHeader.setTopic(topic);
        requestHeader.setDefaultTopic(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC);
        requestHeader.setDefaultTopicQueueNums(3);
        requestHeader.setQueueId(1);
        requestHeader.setSysFlag(0);
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setFlag(124);
        requestHeader.setReconsumeTimes(0);
        Map<String, String> map = new HashMap<>();
        map.put(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, "127.0.0.1");
        requestHeader.setProperties(MessageDecoder.messageProperties2String(map));
        return requestHeader;
    }

    private RemotingCommand createResponse(int code, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        response.setCode(code);
        response.setOpaque(request.getOpaque());
        return response;
    }
}
