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

package org.apache.rocketmq.proxy.remoting.activity;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.server.connection.ClientChannelInfo;
import org.apache.rocketmq.broker.domain.consumer.ConsumerGroupEvent;
import org.apache.rocketmq.broker.domain.consumer.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.domain.producer.ProducerChangeListener;
import org.apache.rocketmq.broker.domain.producer.ProducerGroupEvent;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.channel.RemotingChannel;
import org.apache.rocketmq.proxy.remoting.channel.RemotingChannelManager;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AttributeKeys;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ProducerData;

import java.util.Set;

public class ClientManagerActivity extends AbstractRemotingActivity {

    private final RemotingChannelManager remotingChannelManager;

    public ClientManagerActivity(RequestPipeline requestPipeline, MessagingProcessor messagingProcessor,
        RemotingChannelManager manager) {
        super(requestPipeline, messagingProcessor);
        this.remotingChannelManager = manager;
        this.init();
    }

    protected void init() {
        this.messagingProcessor.registerConsumerListener(new ConsumerIdsChangeListenerImpl());
        this.messagingProcessor.registerProducerListener(new ProducerChangeListenerImpl());
    }

    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        switch (request.getCode()) {
            case RequestCode.HEART_BEAT:
                return this.heartBeat(ctx, request, context);
            case RequestCode.UNREGISTER_CLIENT:
                return this.unregisterClient(ctx, request, context);
            case RequestCode.CHECK_CLIENT_CONFIG:
                return this.checkClientConfig(ctx, request, context);
            default:
                break;
        }
        return null;
    }

    protected RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context) {
        HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);

        registerProducer(ctx, request, context, heartbeatData);
        registerConsumer(ctx, request, context, heartbeatData);

        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark("");
        return response;
    }

    private void registerProducer(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context, HeartbeatData heartbeatData) {
        for (ProducerData data : heartbeatData.getProducerDataSet()) {
            ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                this.remotingChannelManager.createProducerChannel(context, ctx.channel(), data.getGroupName(), heartbeatData.getClientID()),
                heartbeatData.getClientID(), request.getLanguage(),
                request.getVersion());
            setClientPropertiesToChannelAttr(clientChannelInfo);
            messagingProcessor.registerProducer(context, data.getGroupName(), clientChannelInfo);
        }
    }

    private void registerConsumer(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context, HeartbeatData heartbeatData) {
        for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
            ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                this.remotingChannelManager.createConsumerChannel(context, ctx.channel(), data.getGroupName(), heartbeatData.getClientID(), data.getSubscriptionDataSet()),
                heartbeatData.getClientID(), request.getLanguage(),
                request.getVersion());
            setClientPropertiesToChannelAttr(clientChannelInfo);
            messagingProcessor.registerConsumer(context, data.getGroupName(), clientChannelInfo, data.getConsumeType(),
                data.getMessageModel(), data.getConsumeFromWhere(), data.getSubscriptionDataSet(), true);
        }
    }

    private void setClientPropertiesToChannelAttr(final ClientChannelInfo clientChannelInfo) {
        Channel channel = clientChannelInfo.getChannel();
        if (!(channel instanceof RemotingChannel)) {
            return;
        }

        RemotingChannel remotingChannel = (RemotingChannel) channel;
        Channel parent = remotingChannel.parent();
        RemotingHelper.setPropertyToAttr(parent, AttributeKeys.CLIENT_ID_KEY, clientChannelInfo.getClientId());
        RemotingHelper.setPropertyToAttr(parent, AttributeKeys.LANGUAGE_CODE_KEY, clientChannelInfo.getLanguage());
        RemotingHelper.setPropertyToAttr(parent, AttributeKeys.VERSION_KEY, clientChannelInfo.getVersion());
    }

    protected RemotingCommand unregisterClient(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context) throws RemotingCommandException {
        UnregisterClientRequestHeader requestHeader = (UnregisterClientRequestHeader) request.decodeCommandCustomHeader(UnregisterClientRequestHeader.class);

        unRegisterProducer(ctx, request, context, requestHeader);
        unRegisterConsumer(ctx, request, context, requestHeader);

        RemotingCommand response = RemotingCommand.createResponseCommand(UnregisterClientResponseHeader.class);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark("");
        return response;
    }

    private void unRegisterProducer(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context, UnregisterClientRequestHeader requestHeader) {
        final String producerGroup = requestHeader.getProducerGroup();
        if (producerGroup != null) {
            RemotingChannel channel = this.remotingChannelManager.removeProducerChannel(context, producerGroup, ctx.channel());
            if (channel != null) {
                ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                    channel,
                    requestHeader.getClientID(),
                    request.getLanguage(),
                    request.getVersion());
                this.messagingProcessor.unRegisterProducer(context, producerGroup, clientChannelInfo);
            } else {
                log.warn("unregister producer failed, channel not exist, may has been removed, producerGroup={}, channel={}", producerGroup, ctx.channel());
            }
        }
        final String consumerGroup = requestHeader.getConsumerGroup();
        if (consumerGroup != null) {
            RemotingChannel channel = this.remotingChannelManager.removeConsumerChannel(context, consumerGroup, ctx.channel());
            if (channel != null) {
                ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                    channel,
                    requestHeader.getClientID(),
                    request.getLanguage(),
                    request.getVersion());
                this.messagingProcessor.unRegisterConsumer(context, consumerGroup, clientChannelInfo);
            } else {
                log.warn("unregister consumer failed, channel not exist, may has been removed, consumerGroup={}, channel={}", consumerGroup, ctx.channel());
            }
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark("");
        return response;
    }

    protected RemotingCommand checkClientConfig(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark("");
        return response;
    }

    public void doChannelCloseEvent(String remoteAddr, Channel channel) {
        Set<RemotingChannel> remotingChannelSet = this.remotingChannelManager.removeChannel(channel);
        for (RemotingChannel remotingChannel : remotingChannelSet) {
            this.messagingProcessor.doChannelCloseEvent(remoteAddr, remotingChannel);
        }
    }

    protected class ConsumerIdsChangeListenerImpl implements ConsumerIdsChangeListener {

        @Override
        public void handle(ConsumerGroupEvent event, String group, Object... args) {
            if (event == ConsumerGroupEvent.CLIENT_UNREGISTER) {
                if (args == null || args.length < 1) {
                    return;
                }
                if (args[0] instanceof ClientChannelInfo) {
                    ClientChannelInfo clientChannelInfo = (ClientChannelInfo) args[0];
                    remotingChannelManager.removeConsumerChannel(ProxyContext.createForInner(this.getClass()), group, clientChannelInfo.getChannel());
                    log.info("remove remoting channel when client unregister. clientChannelInfo:{}", clientChannelInfo);
                }
            }
        }

        @Override
        public void shutdown() {

        }
    }

    protected class ProducerChangeListenerImpl implements ProducerChangeListener {

        @Override
        public void handle(ProducerGroupEvent event, String group, ClientChannelInfo clientChannelInfo) {
            if (event == ProducerGroupEvent.CLIENT_UNREGISTER) {
                remotingChannelManager.removeProducerChannel(ProxyContext.createForInner(this.getClass()), group, clientChannelInfo.getChannel());
            }
        }
    }
}
