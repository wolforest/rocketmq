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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.broker.server.connection.ClientChannelInfo;
import org.apache.rocketmq.broker.domain.consumer.ConsumerGroupInfo;
import org.apache.rocketmq.common.domain.message.MessageQueue;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerConnectionListRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseHeader;

public class ConsumerManagerActivity extends AbstractRemotingActivity {
    public ConsumerManagerActivity(RequestPipeline requestPipeline, MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
    }

    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        switch (request.getCode()) {
            case RequestCode.GET_CONSUMER_LIST_BY_GROUP: {
                return getConsumerListByGroup(ctx, request, context);
            }
            case RequestCode.LOCK_BATCH_MQ: {
                return lockBatchMQ(ctx, request, context);
            }
            case RequestCode.UNLOCK_BATCH_MQ: {
                return unlockBatchMQ(ctx, request, context);
            }
            case RequestCode.UPDATE_CONSUMER_OFFSET:
            case RequestCode.QUERY_CONSUMER_OFFSET:
            case RequestCode.SEARCH_OFFSET_BY_TIMESTAMP:
            case RequestCode.GET_MIN_OFFSET:
            case RequestCode.GET_MAX_OFFSET:
            case RequestCode.GET_EARLIEST_MSG_STORETIME: {
                return request(ctx, request, context, Duration.ofSeconds(3).toMillis());
            }
            case RequestCode.GET_CONSUMER_CONNECTION_LIST: {
                return getConsumerConnectionList(ctx, request, context);
            }
            default:
                break;
        }
        return null;
    }

    protected RemotingCommand getConsumerListByGroup(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(GetConsumerListByGroupResponseHeader.class);
        GetConsumerListByGroupRequestHeader header = (GetConsumerListByGroupRequestHeader) request.decodeCommandCustomHeader(GetConsumerListByGroupRequestHeader.class);
        ConsumerGroupInfo consumerGroupInfo = messagingProcessor.getConsumerGroupInfo(context, header.getConsumerGroup());
        List<String> clientIds = consumerGroupInfo.getAllClientId();
        GetConsumerListByGroupResponseBody body = new GetConsumerListByGroupResponseBody();
        body.setConsumerIdList(clientIds);
        response.setBody(body.encode());
        response.setCode(ResponseCode.SUCCESS);
        return response;
    }

    private ConsumerConnection createConsumerConnection(ConsumerGroupInfo consumerGroupInfo) {
        ConsumerConnection bodydata = new ConsumerConnection();
        bodydata.setConsumeFromWhere(consumerGroupInfo.getConsumeFromWhere());
        bodydata.setConsumeType(consumerGroupInfo.getConsumeType());
        bodydata.setMessageModel(consumerGroupInfo.getMessageModel());
        bodydata.getSubscriptionTable().putAll(consumerGroupInfo.getSubscriptionTable());

        return bodydata;
    }

    private void addConnection(ConsumerConnection bodydata, ConsumerGroupInfo consumerGroupInfo) {
        for (Map.Entry<Channel, ClientChannelInfo> entry : consumerGroupInfo.getChannelInfoTable().entrySet()) {
            ClientChannelInfo info = entry.getValue();
            Connection connection = new Connection();
            connection.setClientId(info.getClientId());
            connection.setLanguage(info.getLanguage());
            connection.setVersion(info.getVersion());
            connection.setClientAddr(RemotingHelper.parseChannelRemoteAddr(info.getChannel()));

            bodydata.getConnectionSet().add(connection);
        }
    }

    private byte[] getBody(ConsumerGroupInfo consumerGroupInfo) {
        ConsumerConnection connection = createConsumerConnection(consumerGroupInfo);
        addConnection(connection, consumerGroupInfo);
        return connection.encode();
    }

    protected RemotingCommand getConsumerConnectionList(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(GetConsumerConnectionListRequestHeader.class);
        GetConsumerConnectionListRequestHeader header = (GetConsumerConnectionListRequestHeader) request.decodeCommandCustomHeader(GetConsumerConnectionListRequestHeader.class);
        ConsumerGroupInfo consumerGroupInfo = messagingProcessor.getConsumerGroupInfo(context, header.getConsumerGroup());

        if (consumerGroupInfo == null) {
            return response.setCodeAndRemark(ResponseCode.CONSUMER_NOT_ONLINE, "the consumer group[" + header.getConsumerGroup() + "] not online");
        }

        byte[] body = getBody(consumerGroupInfo);
        response.setBody(body);

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    private RemotingCommand emptyMQ(LockBatchRequestBody requestBody) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setBody(requestBody.encode());
        response.setRemark("MessageQueue set is empty");
        return response;
    }

    private RemotingCommand emptyMQ(UnlockBatchRequestBody requestBody) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setBody(requestBody.encode());
        response.setRemark("MessageQueue set is empty");
        return response;
    }

    protected RemotingCommand lockBatchMQ(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        LockBatchRequestBody requestBody = LockBatchRequestBody.decode(request.getBody(), LockBatchRequestBody.class);
        Set<MessageQueue> mqSet = requestBody.getMqSet();
        if (mqSet.isEmpty()) {
            return emptyMQ(requestBody);
        }

        String brokerName = new ArrayList<>(mqSet).get(0).getBrokerName();
        messagingProcessor.request(context, brokerName, request, Duration.ofSeconds(3).toMillis())
            .thenAccept(r -> writeResponse(ctx, context, request, r))
            .exceptionally(t -> {
                writeErrResponse(ctx, context, request, t);
                return null;
            });
        return null;
    }

    protected RemotingCommand unlockBatchMQ(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        UnlockBatchRequestBody requestBody = UnlockBatchRequestBody.decode(request.getBody(), UnlockBatchRequestBody.class);
        Set<MessageQueue> mqSet = requestBody.getMqSet();
        if (mqSet.isEmpty()) {
            return emptyMQ(requestBody);
        }

        String brokerName = new ArrayList<>(mqSet).get(0).getBrokerName();
        messagingProcessor.request(context, brokerName, request, Duration.ofSeconds(3).toMillis())
            .thenAccept(r -> writeResponse(ctx, context, request, r))
            .exceptionally(t -> {
                writeErrResponse(ctx, context, request, t);
                return null;
            });
        return null;
    }
}
