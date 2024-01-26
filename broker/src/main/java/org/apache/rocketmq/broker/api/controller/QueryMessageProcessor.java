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

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.opentelemetry.api.common.Attributes;
import org.apache.rocketmq.broker.infra.zerocopy.OneMessageTransfer;
import org.apache.rocketmq.broker.infra.zerocopy.QueryMessageTransfer;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.metrics.RemotingMetricsManager;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.store.api.dto.QueryMessageResult;
import org.apache.rocketmq.store.infra.mappedfile.SelectMappedBufferResult;

import java.util.concurrent.TimeUnit;

import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_REQUEST_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESPONSE_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESULT;

public class QueryMessageProcessor implements NettyRequestProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final Broker broker;

    public QueryMessageProcessor(final Broker broker) {
        this.broker = broker;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.QUERY_MESSAGE:
                return this.queryMessage(ctx, request);
            case RequestCode.VIEW_MESSAGE_BY_ID:
                return this.viewMessageById(ctx, request);
            default:
                break;
        }

        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand queryMessage(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(QueryMessageResponseHeader.class);
        final QueryMessageResponseHeader responseHeader = (QueryMessageResponseHeader) response.readCustomHeader();
        final QueryMessageRequestHeader requestHeader = (QueryMessageRequestHeader) request.decodeCommandCustomHeader(QueryMessageRequestHeader.class);

        response.setOpaque(request.getOpaque());

        String isUniqueKey = request.getExtFields().get(MQConstants.UNIQUE_MSG_QUERY_FLAG);
        if (isUniqueKey != null && isUniqueKey.equals("true")) {
            requestHeader.setMaxNum(this.broker.getMessageStoreConfig().getDefaultQueryMaxNum());
        }

        final QueryMessageResult queryMessageResult = this.broker.getMessageStore().queryMessage(requestHeader.getTopic(),
                requestHeader.getKey(), requestHeader.getMaxNum(), requestHeader.getBeginTimestamp(), requestHeader.getEndTimestamp());
        assert queryMessageResult != null;

        responseHeader.setIndexLastUpdatePhyoffset(queryMessageResult.getIndexLastUpdatePhyoffset());
        responseHeader.setIndexLastUpdateTimestamp(queryMessageResult.getIndexLastUpdateTimestamp());
        if (queryMessageResult.getBufferTotalSize() <= 0) {
            return response.setCodeAndRemark(ResponseCode.QUERY_NOT_FOUND, "can not find message, maybe time range not correct");
        }

        return handleQueryResult(queryMessageResult, request, response, ctx);
    }

    private RemotingCommand handleQueryResult(QueryMessageResult queryMessageResult, RemotingCommand request, RemotingCommand response, ChannelHandlerContext ctx) {
        response.setCodeAndRemark(ResponseCode.SUCCESS, null);

        try {
            FileRegion fileRegion = new QueryMessageTransfer(response.encodeHeader(queryMessageResult.getBufferTotalSize()), queryMessageResult);
            ctx.channel()
                .writeAndFlush(fileRegion)
                .addListener(createQueryListener(queryMessageResult, request, response));
        } catch (Throwable e) {
            LOGGER.error("", e);
            queryMessageResult.release();
        }

        return null;
    }

    private ChannelFutureListener createQueryListener(QueryMessageResult queryResult, RemotingCommand request, RemotingCommand response) {
        return future -> {
            queryResult.release();

            Attributes attributes = RemotingMetricsManager.newAttributesBuilder()
                .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode()))
                .put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(response.getCode()))
                .put(LABEL_RESULT, RemotingMetricsManager.getWriteAndFlushResult(future))
                .build();

            RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributes);

            if (!future.isSuccess()) {
                LOGGER.error("transfer query message by page cache failed, ", future.cause());
            }
        };
    }

    private ChannelFutureListener createViewListener(SelectMappedBufferResult viewResult, RemotingCommand request, RemotingCommand response) {
        return future -> {
            viewResult.release();

            Attributes attributes = RemotingMetricsManager.newAttributesBuilder()
                .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode()))
                .put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(response.getCode()))
                .put(LABEL_RESULT, RemotingMetricsManager.getWriteAndFlushResult(future))
                .build();
            RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributes);

            if (!future.isSuccess()) {
                LOGGER.error("Transfer one message from page cache failed, ", future.cause());
            }
        };
    }

    private RemotingCommand viewMessageById(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final ViewMessageRequestHeader requestHeader = (ViewMessageRequestHeader) request.decodeCommandCustomHeader(ViewMessageRequestHeader.class);

        response.setOpaque(request.getOpaque());

        final SelectMappedBufferResult result = this.broker.getMessageStore().selectOneMessageByOffset(requestHeader.getOffset());
        if (result == null) {
            return response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "can not find message by the offset, " + requestHeader.getOffset());
        }

        response.setCodeAndRemark(ResponseCode.SUCCESS, null);

        try {
            //Create a FileRegion to write directly to the channel, zero-copy
            FileRegion fileRegion = new OneMessageTransfer(response.encodeHeader(result.getSize()), result);
            ctx.channel()
                .writeAndFlush(fileRegion)
                .addListener(createViewListener(result, request, response));
        } catch (Throwable e) {
            LOGGER.error("", e);
            result.release();
        }

        return null;
    }
}
