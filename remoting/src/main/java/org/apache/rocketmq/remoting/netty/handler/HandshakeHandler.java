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
package org.apache.rocketmq.remoting.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.ProtocolDetectionResult;
import io.netty.handler.codec.ProtocolDetectionState;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;

public class HandshakeHandler extends ByteToMessageDecoder {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    public static final String HA_PROXY_DECODER = "HAProxyDecoder";
    public static final String HA_PROXY_HANDLER = "HAProxyHandler";
    private final NettyRemotingServer server;

    public HandshakeHandler(NettyRemotingServer server) {
        this.server = server;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out) throws Exception {
        try {
            ProtocolDetectionResult<HAProxyProtocolVersion> detectionResult = HAProxyMessageDecoder.detectProtocol(byteBuf);
            if (detectionResult.state() == ProtocolDetectionState.NEEDS_MORE_DATA) {
                return;
            }
            if (detectionResult.state() == ProtocolDetectionState.DETECTED) {
                ctx.pipeline().addAfter(server.getDefaultEventExecutorGroup(), ctx.name(), HA_PROXY_DECODER, new HAProxyMessageDecoder())
                    .addAfter(server.getDefaultEventExecutorGroup(), HA_PROXY_DECODER, HA_PROXY_HANDLER, new HAProxyMessageHandler())
                    .addAfter(server.getDefaultEventExecutorGroup(), HA_PROXY_HANDLER, NettyRemotingServer.TLS_MODE_HANDLER, server.getTlsModeHandler());
            } else {
                ctx.pipeline().addAfter(server.getDefaultEventExecutorGroup(), ctx.name(), NettyRemotingServer.TLS_MODE_HANDLER, server.getTlsModeHandler());
            }

            try {
                // Remove this handler
                ctx.pipeline().remove(this);
            } catch (NoSuchElementException e) {
                log.error("Error while removing HandshakeHandler", e);
            }
        } catch (Exception e) {
            log.error("process proxy protocol negotiator failed.", e);
            throw e;
        }
    }
}

