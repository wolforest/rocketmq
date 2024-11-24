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

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyTLV;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.domain.constant.HAProxyConstants;
import org.apache.rocketmq.common.utils.BinaryUtils;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.AttributeKeys;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;

public class HAProxyMessageHandler extends ChannelInboundHandlerAdapter {

    private final NettyRemotingServer server;

    public HAProxyMessageHandler(NettyRemotingServer server) {
        this.server = server;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HAProxyMessage) {
            handleWithMessage((HAProxyMessage) msg, ctx.channel());
        } else {
            super.channelRead(ctx, msg);
        }
        ctx.pipeline().remove(this);
    }

    /**
     * The definition of key refers to the implementation of nginx
     * <a href="https://nginx.org/en/docs/http/ngx_http_core_module.html#var_proxy_protocol_addr">ngx_http_core_module</a>
     * @param msg msg
     * @param channel channel
     */
    private void handleWithMessage(HAProxyMessage msg, Channel channel) {
        try {
            if (StringUtils.isNotBlank(msg.sourceAddress())) {
                RemotingHelper.setPropertyToAttr(channel, AttributeKeys.PROXY_PROTOCOL_ADDR, msg.sourceAddress());
            }
            if (msg.sourcePort() > 0) {
                RemotingHelper.setPropertyToAttr(channel, AttributeKeys.PROXY_PROTOCOL_PORT, String.valueOf(msg.sourcePort()));
            }
            if (StringUtils.isNotBlank(msg.destinationAddress())) {
                RemotingHelper.setPropertyToAttr(channel, AttributeKeys.PROXY_PROTOCOL_SERVER_ADDR, msg.destinationAddress());
            }
            if (msg.destinationPort() > 0) {
                RemotingHelper.setPropertyToAttr(channel, AttributeKeys.PROXY_PROTOCOL_SERVER_PORT, String.valueOf(msg.destinationPort()));
            }
            if (CollectionUtils.isNotEmpty(msg.tlvs())) {
                msg.tlvs().forEach(tlv -> {
                    handleHAProxyTLV(tlv, channel);
                });
            }
        } finally {
            msg.release();
        }
    }

    protected void handleHAProxyTLV(HAProxyTLV tlv, Channel channel) {
        byte[] valueBytes = ByteBufUtil.getBytes(tlv.content());
        if (!BinaryUtils.isAscii(valueBytes)) {
            return;
        }
        AttributeKey<String> key = AttributeKeys.valueOf(
            HAProxyConstants.PROXY_PROTOCOL_TLV_PREFIX + String.format("%02x", tlv.typeByteValue()));
        RemotingHelper.setPropertyToAttr(channel, key, new String(valueBytes, CharsetUtil.UTF_8));
    }

}


