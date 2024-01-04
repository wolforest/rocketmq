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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

@ChannelHandler.Sharable
public class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    private final NettyRemotingServer server;

    public NettyServerHandler(NettyRemotingServer server) {
        this.server = server;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) {
        int localPort = RemotingHelper.parseSocketAddressPort(ctx.channel().localAddress());
        NettyRemotingAbstract remotingAbstract = server.getRemotingServerTable().get(localPort);
        if (localPort != -1 && remotingAbstract != null) {
            remotingAbstract.processMessageReceived(ctx, msg);
            return;
        }
        // The related remoting server has been shutdown, so close the connected channel
        RemotingHelper.closeChannel(ctx.channel());
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        if (channel.isWritable()) {
            if (!channel.config().isAutoRead()) {
                channel.config().setAutoRead(true);
                log.info("Channel[{}] turns writable, bytes to buffer before changing channel to un-writable: {}",
                    RemotingHelper.parseChannelRemoteAddr(channel), channel.bytesBeforeUnwritable());
            }
        } else {
            channel.config().setAutoRead(false);
            log.warn("Channel[{}] auto-read is disabled, bytes to drain before it turns writable: {}",
                RemotingHelper.parseChannelRemoteAddr(channel), channel.bytesBeforeWritable());
        }
        super.channelWritabilityChanged(ctx);
    }
}

