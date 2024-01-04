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
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.lang.Pair;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * The NettyRemotingServer supports bind multiple ports, each port bound by a SubRemotingServer. The
 * SubRemotingServer will delegate all the functions to NettyRemotingServer, so the sub server can share all the
 * resources from its parent server.
 */
class SubRemotingServer extends NettyRemotingAbstract implements RemotingServer {
    private volatile int listenPort;
    private volatile Channel serverChannel;
    private final NettyRemotingServer parent;

    SubRemotingServer(final int port, final int permitsOnway, final int permitsAsync, NettyRemotingServer parent) {
        super(permitsOnway, permitsAsync);
        listenPort = port;
        this.parent = parent;
    }

    @Override
    public void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
        final ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = parent.getPublicExecutor();
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor) {
        this.defaultRequestProcessorPair = new Pair<>(processor, executor);
    }

    @Override
    public int localListenPort() {
        return listenPort;
    }

    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode) {
        return this.processorTable.get(requestCode);
    }

    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessorPair() {
        return this.defaultRequestProcessorPair;
    }

    @Override
    public RemotingServer newRemotingServer(final int port) {
        throw new UnsupportedOperationException("The SubRemotingServer of NettyRemotingServer " +
            "doesn't support new nested RemotingServer");
    }

    @Override
    public void removeRemotingServer(final int port) {
        throw new UnsupportedOperationException("The SubRemotingServer of NettyRemotingServer " +
            "doesn't support remove nested RemotingServer");
    }

    @Override
    public RemotingCommand invokeSync(final Channel channel, final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        return this.invokeSyncImpl(channel, request, timeoutMillis);
    }

    @Override
    public void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
    }

    @Override
    public void invokeOneway(final Channel channel, final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeOnewayImpl(channel, request, timeoutMillis);
    }

    @Override
    public void start() {
        try {
            if (listenPort < 0) {
                listenPort = 0;
            }
            this.serverChannel = parent.getServerBootstrap().bind(listenPort).sync().channel();
            if (0 == listenPort) {
                InetSocketAddress addr = (InetSocketAddress) this.serverChannel.localAddress();
                this.listenPort = addr.getPort();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("this.subRemotingServer.serverBootstrap.bind().sync() InterruptedException", e);
        }
    }

    @Override
    public void shutdown() {
        isShuttingDown.set(true);
        if (this.serverChannel != null) {
            try {
                this.serverChannel.close().await(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
        }
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return parent.getChannelEventListener();
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return parent.getCallbackExecutor();
    }
}

