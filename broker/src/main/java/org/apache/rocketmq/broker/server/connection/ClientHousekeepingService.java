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
package org.apache.rocketmq.broker.server.connection;

import io.netty.channel.Channel;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.common.lang.thread.ThreadFactoryImpl;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;

public class ClientHousekeepingService implements ChannelEventListener {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final Broker broker;

    private final ScheduledExecutorService scheduledExecutorService;

    public ClientHousekeepingService(final Broker broker) {
        this.broker = broker;
        scheduledExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new ThreadFactoryImpl("ClientHousekeepingScheduledThread", broker.getBrokerIdentity()));
    }

    public void start() {

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    ClientHousekeepingService.this.scanExceptionChannel();
                } catch (Throwable e) {
                    log.error("Error occurred when scan not active client channels.", e);
                }
            }
        }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);
    }

    private void scanExceptionChannel() {
        this.broker.getProducerManager().scanNotActiveChannel();
        this.broker.getConsumerManager().scanNotActiveChannel();
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {
        this.broker.getBrokerStatsManager().incChannelConnectNum();
    }

    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        this.broker.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        this.broker.getConsumerManager().doChannelCloseEvent(remoteAddr, channel);
        this.broker.getBrokerStatsManager().incChannelCloseNum();
    }

    @Override
    public void onChannelException(String remoteAddr, Channel channel) {
        this.broker.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        this.broker.getConsumerManager().doChannelCloseEvent(remoteAddr, channel);
        this.broker.getBrokerStatsManager().incChannelExceptionNum();
    }

    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {
        this.broker.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        this.broker.getConsumerManager().doChannelCloseEvent(remoteAddr, channel);
        this.broker.getBrokerStatsManager().incChannelIdleNum();
    }

    @Override
    public void onChannelActive(String remoteAddr, Channel channel) {

    }
}
