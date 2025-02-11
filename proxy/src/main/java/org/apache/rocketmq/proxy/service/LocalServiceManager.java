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
package org.apache.rocketmq.proxy.service;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.domain.consumer.ConsumerManager;
import org.apache.rocketmq.broker.domain.producer.ProducerManager;
import org.apache.rocketmq.client.common.NameserverAccessConfig;
import org.apache.rocketmq.client.impl.mqclient.DoNothingClientRemotingProcessor;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.common.lang.thread.ThreadFactoryImpl;
import org.apache.rocketmq.common.domain.server.AbstractStartAndShutdown;
import org.apache.rocketmq.common.domain.server.StartAndShutdown;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.proxy.service.admin.DefaultAdminService;
import org.apache.rocketmq.proxy.service.channel.ChannelManager;
import org.apache.rocketmq.proxy.service.message.LocalMessageService;
import org.apache.rocketmq.proxy.service.message.MessageService;
import org.apache.rocketmq.proxy.service.metadata.LocalMetadataService;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.proxy.service.relay.LocalProxyRelayService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.route.LocalTopicRouteService;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.proxy.service.transaction.LocalTransactionService;
import org.apache.rocketmq.proxy.service.transaction.TransactionService;
import org.apache.rocketmq.remoting.RPCHook;

public class LocalServiceManager extends AbstractStartAndShutdown implements ServiceManager {

    private final Broker broker;
    private final TopicRouteService topicRouteService;
    private final MessageService messageService;
    private final TransactionService transactionService;
    private final ProxyRelayService proxyRelayService;
    private final MetadataService metadataService;
    private final AdminService adminService;

    private MQClientAPIFactory mqClientAPIFactory;
    private final ChannelManager channelManager;

    private final ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(
        new ThreadFactoryImpl("LocalServiceManagerScheduledThread"));

    public LocalServiceManager(Broker broker, RPCHook rpcHook) {
        this.broker = broker;
        this.channelManager = new ChannelManager();
        this.messageService = new LocalMessageService(broker, channelManager, rpcHook);

        initMQClientAPIFactory(rpcHook);

        this.topicRouteService = new LocalTopicRouteService(broker, mqClientAPIFactory);
        this.transactionService = new LocalTransactionService(broker.getBrokerConfig());
        this.proxyRelayService = new LocalProxyRelayService(broker, this.transactionService);
        this.metadataService = new LocalMetadataService(broker);
        this.adminService = new DefaultAdminService(this.mqClientAPIFactory);
        this.init();
    }

    private void initMQClientAPIFactory(RPCHook rpcHook) {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        NameserverAccessConfig nameserverAccessConfig = new NameserverAccessConfig(proxyConfig.getNamesrvAddr(),
            proxyConfig.getNamesrvDomain(), proxyConfig.getNamesrvDomainSubgroup());
        this.mqClientAPIFactory = new MQClientAPIFactory(
            nameserverAccessConfig,
            "LocalMQClient_",
            1,
            new DoNothingClientRemotingProcessor(null),
            rpcHook,
            scheduledExecutorService
        );
    }

    protected void init() {
        this.appendStartAndShutdown(this.mqClientAPIFactory);
        this.appendStartAndShutdown(this.topicRouteService);
        this.appendStartAndShutdown(new LocalServiceManagerStartAndShutdown());
    }

    @Override
    public MessageService getMessageService() {
        return this.messageService;
    }

    @Override
    public TopicRouteService getTopicRouteService() {
        return this.topicRouteService;
    }

    @Override
    public ProducerManager getProducerManager() {
        return this.broker.getProducerManager();
    }

    @Override
    public ConsumerManager getConsumerManager() {
        return this.broker.getConsumerManager();
    }

    @Override
    public TransactionService getTransactionService() {
        return this.transactionService;
    }

    @Override
    public ProxyRelayService getProxyRelayService() {
        return this.proxyRelayService;
    }

    @Override
    public MetadataService getMetadataService() {
        return this.metadataService;
    }

    @Override
    public AdminService getAdminService() {
        return this.adminService;
    }

    private class LocalServiceManagerStartAndShutdown implements StartAndShutdown {
        @Override
        public void start() throws Exception {
            LocalServiceManager.this.scheduledExecutorService.scheduleWithFixedDelay(channelManager::scanAndCleanChannels, 5, 5, TimeUnit.MINUTES);
        }

        @Override
        public void shutdown() throws Exception {
            LocalServiceManager.this.scheduledExecutorService.shutdown();
        }
    }
}
