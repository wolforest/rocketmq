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
package org.apache.rocketmq.broker.server;

import java.net.InetSocketAddress;
import java.util.Map;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.broker.ShutdownHook;
import org.apache.rocketmq.broker.server.bootstrap.BrokerAuthService;
import org.apache.rocketmq.broker.server.bootstrap.BrokerClusterService;
import org.apache.rocketmq.broker.server.bootstrap.BrokerMessageService;
import org.apache.rocketmq.broker.server.bootstrap.BrokerMetadataService;
import org.apache.rocketmq.broker.server.bootstrap.BrokerNettyServer;
import org.apache.rocketmq.broker.server.daemon.BrokerScheduleService;
import org.apache.rocketmq.broker.server.bootstrap.BrokerServiceManager;
import org.apache.rocketmq.broker.server.bootstrap.BrokerServiceRegistry;
import org.apache.rocketmq.broker.domain.consumer.ConsumerManager;
import org.apache.rocketmq.broker.domain.producer.ProducerManager;
import org.apache.rocketmq.broker.infra.Broker2Client;
import org.apache.rocketmq.broker.domain.coldctr.ColdDataCgCtrThread;
import org.apache.rocketmq.broker.domain.coldctr.ColdDataPullRequestHoldThread;
import org.apache.rocketmq.broker.infra.EscapeBridge;
import org.apache.rocketmq.broker.domain.metadata.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.domain.queue.offset.BroadcastOffsetManager;
import org.apache.rocketmq.broker.domain.queue.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.domain.queue.offset.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.infra.ClusterClient;
import org.apache.rocketmq.broker.server.daemon.pop.PopInflightMessageCounter;
import org.apache.rocketmq.broker.server.daemon.schedule.ScheduleMessageService;
import org.apache.rocketmq.broker.domain.metadata.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.domain.metadata.topic.TopicConfigManager;
import org.apache.rocketmq.broker.domain.metadata.topic.TopicQueueMappingManager;
import org.apache.rocketmq.broker.domain.metadata.topic.TopicRouteInfoManager;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.app.BrokerIdentity;
import org.apache.rocketmq.common.app.config.BrokerPathConfigHelper;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.api.broker.stats.BrokerStatsManager;
import org.apache.rocketmq.store.domain.timer.persistence.TimerCheckpoint;
import org.apache.rocketmq.store.domain.timer.TimerMessageStore;

/**
 * @renamed from BrokerController to Broker
 */
public class Broker {
    protected static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    protected final BrokerConfig brokerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    protected final MessageStoreConfig messageStoreConfig;
    private final AuthConfig authConfig;
    private Configuration configuration;

    protected volatile boolean shutdown = false;
    protected volatile long shouldStartTime;
    protected volatile boolean isIsolated = false;

    private final BrokerNettyServer brokerNettyServer;
    private final BrokerAuthService brokerAuthService;
    private final BrokerScheduleService brokerScheduleService;
    private final BrokerMetadataService brokerMetadataService;
    private final BrokerServiceRegistry brokerServiceRegistry;
    private final BrokerServiceManager brokerServiceManager;
    private final BrokerClusterService brokerClusterService;
    private final BrokerMessageService brokerMessageService;

    public Broker(BrokerConfig brokerConfig, NettyServerConfig serverConfig, NettyClientConfig clientConfig, MessageStoreConfig storeConfig, AuthConfig authConfig, ShutdownHook shutdownHook) {
        this(brokerConfig, serverConfig, clientConfig, storeConfig, authConfig);
        this.brokerServiceManager.setShutdownHook(shutdownHook);
    }

    public Broker(final BrokerConfig brokerConfig, final MessageStoreConfig messageStoreConfig) {
        this(brokerConfig, null, null, messageStoreConfig, null);
    }

    public Broker(
        final BrokerConfig brokerConfig,
        final NettyServerConfig nettyServerConfig,
        final NettyClientConfig nettyClientConfig,
        final MessageStoreConfig messageStoreConfig
    ) {
        this(brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig, null);
    }

    public Broker(BrokerConfig brokerConfig, NettyServerConfig serverConfig, NettyClientConfig clientConfig, MessageStoreConfig storeConfig, AuthConfig authConfig) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = serverConfig;
        this.nettyClientConfig = clientConfig;
        this.messageStoreConfig = storeConfig;
        this.authConfig = authConfig;

        initConfiguration();

        /* the instance creating order matters, do not change it. start ... */
        this.brokerMetadataService = new BrokerMetadataService(this);
        this.brokerNettyServer = new BrokerNettyServer(brokerConfig, storeConfig, serverConfig, this);
        this.brokerAuthService = new BrokerAuthService(this);
        this.brokerServiceRegistry = new BrokerServiceRegistry(this);
        this.brokerServiceManager = new BrokerServiceManager(this);
        this.brokerScheduleService = new BrokerScheduleService(brokerConfig, storeConfig, this);
        this.brokerClusterService = new BrokerClusterService(this);
        this.brokerMessageService = new BrokerMessageService(this);
        /* the instance creating order matters, do not change it. ... end */
    }

    public boolean initialize() throws CloneNotSupportedException {
        if (!this.brokerMetadataService.load()) {
            return false;
        }

        if (!brokerMessageService.init()) {
            return false;
        }

        brokerClusterService.load();
        if (!brokerServiceManager.load()) {
            return false;
        }
        brokerServiceManager.initialize();
        initializeRemotingServer();

        brokerAuthService.initialize();
        initializeScheduledTasks();
        return brokerNettyServer.initFileWatchService();
    }

    public void shutdown() {
        shutdownBasicService();
        this.brokerServiceRegistry.shutdown();
        this.brokerAuthService.shutdown();
    }

    public void start() throws Exception {
        this.shouldStartTime = System.currentTimeMillis() + messageStoreConfig.getDisappearTimeAfterStart();
        if (messageStoreConfig.getTotalReplicas() > 1 && this.brokerConfig.isEnableSlaveActingMaster()) {
            isIsolated = true;
        }

        this.brokerServiceRegistry.start();
        startBasicService();

        if (!isIsolated && !this.messageStoreConfig.isEnableDLegerCommitLog() && !this.messageStoreConfig.isDuplicationEnable()) {
            this.brokerMessageService.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == MQConstants.MASTER_ID);
            this.brokerServiceRegistry.registerBrokerAll(true, false, true);
        }

        this.brokerScheduleService.start();
        if (brokerConfig.isSkipPreOnline()) {
            registerBroker();
        }

        this.brokerScheduleService.refreshMetadata();
    }

    /**
     * do not store BrokerIdentity instance in the object
     * because brokerId will change while slaveActingMaster
     *
     * @return BrokerIdentity
     */
    public BrokerIdentity getBrokerIdentity() {
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            return new BrokerIdentity(
                brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
                Integer.parseInt(messageStoreConfig.getdLegerSelfId().substring(1)), brokerConfig.isInBrokerContainer());
        } else {
            return new BrokerIdentity(
                brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
                brokerConfig.getBrokerId(), brokerConfig.isInBrokerContainer());
        }
    }

    //**************************************** private or protected methods start ****************************************************
    protected void registerBroker() {
        Broker.LOG.info("{} start service", this.brokerConfig.getCanonicalName());

        this.brokerMessageService.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == MQConstants.MASTER_ID);
        this.brokerServiceRegistry.registerBrokerAll(true, false, brokerConfig.isForceRegister());

        isIsolated = false;
    }

    protected void unregisterBroker() {
        Broker.LOG.info("{} stop service", this.getBrokerConfig().getCanonicalName());
        isIsolated = true;
        this.brokerMessageService.changeSpecialServiceStatus(false);
    }

    protected void initializeRemotingServer() throws CloneNotSupportedException {
        brokerNettyServer.init();
    }

    protected void initializeScheduledTasks() {
        brokerScheduleService.init();
    }

    private void initConfiguration() {
        String brokerConfigPath;
        if (brokerConfig.getBrokerConfigPath() != null && !brokerConfig.getBrokerConfigPath().isEmpty()) {
            brokerConfigPath = brokerConfig.getBrokerConfigPath();
        } else {
            brokerConfigPath = BrokerPathConfigHelper.getBrokerConfigPath();
        }

        this.configuration = new Configuration(
            LOG,
            brokerConfigPath,
            this.brokerConfig, this.nettyServerConfig, this.nettyClientConfig, this.messageStoreConfig
        );
    }

    protected void shutdownBasicService() {
        shutdown = true;

        this.brokerServiceRegistry.unregisterBrokerAll();
        this.brokerNettyServer.shutdown();
        this.brokerMessageService.shutdown();
        this.brokerServiceManager.shutdown();
        this.brokerMetadataService.shutdown();
        this.brokerClusterService.shutdown();
    }

    protected void startBasicService() throws Exception {
        this.brokerClusterService.start();
        this.brokerNettyServer.start();
        this.brokerMessageService.start();
        this.brokerServiceManager.start();
    }

    //**************************************** private or protected methods end   ****************************************************

    //**************************************** getter and setter start ****************************************************

    public BrokerAuthService getBrokerAuthService() {
        return brokerAuthService;
    }

    public BrokerClusterService getBrokerClusterService() {
        return brokerClusterService;
    }

    public BrokerServiceManager getBrokerServiceManager() {
        return brokerServiceManager;
    }

    public BrokerServiceRegistry getBrokerServiceRegistry() {
        return brokerServiceRegistry;
    }

    public BrokerMetadataService getBrokerMetadataManager() {
        return brokerMetadataService;
    }

    public BrokerMessageService getBrokerMessageService() {
        return brokerMessageService;
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public AuthConfig getAuthConfig() {
        return authConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public BrokerScheduleService getBrokerScheduleService() {
        return brokerScheduleService;
    }

    public MessageStore getMessageStore() {
        return brokerMessageService.getMessageStore();
    }

    public void setMessageStore(MessageStore messageStore) {
        brokerMessageService.setMessageStore(messageStore);
    }

    public Broker2Client getBroker2Client() {
        return brokerServiceManager.getBroker2Client();
    }

    public ConsumerManager getConsumerManager() {
        return brokerServiceManager.getConsumerManager();
    }

    public ConsumerFilterManager getConsumerFilterManager() {
        return this.brokerMetadataService.getConsumerFilterManager();
    }

    public ConsumerOrderInfoManager getConsumerOrderInfoManager() {
        return this.brokerMetadataService.getConsumerOrderInfoManager();
    }

    public PopInflightMessageCounter getPopInflightMessageCounter() {
        return brokerServiceManager.getPopInflightMessageCounter();
    }

    public ConsumerOffsetManager getConsumerOffsetManager() {
        return this.brokerMetadataService.getConsumerOffsetManager();
    }

    public BroadcastOffsetManager getBroadcastOffsetManager() {
        return brokerServiceManager.getBroadcastOffsetManager();
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public ProducerManager getProducerManager() {
        return brokerServiceManager.getProducerManager();
    }

    public RemotingServer getFastRemotingServer() {
        return getBrokerNettyServer().getFastRemotingServer();
    }

    public void setSubscriptionGroupManager(SubscriptionGroupManager subscriptionGroupManager) {
        this.brokerMetadataService.setSubscriptionGroupManager(subscriptionGroupManager);
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return this.brokerMetadataService.getSubscriptionGroupManager();
    }

    public TimerMessageStore getTimerMessageStore() {
        return brokerMessageService.getTimerMessageStore();
    }

    public String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
    }

    public TopicConfigManager getTopicConfigManager() {
        return this.brokerMetadataService.getTopicConfigManager();
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.brokerMetadataService.setTopicConfigManager(topicConfigManager);
    }

    public TopicQueueMappingManager getTopicQueueMappingManager() {
        return this.brokerMetadataService.getTopicQueueMappingManager();
    }

    public String getHAServerAddr() {
        return this.brokerConfig.getBrokerIP2() + ":" + this.messageStoreConfig.getHaListenPort();
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerServiceManager.getBrokerStatsManager();
    }

    public RemotingServer getRemotingServer() {
        return getBrokerNettyServer().getRemotingServer();
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        getBrokerNettyServer().setRemotingServer(remotingServer);
    }

    public void setFastRemotingServer(RemotingServer fastRemotingServer) {
        getBrokerNettyServer().setFastRemotingServer(fastRemotingServer);
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public ClusterClient getClusterClient() {
        return this.brokerServiceRegistry.getClusterClient();
    }

    public void setBrokerOuterAPI(ClusterClient clusterClient) {
        this.brokerServiceRegistry.setBrokerOuterAPI(clusterClient);
    }

    public InetSocketAddress getStoreHost() {
        return this.brokerNettyServer.getStoreHost();
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public Map<Class, AccessValidator> getAccessValidatorMap() {
        return getBrokerNettyServer().getAccessValidatorMap();
    }

    public long getMinBrokerIdInGroup() {
        return this.brokerConfig.getBrokerId();
    }

    public Broker peekMasterBroker() {
        return brokerConfig.getBrokerId() == MQConstants.MASTER_ID ? this : null;
    }

    public BrokerMemberGroup getBrokerMemberGroup() {
        return this.getBrokerScheduleService().getBrokerMemberGroup();
    }

    public EscapeBridge getEscapeBridge() {
        return brokerMessageService.getEscapeBridge();
    }

    public long getShouldStartTime() {
        return shouldStartTime;
    }

    public ScheduleMessageService getScheduleMessageService() {
        return brokerMessageService.getScheduleMessageService();
    }

    public void setIsolated(boolean isolated) {
        isIsolated = isolated;
    }

    public boolean isIsolated() {
        return this.isIsolated;
    }

    public TimerCheckpoint getTimerCheckpoint() {
        return brokerMessageService.getTimerCheckpoint();
    }

    public TopicRouteInfoManager getTopicRouteInfoManager() {
        return brokerServiceManager.getTopicRouteInfoManager();
    }

    public ColdDataPullRequestHoldThread getColdDataPullRequestHoldService() {
        return brokerServiceManager.getColdDataPullRequestHoldService();
    }

    public ColdDataCgCtrThread getColdDataCgCtrService() {
        return brokerServiceManager.getColdDataCgCtrService();
    }

    public BrokerNettyServer getBrokerNettyServer() {
        return this.brokerNettyServer;
    }
    //**************************************** getter and setter end ****************************************************

}
