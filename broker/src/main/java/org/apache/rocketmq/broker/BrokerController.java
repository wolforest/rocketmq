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
package org.apache.rocketmq.broker;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.broker.bootstrap.BrokerMessageService;
import org.apache.rocketmq.broker.bootstrap.BrokerMetadataManager;
import org.apache.rocketmq.broker.bootstrap.BrokerNettyServer;
import org.apache.rocketmq.broker.bootstrap.BrokerScheduleService;
import org.apache.rocketmq.broker.bootstrap.BrokerServiceManager;
import org.apache.rocketmq.broker.bootstrap.BrokerServiceRegistry;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.client.rebalance.RebalanceLockManager;
import org.apache.rocketmq.broker.coldctr.ColdDataCgCtrService;
import org.apache.rocketmq.broker.coldctr.ColdDataPullRequestHoldService;
import org.apache.rocketmq.broker.controller.ReplicasManager;
import org.apache.rocketmq.broker.failover.EscapeBridge;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.latency.BrokerFastFailure;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.offset.BroadcastOffsetManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.offset.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.plugin.BrokerAttachedPlugin;
import org.apache.rocketmq.broker.processor.PopInflightMessageCounter;
import org.apache.rocketmq.broker.schedule.ScheduleMessageService;
import org.apache.rocketmq.broker.slave.SlaveSynchronize;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.broker.topic.TopicQueueMappingManager;
import org.apache.rocketmq.broker.topic.TopicRouteInfoManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.BrokerSyncInfo;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStats;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.stats.LmqBrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerMessageStore;

public class BrokerController {
    protected static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    protected final BrokerConfig brokerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    protected final MessageStoreConfig messageStoreConfig;
    private Configuration configuration;

    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();
    protected final SlaveSynchronize slaveSynchronize;
    protected ReplicasManager replicasManager;
    protected boolean updateMasterHAServerAddrPeriodically = false;

    private InetSocketAddress storeHost;

    protected volatile boolean shutdown = false;
    protected ShutdownHook shutdownHook;

    protected List<BrokerAttachedPlugin> brokerAttachedPlugins = new ArrayList<>();
    protected volatile long shouldStartTime;
    protected volatile boolean isIsolated = false;
    protected volatile long minBrokerIdInGroup = 0;
    protected volatile String minBrokerAddrInGroup = null;
    private final Lock lock = new ReentrantLock();

    protected final BrokerStatsManager brokerStatsManager;
    private BrokerMetricsManager brokerMetricsManager;

    private final BrokerNettyServer brokerNettyServer;
    private final BrokerScheduleService brokerScheduleService;
    private final BrokerMetadataManager brokerMetadataManager;
    private final BrokerServiceRegistry brokerServiceRegistry;
    private final BrokerServiceManager brokerServiceManager;
    private BrokerMessageService brokerMessageService;

    public BrokerController(
        final BrokerConfig brokerConfig,
        final NettyServerConfig nettyServerConfig,
        final NettyClientConfig nettyClientConfig,
        final MessageStoreConfig messageStoreConfig,
        final ShutdownHook shutdownHook
    ) {
        this(brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);
        this.shutdownHook = shutdownHook;
    }

    public BrokerController(
        final BrokerConfig brokerConfig,
        final MessageStoreConfig messageStoreConfig
    ) {
        this(brokerConfig, null, null, messageStoreConfig);
    }

    public BrokerController(
        final BrokerConfig brokerConfig,
        final NettyServerConfig nettyServerConfig,
        final NettyClientConfig nettyClientConfig,
        final MessageStoreConfig messageStoreConfig
    ) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.setStoreHost(new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), getListenPort()));
        initConfiguration();

        this.brokerStatsManager = messageStoreConfig.isEnableLmq()
            ? new LmqBrokerStatsManager(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.isEnableDetailStat())
            : new BrokerStatsManager(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.isEnableDetailStat());

        this.brokerMetadataManager = new BrokerMetadataManager(this);
        this.brokerNettyServer = new BrokerNettyServer(brokerConfig, messageStoreConfig, nettyServerConfig, this);
        this.brokerScheduleService = new BrokerScheduleService(brokerConfig, messageStoreConfig, this);
        this.brokerServiceRegistry = new BrokerServiceRegistry(this);
        this.brokerServiceManager = new BrokerServiceManager(this);

        this.slaveSynchronize = new SlaveSynchronize(this);

        initProducerStateGetter();
        initConsumerStateGetter();
    }

    public boolean initialize() throws CloneNotSupportedException {
        boolean result = this.brokerMetadataManager.load();
        if (!result) {
            return false;
        }

        this.brokerMessageService = new BrokerMessageService(this);
        result = brokerMessageService.init();
        if (!result) {
            return false;
        }

        return this.initAndLoadService();
    }

    public void shutdown() {
        shutdownBasicService();
        this.brokerServiceRegistry.shutdown();
    }

    public void start() throws Exception {
        this.shouldStartTime = System.currentTimeMillis() + messageStoreConfig.getDisappearTimeAfterStart();

        if (messageStoreConfig.getTotalReplicas() > 1 && this.brokerConfig.isEnableSlaveActingMaster()) {
            isIsolated = true;
        }

        this.brokerServiceRegistry.start();
        startBasicService();

        if (!isIsolated && !this.messageStoreConfig.isEnableDLegerCommitLog() && !this.messageStoreConfig.isDuplicationEnable()) {
            this.brokerMessageService.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == MixAll.MASTER_ID);
            this.brokerServiceRegistry.registerBrokerAll(true, false, true);
        }

        this.brokerScheduleService.start();
        if (brokerConfig.isSkipPreOnline()) {
            startServiceWithoutCondition();
        }
    }

    public void startService(long minBrokerId, String minBrokerAddr) {
        BrokerController.LOG.info("{} start service, min broker id is {}, min broker addr: {}",
            this.brokerConfig.getCanonicalName(), minBrokerId, minBrokerAddr);
        this.minBrokerIdInGroup = minBrokerId;
        this.minBrokerAddrInGroup = minBrokerAddr;

        this.brokerMessageService.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == minBrokerId);
        this.brokerServiceRegistry.registerBrokerAll(true, false, brokerConfig.isForceRegister());

        isIsolated = false;
    }

    public void startServiceWithoutCondition() {
        BrokerController.LOG.info("{} start service", this.brokerConfig.getCanonicalName());

        this.brokerMessageService.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == MixAll.MASTER_ID);
        this.brokerServiceRegistry.registerBrokerAll(true, false, brokerConfig.isForceRegister());

        isIsolated = false;
    }

    public void updateMinBroker(long minBrokerId, String minBrokerAddr) {
        if (brokerConfig.isEnableSlaveActingMaster() && brokerConfig.getBrokerId() != MixAll.MASTER_ID) {
            if (!lock.tryLock()) {
                return;
            }
        }

        try {
            if (minBrokerId != this.minBrokerIdInGroup) {
                String offlineBrokerAddr = null;
                if (minBrokerId > this.minBrokerIdInGroup) {
                    offlineBrokerAddr = this.minBrokerAddrInGroup;
                }
                onMinBrokerChange(minBrokerId, minBrokerAddr, offlineBrokerAddr, null);
            }
        } finally {
            lock.unlock();
        }
    }

    public void updateMinBroker(long minBrokerId, String minBrokerAddr, String offlineBrokerAddr, String masterHaAddr) {
        if (!brokerConfig.isEnableSlaveActingMaster() || brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
            return;
        }

        try {
            if (!lock.tryLock(3000, TimeUnit.MILLISECONDS)) {
                return;
            }

            try {
                if (minBrokerId != this.minBrokerIdInGroup) {
                    onMinBrokerChange(minBrokerId, minBrokerAddr, offlineBrokerAddr, masterHaAddr);
                }
            } finally {
                lock.unlock();
            }
        } catch (InterruptedException e) {
            LOG.error("Update min broker error, {}", e);
        }
    }

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

    //**************************************** debug methods start ****************************************************
    public long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        final Runnable peek = q.peek();
        if (peek != null) {
            RequestTask rt = BrokerFastFailure.castRunnable(peek);
            slowTimeMills = rt == null ? 0 : this.getBrokerMessageService().getMessageStore().now() - rt.getCreateTimestamp();
        }

        if (slowTimeMills < 0) {
            slowTimeMills = 0;
        }

        return slowTimeMills;
    }

    public long headSlowTimeMills4SendThreadPoolQueue() {
        return this.brokerNettyServer.headSlowTimeMills4SendThreadPoolQueue();
    }

    public long headSlowTimeMills4PullThreadPoolQueue() {
        return this.brokerNettyServer.headSlowTimeMills4PullThreadPoolQueue();
    }

    public long headSlowTimeMills4LitePullThreadPoolQueue() {
        return this.brokerNettyServer.headSlowTimeMills4PullThreadPoolQueue();
    }

    public long headSlowTimeMills4QueryThreadPoolQueue() {
        return this.brokerNettyServer.headSlowTimeMills4QueryThreadPoolQueue();
    }

    public void printWaterMark() {
        this.brokerNettyServer.printWaterMark();
    }

    //**************************************** private or protected methods start ****************************************************
    public void stopService() {
        BrokerController.LOG.info("{} stop service", this.getBrokerConfig().getCanonicalName());
        isIsolated = true;
        this.brokerMessageService.changeSpecialServiceStatus(false);
    }

    public boolean initAndLoadService() throws CloneNotSupportedException {
        boolean result = true;

        if (this.brokerConfig.isEnableControllerMode()) {
            this.replicasManager = new ReplicasManager(this);
            this.replicasManager.setFenced(true);
        }

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                result = result && brokerAttachedPlugin.load();
            }
        }

        if (!result) {
            return false;
        }

        this.brokerMetricsManager = new BrokerMetricsManager(this);

        initializeRemotingServer();
        initializeScheduledTasks();
        return brokerNettyServer.initFileWatchService();
    }

    protected void initializeRemotingServer() throws CloneNotSupportedException {
        brokerNettyServer.init();
    }

    protected void initializeScheduledTasks() {
        brokerScheduleService.init();
    }

    private void initProducerStateGetter() {
        this.brokerStatsManager.setProduerStateGetter(new BrokerStatsManager.StateGetter() {
            @Override
            public boolean online(String instanceId, String group, String topic) {
                if (getTopicConfigManager().getTopicConfigTable().containsKey(NamespaceUtil.wrapNamespace(instanceId, topic))) {
                    return getProducerManager().groupOnline(NamespaceUtil.wrapNamespace(instanceId, group));
                } else {
                    return getProducerManager().groupOnline(group);
                }
            }
        });
    }

    private void initConsumerStateGetter() {
        this.brokerStatsManager.setConsumerStateGetter(new BrokerStatsManager.StateGetter() {
            @Override
            public boolean online(String instanceId, String group, String topic) {
                String topicFullName = NamespaceUtil.wrapNamespace(instanceId, topic);
                if (getTopicConfigManager().getTopicConfigTable().containsKey(topicFullName)) {
                    return getConsumerManager().findSubscriptionData(NamespaceUtil.wrapNamespace(instanceId, group), topicFullName) != null;
                } else {
                    return getConsumerManager().findSubscriptionData(group, topic) != null;
                }
            }
        });
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

        if (this.shutdownHook != null) {
            this.shutdownHook.beforeShutdown(this);
        }
        this.brokerNettyServer.shutdown();
        this.brokerMessageService.shutdown();
        this.brokerServiceManager.shutdown();
        this.brokerMetadataManager.shutdown();

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.shutdown();
        }

        if (this.replicasManager != null) {
            this.replicasManager.shutdown();
        }

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                brokerAttachedPlugin.shutdown();
            }
        }
    }

    protected void startBasicService() throws Exception {
        if (this.replicasManager != null) {
            this.replicasManager.start();
        }

        this.storeHost = new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), this.getNettyServerConfig().getListenPort());

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                brokerAttachedPlugin.start();
            }
        }

        this.brokerNettyServer.start();
        this.brokerMessageService.start();
        this.brokerServiceManager.start();

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
        }
    }

    private void onMasterOffline() {
        // close channels with master broker
        String masterAddr = this.slaveSynchronize.getMasterAddr();
        if (masterAddr != null) {
            this.getBrokerOuterAPI().getRemotingClient().closeChannels(
                Arrays.asList(masterAddr, MixAll.brokerVIPChannel(true, masterAddr)));
        }
        // master not available, stop sync
        this.slaveSynchronize.setMasterAddr(null);
        brokerMessageService.getMessageStore().updateHaMasterAddress(null);
    }

    private void onMasterOnline(String masterAddr, String masterHaAddr) {
        boolean needSyncMasterFlushOffset = brokerMessageService.getMessageStore().getMasterFlushedOffset() == 0
            && this.messageStoreConfig.isSyncMasterFlushOffsetWhenStartup();
        if (masterHaAddr == null || needSyncMasterFlushOffset) {
            doSyncMasterFlushOffset(masterAddr, masterHaAddr, needSyncMasterFlushOffset);
        }

        // set master HA address.
        if (masterHaAddr != null) {
            brokerMessageService.getMessageStore().updateHaMasterAddress(masterHaAddr);
        }

        // wakeup HAClient
        brokerMessageService.getMessageStore().wakeupHAClient();
    }

    private void doSyncMasterFlushOffset(String masterAddr, String masterHaAddr, boolean needSyncMasterFlushOffset) {
        try {
            BrokerSyncInfo brokerSyncInfo = this.getBrokerOuterAPI().retrieveBrokerHaInfo(masterAddr);

            if (needSyncMasterFlushOffset) {
                LOG.info("Set master flush offset in slave to {}", brokerSyncInfo.getMasterFlushOffset());
                brokerMessageService.getMessageStore().setMasterFlushedOffset(brokerSyncInfo.getMasterFlushOffset());
            }

            if (masterHaAddr == null) {
                brokerMessageService.getMessageStore().updateHaMasterAddress(brokerSyncInfo.getMasterHaAddress());
                brokerMessageService.getMessageStore().updateMasterAddress(brokerSyncInfo.getMasterAddress());
            }
        } catch (Exception e) {
            LOG.error("retrieve master ha info exception, {}", e);
        }
    }

    private void onMinBrokerChange(long minBrokerId, String minBrokerAddr, String offlineBrokerAddr,
        String masterHaAddr) {
        LOG.info("Min broker changed, old: {}-{}, new {}-{}",
            this.minBrokerIdInGroup, this.minBrokerAddrInGroup, minBrokerId, minBrokerAddr);

        this.minBrokerIdInGroup = minBrokerId;
        this.minBrokerAddrInGroup = minBrokerAddr;

        this.brokerMessageService.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == this.minBrokerIdInGroup);

        if (offlineBrokerAddr != null && offlineBrokerAddr.equals(this.slaveSynchronize.getMasterAddr())) {
            // master offline
            onMasterOffline();
        }

        if (minBrokerId == MixAll.MASTER_ID && minBrokerAddr != null) {
            // master online
            onMasterOnline(minBrokerAddr, masterHaAddr);
        }

        // notify PullRequest on hold to pull from master.
        if (this.minBrokerIdInGroup == MixAll.MASTER_ID) {
            this.brokerNettyServer.getPullRequestHoldService().notifyMasterOnline();
        }
    }

    //**************************************** private or protected methods end   ****************************************************

    //**************************************** getter and setter start ****************************************************
    public BrokerServiceManager getBrokerServiceManager() {
        return brokerServiceManager;
    }

    public BrokerServiceRegistry getBrokerServiceRegistry() {
        return brokerServiceRegistry;
    }

    public BrokerMetadataManager getBrokerMetadataManager() {
        return brokerMetadataManager;
    }

    public BrokerMessageService getBrokerMessageService() {
        return brokerMessageService;
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public BrokerMetricsManager getBrokerMetricsManager() {
        return brokerMetricsManager;
    }

    public BrokerScheduleService getBrokerScheduleService() {
        return brokerScheduleService;
    }

    public BrokerStats getBrokerStats() {
        return brokerMessageService.getBrokerStats();
    }

    public boolean isUpdateMasterHAServerAddrPeriodically() {
        return updateMasterHAServerAddrPeriodically;
    }

    public void setUpdateMasterHAServerAddrPeriodically(boolean updateMasterHAServerAddrPeriodically) {
        this.updateMasterHAServerAddrPeriodically = updateMasterHAServerAddrPeriodically;
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
        return this.brokerMetadataManager.getConsumerFilterManager();
    }

    public ConsumerOrderInfoManager getConsumerOrderInfoManager() {
        return this.brokerMetadataManager.getConsumerOrderInfoManager();
    }

    public PopInflightMessageCounter getPopInflightMessageCounter() {
        return brokerServiceManager.getPopInflightMessageCounter();
    }

    public ConsumerOffsetManager getConsumerOffsetManager() {
        return this.brokerMetadataManager.getConsumerOffsetManager();
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
        this.brokerMetadataManager.setSubscriptionGroupManager(subscriptionGroupManager);
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return this.brokerMetadataManager.getSubscriptionGroupManager();
    }

    public TimerMessageStore getTimerMessageStore() {
        return brokerMessageService.getTimerMessageStore();
    }

    public String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
    }

    public TopicConfigManager getTopicConfigManager() {
        return this.brokerMetadataManager.getTopicConfigManager();
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.brokerMetadataManager.setTopicConfigManager(topicConfigManager);
    }

    public TopicQueueMappingManager getTopicQueueMappingManager() {
        return this.brokerMetadataManager.getTopicQueueMappingManager();
    }

    public String getHAServerAddr() {
        return this.brokerConfig.getBrokerIP2() + ":" + this.messageStoreConfig.getHaListenPort();
    }

    public RebalanceLockManager getRebalanceLockManager() {
        return rebalanceLockManager;
    }

    public SlaveSynchronize getSlaveSynchronize() {
        return slaveSynchronize;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
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

    public BrokerOuterAPI getBrokerOuterAPI() {
        return this.brokerServiceRegistry.getBrokerOuterAPI();
    }

    public void setBrokerOuterAPI(BrokerOuterAPI brokerOuterAPI) {
        this.brokerServiceRegistry.setBrokerOuterAPI(brokerOuterAPI);
    }

    public InetSocketAddress getStoreHost() {
        return storeHost;
    }

    public void setStoreHost(InetSocketAddress storeHost) {
        this.storeHost = storeHost;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public Map<Class, AccessValidator> getAccessValidatorMap() {
        return getBrokerNettyServer().getAccessValidatorMap();
    }

    public ShutdownHook getShutdownHook() {
        return shutdownHook;
    }

    public void setShutdownHook(ShutdownHook shutdownHook) {
        this.shutdownHook = shutdownHook;
    }

    public long getMinBrokerIdInGroup() {
        return this.brokerConfig.getBrokerId();
    }

    public BrokerController peekMasterBroker() {
        return brokerConfig.getBrokerId() == MixAll.MASTER_ID ? this : null;
    }

    public BrokerMemberGroup getBrokerMemberGroup() {
        return this.getBrokerScheduleService().getBrokerMemberGroup();
    }

    public int getListenPort() {
        return this.nettyServerConfig.getListenPort();
    }

    public List<BrokerAttachedPlugin> getBrokerAttachedPlugins() {
        return brokerAttachedPlugins;
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

    public ReplicasManager getReplicasManager() {
        return replicasManager;
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

    public ColdDataPullRequestHoldService getColdDataPullRequestHoldService() {
        return brokerServiceManager.getColdDataPullRequestHoldService();
    }

    public ColdDataCgCtrService getColdDataCgCtrService() {
        return brokerServiceManager.getColdDataCgCtrService();
    }

    public BrokerNettyServer getBrokerNettyServer() {
        return this.brokerNettyServer;
    }
    //**************************************** getter and setter end ****************************************************

}
