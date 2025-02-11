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
package org.apache.rocketmq.broker.server.daemon;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.common.app.AbstractBrokerRunnable;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.lang.thread.ThreadFactoryImpl;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.app.stats.MomentStatsItem;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.common.utils.TimeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.server.config.BrokerRole;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;

public class BrokerScheduleService {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final Logger LOG_PROTECTION = LoggerFactory.getLogger(LoggerName.PROTECTION_LOGGER_NAME);
    private static final int HA_ADDRESS_MIN_LENGTH = 6;

    private long lastSyncTimeMs = System.currentTimeMillis();
    protected final List<ScheduledFuture<?>> scheduledFutures = new ArrayList<>();

    protected volatile BrokerMemberGroup brokerMemberGroup;
    private final BrokerConfig brokerConfig;
    private final MessageStoreConfig messageStoreConfig;
    private final Broker broker;

    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledExecutorService syncBrokerMemberGroupExecutorService;
    private ScheduledExecutorService brokerHeartbeatExecutorService;

    public BrokerScheduleService(BrokerConfig brokerConfig, MessageStoreConfig messageStoreConfig, Broker broker) {
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.broker = broker;

        this.brokerMemberGroup = new BrokerMemberGroup(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName());
        this.brokerMemberGroup.getBrokerAddrs().put(this.brokerConfig.getBrokerId(), broker.getBrokerAddr());

        initPoolExecutors();
    }

    public void refreshMetadata() {
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                getBrokerController().getClusterClient().refreshMetadata();
            } catch (Exception e) {
                LOG.error("ScheduledTask refresh metadata exception", e);
            }
        }, 10, 5, TimeUnit.SECONDS);
    }

    public void init() {
        initializeBrokerScheduledTasks();

        if (this.brokerConfig.getNamesrvAddr() != null) {
            updateNamesrvAddr();
            LOG.info("Set user specified name server address: {}", this.brokerConfig.getNamesrvAddr());
            // also auto update namesrv if specify
            this.scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    updateNamesrvAddr();
                } catch (Throwable e) {
                    LOG.error("Failed to update nameServer address list", e);
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        } else if (this.brokerConfig.isFetchNamesrvAddrByAddressServer()) {
            this.scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    getBrokerController().getClusterClient().fetchNameServerAddr();
                } catch (Throwable e) {
                    LOG.error("Failed to fetch nameServer address", e);
                }
            }, 1000 * 10, this.brokerConfig.getFetchNamesrvAddrInterval(), TimeUnit.MILLISECONDS);
        }
    }

    public void start() {
        scheduleRegisterBrokerAll();
        scheduleSlaveActingMaster();

        if (this.brokerConfig.isEnableControllerMode()) {
            scheduleSendHeartbeat();
        }
    }

    public void shutdown() {
        for (ScheduledFuture<?> scheduledFuture : scheduledFutures) {
            scheduledFuture.cancel(true);
        }

        shutdownScheduledExecutorService(this.scheduledExecutorService);
        shutdownScheduledExecutorService(this.syncBrokerMemberGroupExecutorService);
        shutdownScheduledExecutorService(this.brokerHeartbeatExecutorService);
    }

    public void sendHeartbeat() {
        if (this.brokerConfig.isEnableControllerMode()) {
            getBrokerController().getBrokerClusterService().getReplicasManager().sendHeartbeatToController();
        }

        if (this.brokerConfig.isEnableSlaveActingMaster()) {
            if (this.brokerConfig.isCompatibleWithOldNameSrv()) {
                getBrokerController().getClusterClient().sendHeartbeatViaDataVersion(
                    this.brokerConfig.getBrokerClusterName(),
                    getBrokerController().getBrokerAddr(),
                    this.brokerConfig.getBrokerName(),
                    this.brokerConfig.getBrokerId(),
                    this.brokerConfig.getSendHeartbeatTimeoutMillis(),
                    getBrokerController().getTopicConfigManager().getDataVersion(),
                    this.brokerConfig.isInBrokerContainer());
            } else {
                getBrokerController().getClusterClient().sendHeartbeat(
                    this.brokerConfig.getBrokerClusterName(),
                    getBrokerController().getBrokerAddr(),
                    this.brokerConfig.getBrokerName(),
                    this.brokerConfig.getBrokerId(),
                    this.brokerConfig.getSendHeartbeatTimeoutMillis(),
                    this.brokerConfig.isInBrokerContainer());
            }
        }
    }

    public void shutdownScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        if (scheduledExecutorService == null) {
            return;
        }
        scheduledExecutorService.shutdown();
        try {
            scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.warn("shutdown ScheduledExecutorService was Interrupted!  ", e);
            Thread.currentThread().interrupt();
        }
    }

    private void scheduleRegisterBrokerAll() {
        scheduledFutures.add(this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(getBrokerController().getBrokerIdentity()) {
            @Override
            public void run0() {
                try {
                    if (System.currentTimeMillis() < getBrokerController().getShouldStartTime()) {
                        LOG.info("Register to namesrv after {}", getBrokerController().getShouldStartTime());
                        return;
                    }
                    if (getBrokerController().isIsolated()) {
                        LOG.info("Skip register for broker is isolated");
                        return;
                    }
                    getBrokerController().getBrokerServiceRegistry().registerBrokerAll(true, false, brokerConfig.isForceRegister());
                } catch (Throwable e) {
                    LOG.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, Math.max(10000, Math.min(brokerConfig.getRegisterNameServerPeriod(), 60000)), TimeUnit.MILLISECONDS));
    }

    public void scheduleSlaveActingMaster() {
        if (!this.brokerConfig.isEnableSlaveActingMaster()) {
            return;
        }

        scheduleSendHeartbeat();

        scheduledFutures.add(this.syncBrokerMemberGroupExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(getBrokerController().getBrokerIdentity()) {
            @Override
            public void run0() {
                try {
                    syncBrokerMemberGroup();
                } catch (Throwable e) {
                    LOG.error("sync BrokerMemberGroup error. ", e);
                }
            }
        }, 1000, this.brokerConfig.getSyncBrokerMemberGroupPeriod(), TimeUnit.MILLISECONDS));
    }

    public void syncBrokerMemberGroup() {
        try {
            brokerMemberGroup = broker.getClusterClient()
                .syncBrokerMemberGroup(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName(), this.brokerConfig.isCompatibleWithOldNameSrv());
        } catch (Exception e) {
            LOG.error("syncBrokerMemberGroup from namesrv failed, ", e);
            return;
        }
        if (brokerMemberGroup == null || brokerMemberGroup.getBrokerAddrs().size() == 0) {
            LOG.warn("Couldn't find any broker member from namesrv in {}/{}", this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName());
            return;
        }
        broker.getMessageStore().setAliveReplicaNumInGroup(calcAliveBrokerNumInGroup(brokerMemberGroup.getBrokerAddrs()));

        if (!broker.isIsolated()) {
            long minBrokerId = brokerMemberGroup.minimumBrokerId();
            broker.getBrokerClusterService().updateMinBroker(minBrokerId, brokerMemberGroup.getBrokerAddrs().get(minBrokerId));
        }
    }

    public void scheduleSendHeartbeat() {
        scheduledFutures.add(this.brokerHeartbeatExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(getBrokerController().getBrokerIdentity()) {
            @Override
            public void run0() {
                if (getBrokerController().isIsolated()) {
                    return;
                }
                try {
                    sendHeartbeat();
                } catch (Exception e) {
                    LOG.error("sendHeartbeat Exception", e);
                }

            }
        }, 1000, brokerConfig.getBrokerHeartbeatInterval(), TimeUnit.MILLISECONDS));
    }

    public void initializeBrokerScheduledTasks() {
        final long initialDelay = TimeUtils.computeNextMorningTimeMillis() - System.currentTimeMillis();
        final long period = TimeUnit.DAYS.toMillis(1);
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                getBrokerController().getBrokerServiceManager().getBrokerStats().record();
            } catch (Throwable e) {
                LOG.error("BrokerController: failed to record broker stats", e);
            }
        }, initialDelay, period, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                getBrokerController().getConsumerOffsetManager().persist();
            } catch (Throwable e) {
                LOG.error(
                    "BrokerController: failed to persist config file of consumerOffset", e);
            }
        }, 1000 * 10, this.brokerConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                getBrokerController().getConsumerFilterManager().persist();
                getBrokerController().getConsumerOrderInfoManager().persist();
            } catch (Throwable e) {
                LOG.error(
                    "BrokerController: failed to persist config file of consumerFilter or consumerOrderInfo",
                    e);
            }
        }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                protectBroker();
            } catch (Throwable e) {
                LOG.error("BrokerController: failed to protectBroker", e);
            }
        }, 3, 3, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                getBrokerController().getBrokerNettyServer().printWaterMark();
            } catch (Throwable e) {
                LOG.error("BrokerController: failed to print broker watermark", e);
            }
        }, 10, 1, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                LOG.info("Dispatch task fall behind commit log {}bytes",
                    getBrokerController().getMessageStore().dispatchBehindBytes());
            } catch (Throwable e) {
                LOG.error("Failed to print dispatchBehindBytes", e);
            }
        }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);

        if (!messageStoreConfig.isEnableDLegerCommitLog() && !messageStoreConfig.isDuplicationEnable() && !brokerConfig.isEnableControllerMode()) {
            if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
                if (this.messageStoreConfig.getHaMasterAddress() != null && this.messageStoreConfig.getHaMasterAddress().length() >= HA_ADDRESS_MIN_LENGTH) {
                    this.getBrokerController().getMessageStore().updateHaMasterAddress(this.messageStoreConfig.getHaMasterAddress());
                    getBrokerController().getBrokerClusterService().setUpdateMasterHAServerAddrPeriodically(false);
                } else {
                    getBrokerController().getBrokerClusterService().setUpdateMasterHAServerAddrPeriodically(true);
                }

                this.scheduledExecutorService.scheduleAtFixedRate(() -> {
                    try {
                        if (System.currentTimeMillis() - lastSyncTimeMs > 60 * 1000) {
                            getBrokerController().getBrokerClusterService().getSlaveSynchronize().syncAll();
                            lastSyncTimeMs = System.currentTimeMillis();
                        }

                        //timer checkpoint, latency-sensitive, so sync it more frequently
                        if (messageStoreConfig.isTimerWheelEnable()) {
                            getBrokerController().getBrokerClusterService().getSlaveSynchronize().syncTimerCheckPoint();
                        }
                    } catch (Throwable e) {
                        LOG.error("Failed to sync all config for slave.", e);
                    }
                }, 1000 * 10, 3 * 1000, TimeUnit.MILLISECONDS);

            } else {
                this.scheduledExecutorService.scheduleAtFixedRate(() -> {
                    try {
                        printMasterAndSlaveDiff();
                    } catch (Throwable e) {
                        LOG.error("Failed to print diff of master and slave.", e);
                    }
                }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
            }
        }

        if (this.brokerConfig.isEnableControllerMode()) {
            getBrokerController().getBrokerClusterService().setUpdateMasterHAServerAddrPeriodically(true);
        }
    }

    public void protectBroker() {
        if (!this.brokerConfig.isDisableConsumeIfConsumerReadSlowly()) {
            return;
        }

        for (Map.Entry<String, MomentStatsItem> next : broker.getBrokerStatsManager().getMomentStatsItemSetFallSize().getStatsItemTable().entrySet()) {
            final long fallBehindBytes = next.getValue().getValue().get();
            if (fallBehindBytes <= this.brokerConfig.getConsumerFallbehindThreshold()) {
                continue;
            }

            final String[] split = next.getValue().getStatsKey().split("@");
            final String group = split[2];
            LOG_PROTECTION.info("[PROTECT_BROKER] the consumer[{}] consume slowly, {} bytes, disable it", group, fallBehindBytes);
            broker.getBrokerMetadataManager().getSubscriptionGroupManager().disableConsume(group);
        }
    }

    private int calcAliveBrokerNumInGroup(Map<Long, String> brokerAddrTable) {
        if (brokerAddrTable.containsKey(this.brokerConfig.getBrokerId())) {
            return brokerAddrTable.size();
        } else {
            return brokerAddrTable.size() + 1;
        }
    }

    public void updateNamesrvAddr() {
        if (this.brokerConfig.isFetchNameSrvAddrByDnsLookup()) {
            getBrokerController().getClusterClient().updateNameServerAddressListByDnsLookup(this.brokerConfig.getNamesrvAddr());
        } else {
            getBrokerController().getClusterClient().updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
        }
    }

    private void printMasterAndSlaveDiff() {
        MessageStore messageStore = getBrokerController().getMessageStore();
        if (messageStore.getHaService() != null && messageStore.getHaService().getConnectionCount().get() > 0) {
            long diff = messageStore.slaveFallBehindMuch();
            LOG.info("CommitLog: slave fall behind master {}bytes", diff);
        }
    }

    private void initPoolExecutors() {
        this.scheduledExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new ThreadFactoryImpl("BrokerControllerScheduledThread", true, getBrokerController().getBrokerIdentity()));

        this.syncBrokerMemberGroupExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new ThreadFactoryImpl("BrokerControllerSyncBrokerScheduledThread", getBrokerController().getBrokerIdentity()));
        this.brokerHeartbeatExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new ThreadFactoryImpl("BrokerControllerHeartbeatScheduledThread", getBrokerController().getBrokerIdentity()));
    }


    public ScheduledExecutorService getSyncBrokerMemberGroupExecutorService() {
        return syncBrokerMemberGroupExecutorService;
    }

    public BrokerMemberGroup getBrokerMemberGroup() {
        return brokerMemberGroup;
    }

    public List<ScheduledFuture<?>> getScheduledFutures() {
        return scheduledFutures;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public Broker getBrokerController() {
        return broker;
    }

}
