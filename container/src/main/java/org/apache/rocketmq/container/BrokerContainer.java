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
package org.apache.rocketmq.container;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.common.app.config.BrokerPathConfigHelper;
import org.apache.rocketmq.broker.infra.ClusterClient;
import org.apache.rocketmq.common.app.AbstractBrokerRunnable;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.app.BrokerIdentity;
import org.apache.rocketmq.common.lang.thread.ThreadFactoryImpl;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.container.logback.BrokerLogbackConfigurator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.server.config.BrokerRole;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BrokerContainer implements IBrokerContainer {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final ScheduledExecutorService scheduledExecutorService = ThreadUtils.newScheduledThreadPool(1,
        new BasicThreadFactory.Builder()
            .namingPattern("BrokerContainerScheduledThread")
            .daemon(true)
            .build());
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    private final ClusterClient clusterClient;
    private final ContainerClientHouseKeepingService containerClientHouseKeepingService;

    private final ConcurrentMap<BrokerIdentity, InnerSalveBrokerController> slaveBrokerControllers = new ConcurrentHashMap<>();
    private final ConcurrentMap<BrokerIdentity, InnerBrokerController> masterBrokerControllers = new ConcurrentHashMap<>();
    private final ConcurrentMap<BrokerIdentity, InnerBrokerController> dLedgerBrokerControllers = new ConcurrentHashMap<>();
    private final List<BrokerBootHook> brokerBootHookList = new ArrayList<>();
    private final BrokerContainerProcessor brokerContainerProcessor;
    private final Configuration configuration;
    private final BrokerContainerConfig brokerContainerConfig;

    private RemotingServer remotingServer;
    private RemotingServer fastRemotingServer;
    private ExecutorService brokerContainerExecutor;

    public BrokerContainer(
        final BrokerContainerConfig brokerContainerConfig,
        final NettyServerConfig nettyServerConfig,
        final NettyClientConfig nettyClientConfig
    ) {
        this.brokerContainerConfig = brokerContainerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;

        this.clusterClient = new ClusterClient(nettyClientConfig, null);

        this.brokerContainerProcessor = new BrokerContainerProcessor(this);
        this.brokerContainerProcessor.registerBrokerBootHook(this.brokerBootHookList);
        this.containerClientHouseKeepingService = new ContainerClientHouseKeepingService(this);

        this.configuration = new Configuration(
            LOG,
            BrokerPathConfigHelper.getBrokerConfigPath(),
            this.brokerContainerConfig, this.nettyServerConfig, this.nettyClientConfig);
    }


    private void updateNamesrvAddr() {
        if (this.brokerContainerConfig.isFetchNameSrvAddrByDnsLookup()) {
            this.clusterClient.updateNameServerAddressListByDnsLookup(this.brokerContainerConfig.getNamesrvAddr());
        } else {
            this.clusterClient.updateNameServerAddressList(this.brokerContainerConfig.getNamesrvAddr());
        }
    }

    public boolean initialize() {
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.containerClientHouseKeepingService);
        this.fastRemotingServer = this.remotingServer.newRemotingServer(this.nettyServerConfig.getListenPort() - 2);

        this.brokerContainerExecutor = ThreadUtils.newThreadPoolExecutor(
            1,
            1,
            1000 * 60,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(10000),
            new ThreadFactoryImpl("SharedBrokerThread_"));

        this.registerProcessor();

        if (this.brokerContainerConfig.getNamesrvAddr() != null) {
            this.updateNamesrvAddr();
            LOG.info("Set user specified name server address: {}", this.brokerContainerConfig.getNamesrvAddr());
            // also auto update namesrv if specify
            this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(BrokerIdentity.BROKER_CONTAINER_IDENTITY) {
                @Override
                public void run0() {
                    try {
                        BrokerContainer.this.updateNamesrvAddr();
                    } catch (Throwable e) {
                        LOG.error("ScheduledTask fetchNameServerAddr exception", e);
                    }
                }
            }, 1000 * 10, this.brokerContainerConfig.getUpdateNamesrvAddrInterval(), TimeUnit.MILLISECONDS);
        } else if (this.brokerContainerConfig.isFetchNamesrvAddrByAddressServer()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(BrokerIdentity.BROKER_CONTAINER_IDENTITY) {

                @Override
                public void run0() {
                    try {
                        BrokerContainer.this.clusterClient.fetchNameServerAddr();
                    } catch (Throwable e) {
                        LOG.error("ScheduledTask fetchNameServerAddr exception", e);
                    }
                }
            }, 1000 * 10, this.brokerContainerConfig.getFetchNamesrvAddrInterval(), TimeUnit.MILLISECONDS);
        }

        this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(BrokerIdentity.BROKER_CONTAINER_IDENTITY) {
            @Override
            public void run0() {
                try {
                    BrokerContainer.this.clusterClient.refreshMetadata();
                } catch (Exception e) {
                    LOG.error("ScheduledTask refresh metadata exception", e);
                }
            }
        }, 10, 5, TimeUnit.SECONDS);

        return true;
    }

    private void registerProcessor() {
        remotingServer.registerDefaultProcessor(brokerContainerProcessor, this.brokerContainerExecutor);
        fastRemotingServer.registerDefaultProcessor(brokerContainerProcessor, this.brokerContainerExecutor);
    }

    @Override
    public void start() throws Exception {
        if (this.remotingServer != null) {
            this.remotingServer.start();
        }

        if (this.fastRemotingServer != null) {
            this.fastRemotingServer.start();
        }

        if (this.clusterClient != null) {
            this.clusterClient.start();
        }
    }

    @Override
    public void shutdown() {
        // Shutdown slave brokers
        for (InnerSalveBrokerController slaveBrokerController : slaveBrokerControllers.values()) {
            slaveBrokerController.shutdown();
        }

        slaveBrokerControllers.clear();

        // Shutdown master brokers
        for (Broker masterBroker : masterBrokerControllers.values()) {
            masterBroker.shutdown();
        }

        masterBrokerControllers.clear();

        // Shutdown dLedger brokers
        dLedgerBrokerControllers.values().forEach(InnerBrokerController::shutdown);
        dLedgerBrokerControllers.clear();

        // Shutdown the remoting server with a high priority to avoid further traffic
        if (this.remotingServer != null) {
            this.remotingServer.shutdown();
        }

        if (this.fastRemotingServer != null) {
            this.fastRemotingServer.shutdown();
        }

        // Shutdown the request executors
        ThreadUtils.shutdown(this.brokerContainerExecutor);

        if (this.clusterClient != null) {
            this.clusterClient.shutdown();
        }
    }

    public void registerClientRPCHook(RPCHook rpcHook) {
        this.getBrokerOuterAPI().registerRPCHook(rpcHook);
    }

    public void clearClientRPCHook() {
        this.getBrokerOuterAPI().clearRPCHook();
    }

    public List<BrokerBootHook> getBrokerBootHookList() {
        return brokerBootHookList;
    }

    public void registerBrokerBootHook(BrokerBootHook brokerBootHook) {
        this.brokerBootHookList.add(brokerBootHook);
        LOG.info("register BrokerBootHook, {}", brokerBootHook.hookName());
    }

    @Override
    public InnerBrokerController addBroker(final BrokerConfig brokerConfig,
        final MessageStoreConfig storeConfig) throws Exception {
        if (storeConfig.isEnableDLegerCommitLog()) {
            return this.addDLedgerBroker(brokerConfig, storeConfig);
        }

        if (brokerConfig.getBrokerId() == MQConstants.MASTER_ID && storeConfig.getBrokerRole() != BrokerRole.SLAVE) {
            return this.addMasterBroker(brokerConfig, storeConfig);
        }
        if (brokerConfig.getBrokerId() != MQConstants.MASTER_ID && storeConfig.getBrokerRole() == BrokerRole.SLAVE) {
            return this.addSlaveBroker(brokerConfig, storeConfig);
        }

        return null;
    }

    public InnerBrokerController addDLedgerBroker(final BrokerConfig brokerConfig, final MessageStoreConfig storeConfig) throws Exception {
        brokerConfig.setInBrokerContainer(true);
        if (storeConfig.isDuplicationEnable()) {
            LOG.error("Can not add broker to container when duplicationEnable is true currently");
            throw new Exception("Can not add broker to container when duplicationEnable is true currently");
        }
        InnerBrokerController brokerController = new InnerBrokerController(this, brokerConfig, storeConfig);
        BrokerIdentity brokerIdentity = brokerController.getBrokerIdentity();
        final Broker previousBroker = dLedgerBrokerControllers.putIfAbsent(brokerIdentity, brokerController);
        if (previousBroker != null) {
            throw new Exception(brokerIdentity.getCanonicalName() + " has already been added to current broker container");
        }

        // New dLedger broker added, start it
        try {
            BrokerLogbackConfigurator.doConfigure(brokerIdentity);
            final boolean initResult = brokerController.initialize();
            if (!initResult) {
                dLedgerBrokerControllers.remove(brokerIdentity);
                brokerController.shutdown();
                throw new Exception("Failed to init dLedger broker " + brokerIdentity.getCanonicalName());
            }
        } catch (Exception e) {
            // Remove the failed dLedger broker and throw the exception
            dLedgerBrokerControllers.remove(brokerIdentity);
            brokerController.shutdown();
            throw new Exception("Failed to initialize dLedger broker " + brokerIdentity.getCanonicalName(), e);
        }
        return brokerController;
    }

    public InnerBrokerController addMasterBroker(final BrokerConfig masterBrokerConfig,
        final MessageStoreConfig storeConfig) throws Exception {

        masterBrokerConfig.setInBrokerContainer(true);
        if (storeConfig.isDuplicationEnable()) {
            LOG.error("Can not add broker to container when duplicationEnable is true currently");
            throw new Exception("Can not add broker to container when duplicationEnable is true currently");
        }
        InnerBrokerController masterBroker = new InnerBrokerController(this, masterBrokerConfig, storeConfig);
        BrokerIdentity brokerIdentity = masterBroker.getBrokerIdentity();
        final Broker previousBroker = masterBrokerControllers.putIfAbsent(brokerIdentity, masterBroker);
        if (previousBroker != null) {
            throw new Exception(masterBrokerConfig.getCanonicalName() + " has already been added to current broker container");
        }

        // New master broker added, start it
        try {
            BrokerLogbackConfigurator.doConfigure(masterBrokerConfig);
            final boolean initResult = masterBroker.initialize();
            if (!initResult) {
                masterBrokerControllers.remove(brokerIdentity);
                masterBroker.shutdown();
                throw new Exception("Failed to init master broker " + masterBrokerConfig.getCanonicalName());
            }

            for (InnerSalveBrokerController slaveBroker : this.getSlaveBrokers()) {
                if (slaveBroker.getMessageStore().getMasterStoreInProcess() == null) {
                    slaveBroker.getMessageStore().setMasterStoreInProcess(masterBroker.getMessageStore());
                }
            }
        } catch (Exception e) {
            // Remove the failed master broker and throw the exception
            masterBrokerControllers.remove(brokerIdentity);
            masterBroker.shutdown();
            throw new Exception("Failed to initialize master broker " + masterBrokerConfig.getCanonicalName(), e);
        }
        return masterBroker;
    }

    /**
     * This function will create a slave broker along with the main broker, and start it with a different port.
     *
     * @param slaveBrokerConfig the specific slave broker config
     * @throws Exception is thrown if an error occurs
     */
    public InnerSalveBrokerController addSlaveBroker(final BrokerConfig slaveBrokerConfig,
        final MessageStoreConfig storeConfig) throws Exception {

        slaveBrokerConfig.setInBrokerContainer(true);
        if (storeConfig.isDuplicationEnable()) {
            LOG.error("Can not add broker to container when duplicationEnable is true currently");
            throw new Exception("Can not add broker to container when duplicationEnable is true currently");
        }

        int ratio = storeConfig.getAccessMessageInMemoryMaxRatio() - 10;
        storeConfig.setAccessMessageInMemoryMaxRatio(Math.max(ratio, 0));
        InnerSalveBrokerController slaveBroker = new InnerSalveBrokerController(this, slaveBrokerConfig, storeConfig);
        BrokerIdentity brokerIdentity = slaveBroker.getBrokerIdentity();
        final InnerSalveBrokerController previousBroker = slaveBrokerControllers.putIfAbsent(brokerIdentity, slaveBroker);
        if (previousBroker != null) {
            throw new Exception(slaveBrokerConfig.getCanonicalName() + " has already been added to current broker container");
        }

        // New slave broker added, start it
        try {
            BrokerLogbackConfigurator.doConfigure(slaveBrokerConfig);
            final boolean initResult = slaveBroker.initialize();
            if (!initResult) {
                slaveBrokerControllers.remove(brokerIdentity);
                slaveBroker.shutdown();
                throw new Exception("Failed to init slave broker " + slaveBrokerConfig.getCanonicalName());
            }
            Broker masterBroker = this.peekMasterBroker();
            if (slaveBroker.getMessageStore().getMasterStoreInProcess() == null && masterBroker != null) {
                slaveBroker.getMessageStore().setMasterStoreInProcess(masterBroker.getMessageStore());
            }
        } catch (Exception e) {
            // Remove the failed slave broker and throw the exception
            slaveBrokerControllers.remove(brokerIdentity);
            slaveBroker.shutdown();
            throw new Exception("Failed to initialize slave broker " + slaveBrokerConfig.getCanonicalName(), e);
        }
        return slaveBroker;
    }

    @Override
    public Broker removeBroker(final BrokerIdentity brokerIdentity) throws Exception {

        InnerBrokerController dLedgerController = dLedgerBrokerControllers.remove(brokerIdentity);
        if (dLedgerController != null) {
            dLedgerController.shutdown();
            return dLedgerController;
        }

        InnerSalveBrokerController slaveBroker = slaveBrokerControllers.remove(brokerIdentity);
        if (slaveBroker != null) {
            slaveBroker.shutdown();
            return slaveBroker;
        }

        Broker masterBroker = masterBrokerControllers.remove(brokerIdentity);

        Broker nextMasterBroker = this.peekMasterBroker();
        for (InnerSalveBrokerController slave : this.getSlaveBrokers()) {
            if (nextMasterBroker == null) {
                slave.getMessageStore().setMasterStoreInProcess(null);
            } else {
                slave.getMessageStore().setMasterStoreInProcess(nextMasterBroker.getMessageStore());
            }

        }

        if (masterBroker != null) {
            masterBroker.shutdown();
            return masterBroker;
        }

        return null;
    }

    @Override
    public Broker getBroker(final BrokerIdentity brokerIdentity) {
        InnerSalveBrokerController slaveBroker = slaveBrokerControllers.get(brokerIdentity);
        if (slaveBroker != null) {
            return slaveBroker;
        }

        return masterBrokerControllers.get(brokerIdentity);
    }

    @Override
    public Collection<InnerBrokerController> getMasterBrokers() {
        return masterBrokerControllers.values();
    }

    @Override
    public Collection<InnerSalveBrokerController> getSlaveBrokers() {
        return slaveBrokerControllers.values();
    }

    @Override
    public List<Broker> getBrokerControllers() {
        List<Broker> brokers = new ArrayList<>();
        brokers.addAll(this.getMasterBrokers());
        brokers.addAll(this.getSlaveBrokers());
        return brokers;
    }

    @Override
    public Broker peekMasterBroker() {
        if (!masterBrokerControllers.isEmpty()) {
            return masterBrokerControllers.values().iterator().next();
        }
        return null;
    }

    public Broker findBrokerControllerByBrokerName(String brokerName) {
        for (Broker broker : masterBrokerControllers.values()) {
            if (broker.getBrokerConfig().getBrokerName().equals(brokerName)) {
                return broker;
            }
        }

        for (Broker broker : slaveBrokerControllers.values()) {
            if (broker.getBrokerConfig().getBrokerName().equals(brokerName)) {
                return broker;
            }
        }
        return null;
    }

    @Override
    public String getBrokerContainerAddr() {
        return this.brokerContainerConfig.getBrokerContainerIP() + ":" + this.nettyServerConfig.getListenPort();
    }

    @Override
    public BrokerContainerConfig getBrokerContainerConfig() {
        return brokerContainerConfig;
    }

    @Override
    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    @Override
    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    @Override
    public ClusterClient getBrokerOuterAPI() {
        return clusterClient;
    }

    @Override
    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

}
