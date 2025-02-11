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

import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.infra.ClusterClient;
import org.apache.rocketmq.common.app.AbstractBrokerRunnable;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;

public class InnerBrokerController extends Broker {
    protected BrokerContainer brokerContainer;

    public InnerBrokerController(
        final BrokerContainer brokerContainer,
        final BrokerConfig brokerConfig,
        final MessageStoreConfig messageStoreConfig
    ) {
        super(brokerConfig, messageStoreConfig);
        this.brokerContainer = brokerContainer;
        super.setBrokerOuterAPI(this.brokerContainer.getBrokerOuterAPI());
    }

    @Override
    protected void initializeRemotingServer() {
        setRemotingServer(this.brokerContainer.getRemotingServer().newRemotingServer(brokerConfig.getListenPort()));
        setFastRemotingServer(this.brokerContainer.getRemotingServer().newRemotingServer(brokerConfig.getListenPort() - 2));
    }

    @Override
    protected void initializeScheduledTasks() {
        getBrokerScheduleService().initializeBrokerScheduledTasks();
    }

    @Override
    public void start() throws Exception {
        this.shouldStartTime = System.currentTimeMillis() + messageStoreConfig.getDisappearTimeAfterStart();

        if (messageStoreConfig.getTotalReplicas() > 1 && this.brokerConfig.isEnableSlaveActingMaster()) {
            isIsolated = true;
        }

        startBasicService();

        if (!isIsolated && !this.messageStoreConfig.isEnableDLegerCommitLog() && !this.messageStoreConfig.isDuplicationEnable()) {
            this.getBrokerMessageService().changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == MQConstants.MASTER_ID);
            this.getBrokerServiceRegistry().registerBrokerAll(true, false, true);
        }

        getBrokerScheduleService().getScheduledFutures().add(getBrokerScheduleService().getScheduledExecutorService().scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
            @Override
            public void run0() {
                try {
                    if (System.currentTimeMillis() < shouldStartTime) {
                        Broker.LOG.info("Register to namesrv after {}", shouldStartTime);
                        return;
                    }
                    if (isIsolated) {
                        Broker.LOG.info("Skip register for broker is isolated");
                        return;
                    }
                    InnerBrokerController.this.getBrokerServiceRegistry().registerBrokerAll(true, false, brokerConfig.isForceRegister());
                } catch (Throwable e) {
                    Broker.LOG.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, Math.max(10000, Math.min(brokerConfig.getRegisterNameServerPeriod(), 60000)), TimeUnit.MILLISECONDS));

        if (this.brokerConfig.isEnableSlaveActingMaster()) {
            getBrokerScheduleService().scheduleSendHeartbeat();

            getBrokerScheduleService().getScheduledFutures().add(getBrokerScheduleService().getSyncBrokerMemberGroupExecutorService().scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
                @Override
                public void run0() {
                    try {
                        InnerBrokerController.this.getBrokerScheduleService().syncBrokerMemberGroup();
                    } catch (Throwable e) {
                        Broker.LOG.error("sync BrokerMemberGroup error. ", e);
                    }
                }
            }, 1000, this.brokerConfig.getSyncBrokerMemberGroupPeriod(), TimeUnit.MILLISECONDS));
        }

        if (this.brokerConfig.isEnableControllerMode()) {
            getBrokerScheduleService().scheduleSendHeartbeat();
        }

        if (brokerConfig.isSkipPreOnline()) {
            registerBroker();
        }
    }

    @Override
    public void shutdown() {
        shutdownBasicService();

        if (this.getRemotingServer() != null) {
            this.brokerContainer.getRemotingServer().removeRemotingServer(brokerConfig.getListenPort());
        }

        if (this.getFastRemotingServer() != null) {
            this.brokerContainer.getRemotingServer().removeRemotingServer(brokerConfig.getListenPort() - 2);
        }
    }

    @Override
    public String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.brokerConfig.getListenPort();
    }

    @Override
    public String getHAServerAddr() {
        return this.brokerConfig.getBrokerIP2() + ":" + this.messageStoreConfig.getHaListenPort();
    }

    @Override
    public long getMinBrokerIdInGroup() {
        return this.getBrokerClusterService().getMinBrokerIdInGroup();
    }

    public ClusterClient getClusterClient() {
        return brokerContainer == null ? super.getClusterClient() : brokerContainer.getBrokerOuterAPI();
    }

    public BrokerContainer getBrokerContainer() {
        return this.brokerContainer;
    }

    public NettyServerConfig getNettyServerConfig() {
        return brokerContainer.getNettyServerConfig();
    }

    public NettyClientConfig getNettyClientConfig() {
        return brokerContainer == null ? super.getNettyClientConfig() : brokerContainer.getNettyClientConfig();
    }

    public MessageStore getMessageStoreByBrokerName(String brokerName) {
        if (this.brokerConfig.getBrokerName().equals(brokerName)) {
            return this.getMessageStore();
        }
        Broker broker = this.brokerContainer.findBrokerControllerByBrokerName(brokerName);
        if (broker != null) {
            return broker.getMessageStore();
        }
        return null;
    }

    @Override
    public Broker peekMasterBroker() {
        if (brokerConfig.getBrokerId() == MQConstants.MASTER_ID) {
            return this;
        }
        return this.brokerContainer.peekMasterBroker();
    }
}
