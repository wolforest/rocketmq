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
package org.apache.rocketmq.broker.server.bootstrap;

import com.google.common.collect.Lists;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.infra.ClusterClient;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.constant.PermName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;

/**
 * nameserver related operations
 * register & unregister
 */
public class BrokerServiceRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerConfig brokerConfig;
    private final Broker broker;
    private ClusterClient clusterClient;

    public BrokerServiceRegistry(Broker broker) {
        this.broker = broker;
        this.brokerConfig = broker.getBrokerConfig();
        NettyClientConfig nettyClientConfig = broker.getNettyClientConfig();

        if (null == nettyClientConfig) {
            LOG.info("nettyClientConfig is null,brokerOuterAPI does not need to be started");
            return;
        }

        this.clusterClient = new ClusterClient(nettyClientConfig, broker.getAuthConfig());
    }

    public void start() {
        if (this.clusterClient != null) {
            this.clusterClient.start();
        }
    }

    public void shutdown() {
        if (this.clusterClient != null) {
            this.clusterClient.shutdown();
        }
    }

    public synchronized void registerSingleTopicAll(final TopicConfig topicConfig) {
        TopicConfig tmpTopic = topicConfig;
        if (!PermName.isWriteable(this.brokerConfig.getBrokerPermission())
            || !PermName.isReadable(this.brokerConfig.getBrokerPermission())) {
            // Copy the topic config and modify the perm
            tmpTopic = new TopicConfig(topicConfig);
            tmpTopic.setPerm(topicConfig.getPerm() & this.brokerConfig.getBrokerPermission());
        }
        this.clusterClient.registerSingleTopicAll(this.brokerConfig.getBrokerName(), tmpTopic, 3000);
    }

    public synchronized void registerIncrementBrokerData(TopicConfig topicConfig, DataVersion dataVersion) {
        this.registerIncrementBrokerData(Collections.singletonList(topicConfig), dataVersion);
    }

    public synchronized void registerIncrementBrokerData(List<TopicConfig> topicConfigList, DataVersion dataVersion) {
        if (topicConfigList == null || topicConfigList.isEmpty()) {
            return;
        }

        TopicConfigAndMappingSerializeWrapper topicConfigSerializeWrapper = new TopicConfigAndMappingSerializeWrapper();
        topicConfigSerializeWrapper.setDataVersion(dataVersion);

        ConcurrentMap<String, TopicConfig> topicConfigTable = topicConfigList.stream()
            .map(topicConfig -> {
                TopicConfig registerTopicConfig;
                if (!PermName.isWriteable(this.brokerConfig.getBrokerPermission())
                    || !PermName.isReadable(this.brokerConfig.getBrokerPermission())) {
                    registerTopicConfig =
                        new TopicConfig(topicConfig.getTopicName(),
                            topicConfig.getReadQueueNums(),
                            topicConfig.getWriteQueueNums(),
                            topicConfig.getPerm() & this.brokerConfig.getBrokerPermission(), topicConfig.getTopicSysFlag());
                } else {
                    registerTopicConfig = new TopicConfig(topicConfig);
                }
                return registerTopicConfig;
            })
            .collect(Collectors.toConcurrentMap(TopicConfig::getTopicName, Function.identity()));
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigTable);

        Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = topicConfigList.stream()
            .map(TopicConfig::getTopicName)
            .map(topicName -> Optional.ofNullable(broker.getBrokerMetadataManager().getTopicQueueMappingManager().getTopicQueueMapping(topicName))
                .map(info -> new AbstractMap.SimpleImmutableEntry<>(topicName, TopicQueueMappingDetail.cloneAsMappingInfo(info)))
                .orElse(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (!topicQueueMappingInfoMap.isEmpty()) {
            topicConfigSerializeWrapper.setTopicQueueMappingInfoMap(topicQueueMappingInfoMap);
        }

        doRegisterBrokerAll(true, false, topicConfigSerializeWrapper);
    }

    public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway, boolean forceRegister) {
        ConcurrentMap<String, TopicConfig> topicConfigMap = broker.getTopicConfigManager().getTopicConfigTable();
        ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();

        for (TopicConfig topicConfig : topicConfigMap.values()) {
            if (!PermName.isWriteable(broker.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(broker.getBrokerConfig().getBrokerPermission())) {
                topicConfigTable.put(topicConfig.getTopicName(),
                    new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums(),
                        topicConfig.getPerm() & broker.getBrokerConfig().getBrokerPermission()));
            } else {
                topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }

            if (this.brokerConfig.isEnableSplitRegistration()
                && topicConfigTable.size() >= this.brokerConfig.getSplitRegistrationSize()) {
                TopicConfigAndMappingSerializeWrapper topicConfigWrapper = broker.getTopicConfigManager().buildSerializeWrapper(topicConfigTable);
                doRegisterBrokerAll(checkOrderConfig, oneway, topicConfigWrapper);
                topicConfigTable.clear();
            }
        }

        Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = broker.getTopicQueueMappingManager().getTopicQueueMappingTable()
            .entrySet()
            .stream()
            .map(entry -> new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), TopicQueueMappingDetail.cloneAsMappingInfo(entry.getValue())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        TopicConfigAndMappingSerializeWrapper topicConfigWrapper = broker.getTopicConfigManager().buildSerializeWrapper(topicConfigTable, topicQueueMappingInfoMap);
        if (this.brokerConfig.isEnableSplitRegistration()
            || forceRegister
            || needRegister(this.brokerConfig.getBrokerClusterName(),
                    broker.getBrokerAddr(),
                    this.brokerConfig.getBrokerName(),
                    this.brokerConfig.getBrokerId(),
                    this.brokerConfig.getRegisterBrokerTimeoutMills(),
                    this.brokerConfig.isInBrokerContainer()
                )
            ) {
            doRegisterBrokerAll(checkOrderConfig, oneway, topicConfigWrapper);
        }
    }

    public void unregisterBrokerAll() {
        this.clusterClient.unregisterBrokerAll(
            this.brokerConfig.getBrokerClusterName(),
            broker.getBrokerAddr(),
            this.brokerConfig.getBrokerName(),
            this.brokerConfig.getBrokerId());
    }

    private void doRegisterBrokerAll(boolean checkOrderConfig, boolean oneway, TopicConfigSerializeWrapper topicConfigWrapper) {
        if (broker.isShutdown()) {
            LOG.info("BrokerController#doRegisterBrokerAll: broker has shutdown, no need to register any more.");
            return;
        }

        List<RegisterBrokerResult> registerBrokerResultList = this.clusterClient.registerBrokerAll(
            this.brokerConfig.getBrokerClusterName(),
            broker.getBrokerAddr(),
            this.brokerConfig.getBrokerName(),
            this.brokerConfig.getBrokerId(),
            broker.getHAServerAddr(),
            topicConfigWrapper,
            Lists.newArrayList(),
            oneway,
            this.brokerConfig.getRegisterBrokerTimeoutMills(),
            this.brokerConfig.isEnableSlaveActingMaster(),
            this.brokerConfig.isCompressedRegister(),
            this.brokerConfig.isEnableSlaveActingMaster() ? this.brokerConfig.getBrokerNotActiveTimeoutMillis() : null,
            broker.getBrokerIdentity());

        handleRegisterBrokerResult(registerBrokerResultList, checkOrderConfig);
    }

    private boolean needRegister(String clusterName, String brokerAddr, String brokerName, long brokerId, int timeoutMills, boolean isInBrokerContainer) {
        TopicConfigSerializeWrapper topicConfigWrapper = broker.getBrokerMetadataManager().getTopicConfigManager().buildTopicConfigSerializeWrapper();
        List<Boolean> changeList = clusterClient.needRegister(clusterName, brokerAddr, brokerName, brokerId, topicConfigWrapper, timeoutMills, isInBrokerContainer);
        boolean needRegister = false;
        for (Boolean changed : changeList) {
            if (!changed) {
                continue;
            }

            needRegister = true;
            break;
        }
        return needRegister;
    }

    private void handleRegisterBrokerResult(List<RegisterBrokerResult> registerBrokerResultList, boolean checkOrderConfig) {
        for (RegisterBrokerResult registerBrokerResult : registerBrokerResultList) {
            if (registerBrokerResult == null) {
                continue;
            }

            if (broker.getBrokerClusterService().isUpdateMasterHAServerAddrPeriodically() && registerBrokerResult.getHaServerAddr() != null) {
                broker.getMessageStore().updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
                broker.getMessageStore().updateMasterAddress(registerBrokerResult.getMasterAddr());
            }

            broker.getBrokerClusterService().getSlaveSynchronize().setMasterAddr(registerBrokerResult.getMasterAddr());
            if (checkOrderConfig) {
                broker.getBrokerMetadataManager().getTopicConfigManager().updateOrderTopicConfig(registerBrokerResult.getKvTable());
            }
            break;
        }
    }

    public void setBrokerOuterAPI(ClusterClient clusterClient) {
        this.clusterClient = clusterClient;
    }

    public ClusterClient getClusterClient() {
        return clusterClient;
    }

}
