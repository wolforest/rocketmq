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
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.ShutdownHook;
import org.apache.rocketmq.broker.domain.consumer.ConsumerManager;
import org.apache.rocketmq.broker.domain.producer.ProducerManager;
import org.apache.rocketmq.broker.infra.network.Broker2Client;
import org.apache.rocketmq.broker.domain.coldctr.ColdDataCgCtrService;
import org.apache.rocketmq.broker.domain.coldctr.ColdDataPullRequestHoldService;
import org.apache.rocketmq.broker.server.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.infra.offset.BroadcastOffsetManager;
import org.apache.rocketmq.broker.api.plugin.BrokerPlugin;
import org.apache.rocketmq.broker.server.daemon.pop.PopInflightMessageCounter;
import org.apache.rocketmq.broker.infra.topic.TopicQueueMappingCleanService;
import org.apache.rocketmq.broker.infra.topic.TopicRouteInfoManager;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.api.broker.stats.BrokerStats;
import org.apache.rocketmq.store.api.broker.stats.BrokerStatsManager;
import org.apache.rocketmq.store.api.broker.stats.LmqBrokerStatsManager;

/**
 * manager services type like
 *      1, context style: getter and setter
 *      2, have methods: start and shutdown
 *      3, monitor service:
 *      4, broker plugins
 *      5, shutdown hook
 */
public class BrokerServiceManager {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final Broker broker;
    private final BrokerConfig brokerConfig;
    private final MessageStoreConfig messageStoreConfig;

    /* ServiceWithGetterAndSetter start */
    private ProducerManager producerManager;
    private ConsumerManager consumerManager;
    private Broker2Client broker2Client;
    private PopInflightMessageCounter popInflightMessageCounter;
    /* ServiceWithGetterAndSetter end */

    /* ServiceWithStartAndShutdown start */
    private BroadcastOffsetManager broadcastOffsetManager;
    private TopicRouteInfoManager topicRouteInfoManager;
    private BrokerFastFailure brokerFastFailure;
    private BrokerPreOnlineService brokerPreOnlineService;
    private TopicQueueMappingCleanService topicQueueMappingCleanService;

    private ColdDataPullRequestHoldService coldDataPullRequestHoldService;
    private ColdDataCgCtrService coldDataCgCtrService;
    /* ServiceWithStartAndShutdown start */

    /* monitor servie start */
    private BrokerStatsManager brokerStatsManager;
    private BrokerMetricsManager brokerMetricsManager;
    private BrokerStats brokerStats;
    /* monitor servie end */

    /*  broker plugin start */
    private List<BrokerPlugin> brokerPlugins = new ArrayList<>();
    /*  broker plugin end */

    private ShutdownHook shutdownHook;

    public BrokerServiceManager(Broker broker) {
        this.broker = broker;
        this.brokerConfig = broker.getBrokerConfig();
        this.messageStoreConfig = broker.getMessageStoreConfig();
        initMonitorService();
        initServiceWithGetterAndSetter();
        initBrokerPlugin();
    }

    public boolean load() {
        this.brokerMetricsManager = new BrokerMetricsManager(broker);
        this.brokerStats = new BrokerStats(broker.getBrokerMessageService().getMessageStore());

        boolean result = true;
        for (BrokerPlugin brokerPlugin : brokerPlugins) {
            if (brokerPlugin != null) {
                result = result && brokerPlugin.load();
            }
        }
        return result;
    }

    public void initialize() {
        initServiceWithStartAndShutdown();
    }

    public void start() {
        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.start();
        }

        if (this.broadcastOffsetManager != null) {
            this.broadcastOffsetManager.start();
        }

        if (this.topicRouteInfoManager != null) {
            this.topicRouteInfoManager.start();
        }

        if (this.brokerPreOnlineService != null) {
            this.brokerPreOnlineService.start();
        }

        if (this.coldDataPullRequestHoldService != null) {
            this.coldDataPullRequestHoldService.start();
        }

        if (this.coldDataCgCtrService != null) {
            this.coldDataCgCtrService.start();
        }

        if (this.topicQueueMappingCleanService != null) {
            this.topicQueueMappingCleanService.start();
        }

        for (BrokerPlugin brokerPlugin : brokerPlugins) {
            if (brokerPlugin != null) {
                brokerPlugin.start();
            }
        }

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
        }
    }

    public void shutdown() {
        if (this.broadcastOffsetManager != null) {
            this.broadcastOffsetManager.shutdown();
        }

        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.shutdown();
        }

        if (this.topicRouteInfoManager != null) {
            this.topicRouteInfoManager.shutdown();
        }

        if (this.brokerPreOnlineService != null && !this.brokerPreOnlineService.isStopped()) {
            this.brokerPreOnlineService.shutdown();
        }

        if (this.coldDataPullRequestHoldService != null) {
            this.coldDataPullRequestHoldService.shutdown();
        }

        if (this.coldDataCgCtrService != null) {
            this.coldDataCgCtrService.shutdown();
        }

        if (this.topicQueueMappingCleanService != null) {
            this.topicQueueMappingCleanService.shutdown();
        }

        if (this.brokerMetricsManager != null) {
            this.brokerMetricsManager.shutdown();
        }

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.shutdown();
        }

        for (BrokerPlugin brokerPlugin : brokerPlugins) {
            if (brokerPlugin != null) {
                brokerPlugin.shutdown();
            }
        }

        if (this.shutdownHook != null) {
            this.shutdownHook.beforeShutdown(broker);
        }
    }

    private void initServiceWithGetterAndSetter() {
        this.consumerManager = new ConsumerManager(broker.getBrokerNettyServer().getConsumerIdsChangeListener(), brokerStatsManager, broker.getBrokerConfig());
        this.producerManager = new ProducerManager(brokerStatsManager);

        this.broker2Client = new Broker2Client(broker);
        this.popInflightMessageCounter = new PopInflightMessageCounter(broker);
    }

    private void initServiceWithStartAndShutdown() {
        this.broadcastOffsetManager = new BroadcastOffsetManager(broker);

        this.brokerFastFailure = new BrokerFastFailure(broker);
        this.topicRouteInfoManager = new TopicRouteInfoManager(broker);

        this.coldDataPullRequestHoldService = new ColdDataPullRequestHoldService(broker);
        this.coldDataCgCtrService = new ColdDataCgCtrService(broker);

        this.topicQueueMappingCleanService = new TopicQueueMappingCleanService(broker);

        if (this.brokerConfig.isEnableSlaveActingMaster() && !this.brokerConfig.isSkipPreOnline()) {
            this.brokerPreOnlineService = new BrokerPreOnlineService(broker);
        }
    }

    private void initMonitorService() {

        this.brokerStatsManager = messageStoreConfig.isEnableLmq()
            ? new LmqBrokerStatsManager(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.isEnableDetailStat())
            : new BrokerStatsManager(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.isEnableDetailStat());

        initProducerStateGetter();
        initConsumerStateGetter();
    }

    private void initProducerStateGetter() {
        this.brokerStatsManager.setProduerStateGetter(new BrokerStatsManager.StateGetter() {
            @Override
            public boolean online(String instanceId, String group, String topic) {
                if (broker.getTopicConfigManager().getTopicConfigTable().containsKey(NamespaceUtil.wrapNamespace(instanceId, topic))) {
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
                if (broker.getTopicConfigManager().getTopicConfigTable().containsKey(topicFullName)) {
                    return getConsumerManager().findSubscriptionData(NamespaceUtil.wrapNamespace(instanceId, group), topicFullName) != null;
                } else {
                    return getConsumerManager().findSubscriptionData(group, topic) != null;
                }
            }
        });
    }

    private void initBrokerPlugin() {

    }

    public BrokerStats getBrokerStats() {
        return brokerStats;
    }

    public ShutdownHook getShutdownHook() {
        return shutdownHook;
    }

    public void setShutdownHook(ShutdownHook shutdownHook) {
        this.shutdownHook = shutdownHook;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public BrokerMetricsManager getBrokerMetricsManager() {
        return brokerMetricsManager;
    }

    public List<BrokerPlugin> getBrokerAttachedPlugins() {
        return brokerPlugins;
    }

    public ProducerManager getProducerManager() {
        return producerManager;
    }

    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public Broker2Client getBroker2Client() {
        return broker2Client;
    }

    public PopInflightMessageCounter getPopInflightMessageCounter() {
        return popInflightMessageCounter;
    }

    public BroadcastOffsetManager getBroadcastOffsetManager() {
        return broadcastOffsetManager;
    }

    public TopicRouteInfoManager getTopicRouteInfoManager() {
        return topicRouteInfoManager;
    }

    public BrokerFastFailure getBrokerFastFailure() {
        return brokerFastFailure;
    }

    public BrokerPreOnlineService getBrokerPreOnlineService() {
        return brokerPreOnlineService;
    }

    public ColdDataPullRequestHoldService getColdDataPullRequestHoldService() {
        return coldDataPullRequestHoldService;
    }

    public ColdDataCgCtrService getColdDataCgCtrService() {
        return coldDataCgCtrService;
    }

    public TopicQueueMappingCleanService getTopicQueueMappingCleanService() {
        return topicQueueMappingCleanService;
    }

}
