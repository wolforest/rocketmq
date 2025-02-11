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

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.broker.ShutdownHook;
import org.apache.rocketmq.broker.api.plugin.BrokerPlugin;
import org.apache.rocketmq.broker.domain.coldctr.ColdDataCgCtrThread;
import org.apache.rocketmq.broker.domain.coldctr.ColdDataPullRequestHoldThread;
import org.apache.rocketmq.broker.domain.consumer.ConsumerManager;
import org.apache.rocketmq.broker.domain.metadata.topic.TopicQueueMappingCleanService;
import org.apache.rocketmq.broker.domain.metadata.topic.TopicRouteInfoManager;
import org.apache.rocketmq.broker.domain.producer.ProducerManager;
import org.apache.rocketmq.broker.domain.queue.offset.BroadcastOffsetManager;
import org.apache.rocketmq.broker.infra.Broker2Client;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.daemon.BrokerFastFailure;
import org.apache.rocketmq.broker.server.daemon.BrokerPreOnlineThread;
import org.apache.rocketmq.broker.server.daemon.pop.PopInflightMessageCounter;
import org.apache.rocketmq.broker.server.metrics.BrokerMetricsManager;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.store.api.broker.stats.BrokerStats;
import org.apache.rocketmq.store.api.broker.stats.BrokerStatsManager;
import org.apache.rocketmq.store.api.broker.stats.LmqBrokerStatsManager;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;

/**
 * manager services type like
 *      1, context style: getter and setter
 *      2, have methods: start and shutdown
 *      3, monitor service:
 *      4, broker plugins
 *      5, shutdown hook
 */
public class BrokerServiceManager {
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
    private BrokerPreOnlineThread brokerPreOnlineThread;
    private TopicQueueMappingCleanService topicQueueMappingCleanService;

    private ColdDataPullRequestHoldThread coldDataPullRequestHoldThread;
    private ColdDataCgCtrThread coldDataCgCtrThread;
    /* ServiceWithStartAndShutdown start */

    /* monitor servie start */
    private BrokerStatsManager brokerStatsManager;
    private BrokerMetricsManager brokerMetricsManager;
    private BrokerStats brokerStats;
    /* monitor servie end */

    /*  broker plugin start */
    private final List<BrokerPlugin> brokerPlugins = new ArrayList<>();
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

        if (this.brokerPreOnlineThread != null) {
            this.brokerPreOnlineThread.start();
        }

        if (this.coldDataPullRequestHoldThread != null) {
            this.coldDataPullRequestHoldThread.start();
        }

        if (this.coldDataCgCtrThread != null) {
            this.coldDataCgCtrThread.start();
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

        if (this.brokerPreOnlineThread != null && !this.brokerPreOnlineThread.isStopped()) {
            this.brokerPreOnlineThread.shutdown();
        }

        if (this.coldDataPullRequestHoldThread != null) {
            this.coldDataPullRequestHoldThread.shutdown();
        }

        if (this.coldDataCgCtrThread != null) {
            this.coldDataCgCtrThread.shutdown();
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

        this.coldDataPullRequestHoldThread = new ColdDataPullRequestHoldThread(broker);
        this.coldDataCgCtrThread = new ColdDataCgCtrThread(broker);

        this.topicQueueMappingCleanService = new TopicQueueMappingCleanService(broker);

        if (this.brokerConfig.isEnableSlaveActingMaster() && !this.brokerConfig.isSkipPreOnline()) {
            this.brokerPreOnlineThread = new BrokerPreOnlineThread(broker);
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
        this.brokerStatsManager.setProduerStateGetter((instanceId, group, topic) -> {
            if (broker.getTopicConfigManager().getTopicConfigTable().containsKey(NamespaceUtil.wrapNamespace(instanceId, topic))) {
                return getProducerManager().groupOnline(NamespaceUtil.wrapNamespace(instanceId, group));
            } else {
                return getProducerManager().groupOnline(group);
            }
        });
    }

    private void initConsumerStateGetter() {
        this.brokerStatsManager.setConsumerStateGetter((instanceId, group, topic) -> {
            String topicFullName = NamespaceUtil.wrapNamespace(instanceId, topic);
            if (broker.getTopicConfigManager().getTopicConfigTable().containsKey(topicFullName)) {
                return getConsumerManager().findSubscriptionData(NamespaceUtil.wrapNamespace(instanceId, group), topicFullName) != null;
            } else {
                return getConsumerManager().findSubscriptionData(group, topic) != null;
            }
        });
    }

    private void initBrokerPlugin() {

    }

    public BrokerStats getBrokerStats() {
        return brokerStats;
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

    public ColdDataPullRequestHoldThread getColdDataPullRequestHoldService() {
        return coldDataPullRequestHoldThread;
    }

    public ColdDataCgCtrThread getColdDataCgCtrService() {
        return coldDataCgCtrThread;
    }

    public TopicQueueMappingCleanService getTopicQueueMappingCleanService() {
        return topicQueueMappingCleanService;
    }

}
