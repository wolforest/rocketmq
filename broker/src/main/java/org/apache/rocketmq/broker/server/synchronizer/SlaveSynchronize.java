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
package org.apache.rocketmq.broker.server.synchronizer;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.domain.metadata.loadbalance.MessageRequestModeManager;
import org.apache.rocketmq.broker.domain.metadata.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.common.utils.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.MessageRequestModeSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.store.server.config.StorePathConfigHelper;
import org.apache.rocketmq.store.domain.timer.TimerCheckpoint;
import org.apache.rocketmq.store.domain.timer.TimerMetrics;

public class SlaveSynchronize {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final Broker broker;
    private volatile String masterAddr = null;

    public SlaveSynchronize(Broker broker) {
        this.broker = broker;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public void setMasterAddr(String masterAddr) {
        if (StringUtils.equals(this.masterAddr, masterAddr)) {
            return;
        }

        LOGGER.info("Update master address from {} to {}", this.masterAddr, masterAddr);
        this.masterAddr = masterAddr;
    }

    public void syncAll() {
        this.syncTopicConfig();
        this.syncConsumerOffset();
        this.syncDelayOffset();
        this.syncSubscriptionGroupConfig();
        this.syncMessageRequestMode();

        if (broker.getMessageStoreConfig().isTimerWheelEnable()) {
            this.syncTimerMetrics();
        }
    }

    public void syncTimerCheckPoint() {
        String masterAddrBak = this.masterAddr;
        if (null == masterAddrBak) {
            return;
        }

        try {
            if (null == broker.getMessageStore().getTimerMessageStore()) {
                return;
            }

            if (broker.getMessageStore().getTimerMessageStore().getTimerState().isShouldRunningDequeue()) {
                return;
            }

            TimerCheckpoint checkpoint = this.broker.getClusterClient().getTimerCheckPoint(masterAddrBak);
            if (null != this.broker.getTimerCheckpoint()) {
                this.broker.getTimerCheckpoint().setLastReadTimeMs(checkpoint.getLastReadTimeMs());
                this.broker.getTimerCheckpoint().setMasterTimerQueueOffset(checkpoint.getMasterTimerQueueOffset());
                this.broker.getTimerCheckpoint().getDataVersion().assignNewOne(checkpoint.getDataVersion());
            }
        } catch (Exception e) {
            LOGGER.error("syncTimerCheckPoint Exception, {}", masterAddrBak, e);
        }
    }

    private void syncTopicConfig() {
        String masterAddrBak = this.masterAddr;
        if (null == masterAddrBak || masterAddrBak.equals(broker.getBrokerAddr())) {
            return;
        }

        try {
            TopicConfigAndMappingSerializeWrapper topicWrapper = this.broker.getClusterClient().getAllTopicConfig(masterAddrBak);
            if (!this.broker.getTopicConfigManager().getDataVersion().equals(topicWrapper.getDataVersion())) {

                this.broker.getTopicConfigManager().getDataVersion().assignNewOne(topicWrapper.getDataVersion());

                ConcurrentMap<String, TopicConfig> newTopicConfigTable = topicWrapper.getTopicConfigTable();
                //delete
                ConcurrentMap<String, TopicConfig> topicConfigTable = this.broker.getTopicConfigManager().getTopicConfigTable();
                topicConfigTable.entrySet().removeIf(item -> !newTopicConfigTable.containsKey(item.getKey()));
                //update
                topicConfigTable.putAll(newTopicConfigTable);

                this.broker.getTopicConfigManager().persist();
            }
            if (topicWrapper.getTopicQueueMappingDetailMap() != null
                && !topicWrapper.getMappingDataVersion().equals(this.broker.getTopicQueueMappingManager().getDataVersion())) {
                this.broker.getTopicQueueMappingManager().getDataVersion().assignNewOne(topicWrapper.getMappingDataVersion());

                ConcurrentMap<String, TopicConfig> newTopicConfigTable = topicWrapper.getTopicConfigTable();
                //delete
                ConcurrentMap<String, TopicConfig> topicConfigTable = this.broker.getTopicConfigManager().getTopicConfigTable();
                topicConfigTable.entrySet().removeIf(item -> !newTopicConfigTable.containsKey(item.getKey()));
                //update
                topicConfigTable.putAll(newTopicConfigTable);

                this.broker.getTopicQueueMappingManager().persist();
            }
            LOGGER.info("Update slave topic config from master, {}", masterAddrBak);
        } catch (Exception e) {
            LOGGER.error("SyncTopicConfig Exception, {}", masterAddrBak, e);
        }
    }

    private void syncConsumerOffset() {
        String masterAddrBak = this.masterAddr;
        if (null == masterAddrBak || masterAddrBak.equals(broker.getBrokerAddr())) {
            return;
        }

        try {
            ConsumerOffsetSerializeWrapper offsetWrapper = this.broker.getClusterClient().getAllConsumerOffset(masterAddrBak);
            this.broker.getConsumerOffsetManager().getOffsetTable().putAll(offsetWrapper.getOffsetTable());
            this.broker.getConsumerOffsetManager().getDataVersion().assignNewOne(offsetWrapper.getDataVersion());
            this.broker.getConsumerOffsetManager().persist();
            LOGGER.info("Update slave consumer offset from master, {}", masterAddrBak);
        } catch (Exception e) {
            LOGGER.error("SyncConsumerOffset Exception, {}", masterAddrBak, e);
        }
    }

    private void syncDelayOffset() {
        String masterAddrBak = this.masterAddr;
        if (null == masterAddrBak || masterAddrBak.equals(broker.getBrokerAddr())) {
            return;
        }

        try {
            String delayOffset = this.broker.getClusterClient().getAllDelayOffset(masterAddrBak);
            if (delayOffset == null) {
                return;
            }

            String dir = this.broker.getMessageStoreConfig().getStorePathRootDir();
            String fileName = StorePathConfigHelper.getDelayOffsetStorePath(dir);

            try {
                IOUtils.string2File(delayOffset, fileName);
                this.broker.getScheduleMessageService().loadWhenSyncDelayOffset();
            } catch (IOException e) {
                LOGGER.error("Persist file Exception, {}", fileName, e);
            }

            LOGGER.info("Update slave delay offset from master, {}", masterAddrBak);
        } catch (Exception e) {
            LOGGER.error("SyncDelayOffset Exception, {}", masterAddrBak, e);
        }
    }

    private void syncSubscriptionGroupConfig() {
        String masterAddrBak = this.masterAddr;
        if (null == masterAddrBak || masterAddrBak.equals(broker.getBrokerAddr())) {
            return;
        }

        try {
            SubscriptionGroupWrapper subscription = this.broker.getClusterClient().getAllSubscriptionGroupConfig(masterAddrBak);
            if (this.broker.getSubscriptionGroupManager().getDataVersion().equals(subscription.getDataVersion())) {
                return;
            }

            SubscriptionGroupManager manager = this.broker.getSubscriptionGroupManager();
            manager.getDataVersion().assignNewOne(subscription.getDataVersion());
            manager.getSubscriptionGroupTable().clear();
            manager.getSubscriptionGroupTable().putAll(subscription.getSubscriptionGroupTable());
            manager.persist();
            LOGGER.info("Update slave Subscription Group from master, {}", masterAddrBak);
        } catch (Exception e) {
            LOGGER.error("SyncSubscriptionGroup Exception, {}", masterAddrBak, e);
        }
    }

    private void syncMessageRequestMode() {
        String masterAddrBak = this.masterAddr;
        if (null == masterAddrBak || masterAddrBak.equals(broker.getBrokerAddr())) {
            return;
        }

        try {
            MessageRequestModeSerializeWrapper requestMode = this.broker.getClusterClient().getAllMessageRequestMode(masterAddrBak);
            MessageRequestModeManager manager = this.broker.getBrokerNettyServer().getQueryAssignmentProcessor().getMessageRequestModeManager();

            manager.getMessageRequestModeMap().clear();
            manager.getMessageRequestModeMap().putAll(requestMode.getMessageRequestModeMap());
            manager.persist();
            LOGGER.info("Update slave Message Request Mode from master, {}", masterAddrBak);
        } catch (Exception e) {
            LOGGER.error("SyncMessageRequestMode Exception, {}", masterAddrBak, e);
        }
    }

    private void syncTimerMetrics() {
        String masterAddrBak = this.masterAddr;
        if (null == masterAddrBak) {
            return;
        }

        try {
            if (null == broker.getMessageStore().getTimerMessageStore()) {
                return;
            }

            TimerMetrics.TimerMetricsSerializeWrapper metrics = this.broker.getClusterClient().getTimerMetrics(masterAddrBak);
            TimerMetrics timerMetrics = broker.getMessageStore().getTimerMessageStore().getTimerMetrics();
            if (timerMetrics.getDataVersion().equals(metrics.getDataVersion())) {
                return;
            }

            timerMetrics.getDataVersion().assignNewOne(metrics.getDataVersion());
            timerMetrics.getTimingCount().clear();
            timerMetrics.getTimingCount().putAll(metrics.getTimingCount());
            timerMetrics.persist();
        } catch (Exception e) {
            LOGGER.error("SyncTimerMetrics Exception, {}", masterAddrBak, e);
        }
    }
}
