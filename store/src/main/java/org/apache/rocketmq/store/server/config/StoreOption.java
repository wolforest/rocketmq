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
package org.apache.rocketmq.store.server.config;

import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.store.api.broker.stats.BrokerStatsManager;
import org.apache.rocketmq.store.api.plugin.MessageArrivingListener;

public class StoreOption {
    private BrokerConfig brokerConfig;
    private MessageStoreConfig storeConfig;
    private ConcurrentMap<String, TopicConfig> topicTable;
    private MessageArrivingListener listener;

    public StoreOption() {

    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public MessageStoreConfig getStoreConfig() {
        return storeConfig;
    }

    public ConcurrentMap<String, TopicConfig> getTopicTable() {
        return topicTable;
    }

    public MessageArrivingListener getListener() {
        return listener;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    private BrokerStatsManager brokerStatsManager;

    public static StoreOptionBuilder builder() {
        return StoreOptionBuilder.aStoreOption();
    }

    public static final class StoreOptionBuilder {
        private BrokerConfig brokerConfig;
        private MessageStoreConfig storeConfig;
        private ConcurrentMap<String, TopicConfig> topicTable;
        private MessageArrivingListener listener;
        private BrokerStatsManager brokerStatsManager;

        private StoreOptionBuilder() {
        }

        public static StoreOptionBuilder aStoreOption() {
            return new StoreOptionBuilder();
        }

        public StoreOptionBuilder brokerConfig(BrokerConfig brokerConfig) {
            this.brokerConfig = brokerConfig;
            return this;
        }

        public StoreOptionBuilder storeConfig(MessageStoreConfig storeConfig) {
            this.storeConfig = storeConfig;
            return this;
        }

        public StoreOptionBuilder topicTable(ConcurrentMap<String, TopicConfig> topicTable) {
            this.topicTable = topicTable;
            return this;
        }

        public StoreOptionBuilder listener(MessageArrivingListener listener) {
            this.listener = listener;
            return this;
        }

        public StoreOptionBuilder brokerStatsManager(BrokerStatsManager brokerStatsManager) {
            this.brokerStatsManager = brokerStatsManager;
            return this;
        }

        public StoreOption build() {
            StoreOption storeOption = new StoreOption();
            storeOption.topicTable = this.topicTable;
            storeOption.brokerStatsManager = this.brokerStatsManager;
            storeOption.brokerConfig = this.brokerConfig;
            storeOption.listener = this.listener;
            storeOption.storeConfig = this.storeConfig;
            return storeOption;
        }
    }
}
