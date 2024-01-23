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

import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.domain.metadata.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.domain.queue.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.domain.queue.offset.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.domain.queue.offset.LmqConsumerOffsetManager;
import org.apache.rocketmq.broker.domain.queue.offset.RocksDBConsumerOffsetManager;
import org.apache.rocketmq.broker.domain.queue.offset.RocksDBLmqConsumerOffsetManager;
import org.apache.rocketmq.broker.domain.metadata.subscription.LmqSubscriptionGroupManager;
import org.apache.rocketmq.broker.domain.metadata.subscription.RocksDBLmqSubscriptionGroupManager;
import org.apache.rocketmq.broker.domain.metadata.subscription.RocksDBSubscriptionGroupManager;
import org.apache.rocketmq.broker.domain.metadata.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.domain.metadata.topic.LmqTopicConfigManager;
import org.apache.rocketmq.broker.domain.metadata.topic.RocksDBLmqTopicConfigManager;
import org.apache.rocketmq.broker.domain.metadata.topic.RocksDBTopicConfigManager;
import org.apache.rocketmq.broker.domain.metadata.topic.TopicConfigManager;
import org.apache.rocketmq.broker.domain.metadata.topic.TopicQueueMappingManager;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.server.config.StoreType;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;

public class BrokerMetadataService {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final MessageStoreConfig messageStoreConfig;
    private final Broker broker;

    protected TopicQueueMappingManager topicQueueMappingManager;
    private ConsumerOffsetManager consumerOffsetManager;
    private ConsumerFilterManager consumerFilterManager;
    protected ConsumerOrderInfoManager consumerOrderInfoManager;
    protected TopicConfigManager topicConfigManager;
    protected SubscriptionGroupManager subscriptionGroupManager;

    public BrokerMetadataService(Broker broker) {
        this.broker = broker;
        this.messageStoreConfig = broker.getMessageStoreConfig();
        init();
    }

    public boolean load() {
        boolean result = this.topicConfigManager.load();
        result = result && this.topicQueueMappingManager.load();
        result = result && this.consumerOffsetManager.load();
        result = result && this.subscriptionGroupManager.load();
        result = result && this.consumerFilterManager.load();
        result = result && this.consumerOrderInfoManager.load();
        return result;
    }

    public void start() {

    }

    public void shutdown() {
        this.consumerOffsetManager.persist();

        if (this.consumerFilterManager != null) {
            this.consumerFilterManager.persist();
        }

        if (this.consumerOrderInfoManager != null) {
            this.consumerOrderInfoManager.persist();
        }

        if (this.topicConfigManager != null) {
            this.topicConfigManager.persist();
            this.topicConfigManager.stop();
        }

        if (this.subscriptionGroupManager != null) {
            this.subscriptionGroupManager.persist();
            this.subscriptionGroupManager.stop();
        }

        if (this.consumerOffsetManager != null) {
            this.consumerOffsetManager.persist();
            this.consumerOffsetManager.stop();
        }
    }

    private boolean isEnableRocksDBStore() {
        return StoreType.DEFAULT_ROCKSDB.getStoreType().equalsIgnoreCase(this.messageStoreConfig.getStoreType());
    }

    private void init() {
        if (isEnableRocksDBStore()) {
            this.topicConfigManager = messageStoreConfig.isEnableLmq() ? new RocksDBLmqTopicConfigManager(broker) : new RocksDBTopicConfigManager(broker);
            this.subscriptionGroupManager = messageStoreConfig.isEnableLmq() ? new RocksDBLmqSubscriptionGroupManager(broker) : new RocksDBSubscriptionGroupManager(broker);
            this.consumerOffsetManager = messageStoreConfig.isEnableLmq() ? new RocksDBLmqConsumerOffsetManager(broker) : new RocksDBConsumerOffsetManager(broker);
        } else {
            this.topicConfigManager = messageStoreConfig.isEnableLmq() ? new LmqTopicConfigManager(broker) : new TopicConfigManager(broker);
            this.subscriptionGroupManager = messageStoreConfig.isEnableLmq() ? new LmqSubscriptionGroupManager(broker) : new SubscriptionGroupManager(broker);
            this.consumerOffsetManager = messageStoreConfig.isEnableLmq() ? new LmqConsumerOffsetManager(broker) : new ConsumerOffsetManager(broker);
        }

        this.topicQueueMappingManager = new TopicQueueMappingManager(broker);
        this.consumerFilterManager = new ConsumerFilterManager(broker);
        this.consumerOrderInfoManager = new ConsumerOrderInfoManager(broker);
    }

    public TopicQueueMappingManager getTopicQueueMappingManager() {
        return topicQueueMappingManager;
    }

    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }

    public ConsumerFilterManager getConsumerFilterManager() {
        return consumerFilterManager;
    }

    public ConsumerOrderInfoManager getConsumerOrderInfoManager() {
        return consumerOrderInfoManager;
    }

    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }

    public void setTopicQueueMappingManager(TopicQueueMappingManager topicQueueMappingManager) {
        this.topicQueueMappingManager = topicQueueMappingManager;
    }

    public void setConsumerOffsetManager(ConsumerOffsetManager consumerOffsetManager) {
        this.consumerOffsetManager = consumerOffsetManager;
    }

    public void setConsumerFilterManager(ConsumerFilterManager consumerFilterManager) {
        this.consumerFilterManager = consumerFilterManager;
    }

    public void setConsumerOrderInfoManager(ConsumerOrderInfoManager consumerOrderInfoManager) {
        this.consumerOrderInfoManager = consumerOrderInfoManager;
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }

    public void setSubscriptionGroupManager(
        SubscriptionGroupManager subscriptionGroupManager) {
        this.subscriptionGroupManager = subscriptionGroupManager;
    }

}
