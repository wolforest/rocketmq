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
package org.apache.rocketmq.store.server.store;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.store.api.plugin.MessageArrivingListener;
import org.apache.rocketmq.store.api.filter.MessageFilter;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.domain.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.domain.queue.ConsumeQueueStoreInterface;
import org.apache.rocketmq.store.domain.queue.rocksdb.RocksDBConsumeQueue;
import org.apache.rocketmq.store.domain.queue.rocksdb.RocksDBConsumeQueueStore;
import org.apache.rocketmq.store.domain.queue.rocksdb.RocksDBCleanConsumeQueueService;
import org.apache.rocketmq.store.domain.queue.rocksdb.RocksDBCorrectLogicOffsetService;
import org.apache.rocketmq.store.domain.queue.rocksdb.RocksDBFlushConsumeQueueService;
import org.apache.rocketmq.store.server.daemon.CleanConsumeQueueService;
import org.apache.rocketmq.store.server.daemon.CorrectLogicOffsetService;
import org.apache.rocketmq.store.server.daemon.FlushConsumeQueueService;
import org.apache.rocketmq.store.api.broker.stats.BrokerStatsManager;
import org.apache.rocketmq.store.server.metrics.DefaultStoreMetricsManager;
import org.apache.rocketmq.store.server.metrics.RocksDBStoreMetricsManager;
import org.rocksdb.RocksDBException;

public class RocksDBMessageStore extends DefaultMessageStore {

    public RocksDBMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
        final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig, final ConcurrentMap<String, TopicConfig> topicConfigTable) throws
        IOException {
        super(messageStoreConfig, brokerStatsManager, messageArrivingListener, brokerConfig, topicConfigTable);
        notifyMessageArriveInBatch = true;
    }

    @Override
    public ConsumeQueueStoreInterface createConsumeQueueStore() {
        return new RocksDBConsumeQueueStore(this);
    }

    @Override
    public CleanConsumeQueueService createCleanConsumeQueueService() {
        return new RocksDBCleanConsumeQueueService(this);
    }

    @Override
    public FlushConsumeQueueService createFlushConsumeQueueService() {
        return new RocksDBFlushConsumeQueueService(this);
    }

    @Override
    public CorrectLogicOffsetService createCorrectLogicOffsetService() {
        return new RocksDBCorrectLogicOffsetService(this);
    }

    /**
     * Try to set topicQueueTable = new HashMap<>(), otherwise it will cause bug when broker role changes.
     * And unlike method in DefaultMessageStore, we don't need to really recover topic queue table advance,
     * because we can recover topic queue table from rocksdb when we need to use it.
     * @see RocksDBConsumeQueue#assignQueueOffset
     */
    @Override
    public void recoverTopicQueueTable() {
        this.consumeQueueStore.setTopicQueueTable(new ConcurrentHashMap<>());
    }

    @Override
    public void finishCommitLogDispatch() {
        try {
            putMessagePositionInfo(null);
        } catch (RocksDBException e) {
            ERROR_LOG.info("try to finish commitlog dispatch error.", e);
        }
    }

    @Override
    public ConsumeQueueInterface getConsumeQueue(String topic, int queueId) {
        return findConsumeQueue(topic, queueId);
    }

    @Override
    public long estimateMessageCount(String topic, int queueId, long from, long to, MessageFilter filter) {
        // todo
        return 0;
    }

    @Override
    public void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier) {
        DefaultStoreMetricsManager.init(meter, attributesBuilderSupplier, this);
        // Also add some metrics for rocksdb's monitoring.
        RocksDBStoreMetricsManager.init(meter, attributesBuilderSupplier, this);
    }
}
