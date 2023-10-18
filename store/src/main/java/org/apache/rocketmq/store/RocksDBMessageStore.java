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
package org.apache.rocketmq.store;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.ConsumeQueueStoreInterface;
import org.apache.rocketmq.store.queue.RocksDBConsumeQueue;
import org.apache.rocketmq.store.queue.RocksDBConsumeQueueStore;
import org.apache.rocketmq.store.rocksdb.RocksDBCleanConsumeQueueService;
import org.apache.rocketmq.store.rocksdb.RocksDBCorrectLogicOffsetService;
import org.apache.rocketmq.store.rocksdb.RocksDBFlushConsumeQueueService;
import org.apache.rocketmq.store.service.CleanConsumeQueueService;
import org.apache.rocketmq.store.service.CorrectLogicOffsetService;
import org.apache.rocketmq.store.service.FlushConsumeQueueService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
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
}
