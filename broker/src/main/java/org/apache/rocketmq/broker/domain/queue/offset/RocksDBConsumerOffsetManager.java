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
package org.apache.rocketmq.broker.domain.queue.offset;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import java.io.File;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.common.app.config.RocksDBConfigManager;
import org.apache.rocketmq.common.utils.DataConverter;
import org.rocksdb.WriteBatch;

public class RocksDBConsumerOffsetManager extends ConsumerOffsetManager {

    public RocksDBConsumerOffsetManager(Broker broker) {
        super(broker);
        this.rocksDBConfigManager = new RocksDBConfigManager(broker.getMessageStoreConfig().getMemTableFlushIntervalMs());
    }

    @Override
    public boolean load() {
        return this.rocksDBConfigManager.load(configFilePath(), this::decode0);
    }

    @Override
    public boolean stop() {
        return this.rocksDBConfigManager.stop();
    }

    @Override
    protected void removeConsumerOffset(String topicAtGroup) {
        try {
            byte[] keyBytes = topicAtGroup.getBytes(DataConverter.CHARSET_UTF8);
            this.rocksDBConfigManager.delete(keyBytes);
        } catch (Exception e) {
            LOG.error("kv remove consumerOffset Failed, {}", topicAtGroup);
        }
    }

    @Override
    protected void decode0(final byte[] key, final byte[] body) {
        String topicAtGroup = new String(key, DataConverter.CHARSET_UTF8);
        RocksDBOffsetSerializeWrapper wrapper = JSON.parseObject(body, RocksDBOffsetSerializeWrapper.class);

        this.offsetTable.put(topicAtGroup, wrapper.getOffsetTable());
        LOG.info("load exist local offset, {}, {}", topicAtGroup, wrapper.getOffsetTable());
    }

    @Override
    public String configFilePath() {
        return this.broker.getMessageStoreConfig().getStorePathRootDir() + File.separator + "config" + File.separator + "consumerOffsets" + File.separator;
    }

    @Override
    public synchronized void persist() {
        try (WriteBatch writeBatch = new WriteBatch()) {
            for (Entry<String, ConcurrentMap<Integer, Long>> entry : this.offsetTable.entrySet()) {
                putWriteBatch(writeBatch, entry.getKey(), entry.getValue());
                if (writeBatch.getDataSize() < 4 * 1024) {
                    continue;
                }

                this.rocksDBConfigManager.batchPutWithWal(writeBatch);
            }
            this.rocksDBConfigManager.batchPutWithWal(writeBatch);
            this.rocksDBConfigManager.flushWAL();
        } catch (Exception e) {
            LOG.error("consumer offset persist Failed", e);
        }
    }

    private void putWriteBatch(final WriteBatch writeBatch, final String topicGroupName, final ConcurrentMap<Integer, Long> offsetMap) throws Exception {
        byte[] keyBytes = topicGroupName.getBytes(DataConverter.CHARSET_UTF8);
        RocksDBOffsetSerializeWrapper wrapper = new RocksDBOffsetSerializeWrapper();
        wrapper.setOffsetTable(offsetMap);
        byte[] valueBytes = JSON.toJSONBytes(wrapper, SerializerFeature.BrowserCompatible);
        writeBatch.put(keyBytes, valueBytes);
    }
}
