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
package org.apache.rocketmq.store.domain.queue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.consumer.CQType;
import org.apache.rocketmq.common.domain.message.MessageAccessor;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.domain.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.domain.topic.TopicAttributes;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.store.StoreTestBase;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.api.broker.stats.BrokerStatsManager;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;

public class QueueTestBase extends StoreTestBase {
    protected ConcurrentMap<String, TopicConfig> createTopicConfigTable(String topic, CQType cqType) {
        ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
        TopicConfig topicConfigToBeAdded = new TopicConfig();

        Map<String, String> attributes = new HashMap<>();
        attributes.put(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getName(), cqType.toString());
        topicConfigToBeAdded.setTopicName(topic);
        topicConfigToBeAdded.setAttributes(attributes);

        topicConfigTable.put(topic, topicConfigToBeAdded);
        return topicConfigTable;
    }

    protected Callable<Boolean> fullyDispatched(MessageStore messageStore) {
        return () -> messageStore.dispatchBehindBytes() == 0;
    }

    protected MessageStore createMessageStore(String baseDir, boolean extent,  ConcurrentMap<String, TopicConfig> topicConfigTable) throws Exception {
        if (baseDir == null) {
            baseDir = createBaseDir();
        }
        baseDirs.add(baseDir);
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 8);
        messageStoreConfig.setMappedFileSizeConsumeQueue(100 * ConsumeQueue.CQ_STORE_UNIT_SIZE);
        messageStoreConfig.setMapperFileSizeBatchConsumeQueue(20 * BatchConsumeQueue.CQ_STORE_UNIT_SIZE);
        messageStoreConfig.setMappedFileSizeConsumeQueueExt(1024);
        messageStoreConfig.setMaxIndexNum(100 * 10);
        messageStoreConfig.setEnableConsumeQueueExt(extent);
        messageStoreConfig.setStorePathRootDir(baseDir);
        messageStoreConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");
        messageStoreConfig.setHaListenPort(nextPort());
        messageStoreConfig.setMaxTransferBytesOnMessageInDisk(1024 * 1024);
        messageStoreConfig.setMaxTransferBytesOnMessageInMemory(1024 * 1024);
        messageStoreConfig.setMaxTransferCountOnMessageInDisk(1024);
        messageStoreConfig.setMaxTransferCountOnMessageInMemory(1024);

        messageStoreConfig.setFlushIntervalCommitLog(1);
        messageStoreConfig.setFlushCommitLogThoroughInterval(2);
        messageStoreConfig.setHaListenPort(nextPort());
        return new DefaultMessageStore(
            messageStoreConfig,
            new BrokerStatsManager("simpleTest", true),
            (topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties) -> {
            },
            new BrokerConfig(), topicConfigTable);
    }

    public MessageExtBrokerInner buildMessage(String topic, int batchNum) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(new byte[1024]);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(0);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(storeHost);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_INNER_NUM, String.valueOf(batchNum));
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        if (batchNum > 1) {
            msg.setSysFlag(MessageSysFlag.INNER_BATCH_FLAG);
        }
        if (batchNum == -1) {
            MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_INNER_NUM);
        }
        return msg;
    }
}
