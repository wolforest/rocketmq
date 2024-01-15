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
package org.apache.rocketmq.store.queue;

import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.domain.consumer.CQType;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

public class ConsumeQueueStoreTest extends QueueTestBase {

    @Test
    public void testLoadConsumeQueuesWithWrongAttribute() throws Exception {
        ConcurrentMap<String, TopicConfig> topicConfigTableMap = new ConcurrentHashMap<String, TopicConfig>();
        MessageStore messageStore = createMessageStore(null, true, topicConfigTableMap);
        messageStore.load();
        messageStore.start();

        String normalTopic = UUID.randomUUID().toString();
        ConcurrentMap<String, TopicConfig> topicConfigTable = createTopicConfigTable(normalTopic, CQType.SimpleCQ);
        topicConfigTableMap.putAll(topicConfigTable);

        for (int i = 0; i < 10; i++) {
            PutMessageResult putMessageResult = messageStore.putMessage(buildMessage(normalTopic, -1));
            assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        }

        await().atMost(5, SECONDS).until(fullyDispatched(messageStore));

        // simulate delete topic but with files left.
        topicConfigTableMap.clear();

        topicConfigTable = createTopicConfigTable(normalTopic, CQType.BatchCQ);
        topicConfigTableMap.putAll(topicConfigTable);

        RuntimeException runtimeException = Assert.assertThrows(RuntimeException.class, () -> messageStore.getConsumeQueueStore().load());
        Assert.assertTrue(runtimeException.getMessage().endsWith("should be SimpleCQ, but is BatchCQ"));

        messageStore.shutdown();
        messageStore.destroy();
        File file = new File(messageStore.getMessageStoreConfig().getStorePathRootDir());
        IOUtils.deleteFile(file);
    }

    @Test
    public void testLoadBatchConsumeQueuesWithWrongAttribute() throws Exception {
        String batchTopic = UUID.randomUUID().toString();
        ConcurrentMap<String, TopicConfig>  topicConfigTable = createTopicConfigTable(batchTopic, CQType.BatchCQ);

        ConcurrentMap<String, TopicConfig> topicConfigTableMap = new ConcurrentHashMap<String, TopicConfig>();
        topicConfigTableMap.putAll(topicConfigTable);
        MessageStore messageStore = createMessageStore(null, true, topicConfigTableMap);
        messageStore.load();
        messageStore.start();

        for (int i = 0; i < 10; i++) {
            PutMessageResult putMessageResult = messageStore.putMessage(buildMessage(batchTopic, 10));
            assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        }

        await().atMost(5, SECONDS).until(fullyDispatched(messageStore));

        // simulate delete topic but with files left.
        topicConfigTableMap.clear();

        topicConfigTable = createTopicConfigTable(batchTopic, CQType.SimpleCQ);
        topicConfigTableMap.putAll(topicConfigTable);
        messageStore.shutdown();

        RuntimeException runtimeException = Assert.assertThrows(RuntimeException.class, () -> messageStore.getConsumeQueueStore().load());
        Assert.assertTrue(runtimeException.getMessage().endsWith("should be BatchCQ, but is SimpleCQ"));

        messageStore.shutdown();
        messageStore.destroy();
        File file = new File(messageStore.getMessageStoreConfig().getStorePathRootDir());
        IOUtils.deleteFile(file);
    }

}
