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
package org.apache.rocketmq.apitest.admin;

import org.apache.rocketmq.apitest.ApiBaseTest;
import org.apache.rocketmq.apitest.manager.TopicManager;
import org.apache.rocketmq.common.lang.attribute.TopicMessageType;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TopicTest extends ApiBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(TopicTest.class);

    @Before
    public void before() throws Throwable {
        super.before();
    }

    @After
    public void after() {
        super.after();
    }

    @Test
    public void testNormalTopic() {
        String topic = TopicManager.createUniqueTopic();
        TopicManager.createTopic(topic);

        TopicConfig topicConfig = TopicManager.findTopic(topic);
        assertNotNull(topicConfig);
        assertEquals(topic, topicConfig.getTopicName());
        assertEquals(TopicMessageType.NORMAL, topicConfig.getTopicMessageType());

        TopicManager.deleteTopic(topic);

        TopicConfig config2 = TopicManager.findTopic(topic);
        assertNull(config2);
    }

    @Test
    public void testDelayTopic() {
        String topic = TopicManager.createUniqueTopic();
        TopicManager.createDelayTopic(topic);

        TopicConfig topicConfig = TopicManager.findTopic(topic);
        assertNotNull(topicConfig);
        assertEquals(topic, topicConfig.getTopicName());
        assertEquals(TopicMessageType.DELAY, topicConfig.getTopicMessageType());

        TopicManager.deleteTopic(topic);

        TopicConfig config2 = TopicManager.findTopic(topic);
        assertNull(config2);
    }

    @Test
    public void testTransactionTopic() {
        String topic = TopicManager.createUniqueTopic();
        TopicManager.createTransactionalTopic(topic);

        TopicConfig topicConfig = TopicManager.findTopic(topic);
        assertNotNull(topicConfig);
        assertEquals(topic, topicConfig.getTopicName());
        assertEquals(TopicMessageType.TRANSACTION, topicConfig.getTopicMessageType());

        TopicManager.deleteTopic(topic);

        TopicConfig config2 = TopicManager.findTopic(topic);
        assertNull(config2);
    }
}
