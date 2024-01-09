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
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.domain.topic.TopicMessageType;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test( groups = {"admin"})
public class TopicTest extends ApiBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(TopicTest.class);

    @BeforeMethod
    public void beforeMethod() throws Throwable {
    }

    @AfterMethod
    public void afterMethod() {
    }

    @Test
    public void testNormalTopic() {
        String topic = TopicManager.createUniqueTopic();
        boolean status = TopicManager.createTopic(topic);
        if (!status) {
            return;
        }

        TopicConfig topicConfig = TopicManager.findTopic(topic);
        Assert.assertNotNull(topicConfig);
        Assert.assertEquals(topic, topicConfig.getTopicName());
        Assert.assertEquals(TopicMessageType.NORMAL, topicConfig.getTopicMessageType());

        TopicManager.deleteTopic(topic);

        TopicConfig config2 = TopicManager.findTopic(topic);
        Assert.assertNull(config2);
    }

    @Test
    public void testDelayTopic() {
        String topic = TopicManager.createUniqueTopic();
        boolean status = TopicManager.createDelayTopic(topic);
        if (!status) {
            return;
        }

        TopicConfig topicConfig = TopicManager.findTopic(topic);
        Assert.assertNotNull(topicConfig);
        Assert.assertEquals(topic, topicConfig.getTopicName());
        Assert.assertEquals(TopicMessageType.DELAY, topicConfig.getTopicMessageType());

        TopicManager.deleteTopic(topic);

        TopicConfig config2 = TopicManager.findTopic(topic);
        Assert.assertNull(config2);
    }

    @Test
    public void testTransactionTopic() {
        String topic = TopicManager.createUniqueTopic();
        boolean status = TopicManager.createTransactionalTopic(topic);
        if (!status) {
            return;
        }

        TopicConfig topicConfig = TopicManager.findTopic(topic);
        Assert.assertNotNull(topicConfig);
        Assert.assertEquals(topic, topicConfig.getTopicName());
        Assert.assertEquals(TopicMessageType.TRANSACTION, topicConfig.getTopicMessageType());

        TopicManager.deleteTopic(topic);

        TopicConfig config2 = TopicManager.findTopic(topic);
        Assert.assertNull(config2);
    }
}
