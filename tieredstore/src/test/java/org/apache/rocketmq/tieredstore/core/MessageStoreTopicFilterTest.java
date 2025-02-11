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
package org.apache.rocketmq.tieredstore.core;

import org.apache.rocketmq.common.domain.topic.TopicValidator;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Test;

public class MessageStoreTopicFilterTest {

    @Test
    public void filterTopicTest() {
        MessageStoreFilter topicFilter = new MessageStoreTopicFilter(new MessageStoreConfig());
        Assert.assertTrue(topicFilter.filterTopic(""));
        Assert.assertTrue(topicFilter.filterTopic(TopicValidator.SYSTEM_TOPIC_PREFIX + "_Topic"));

        String topicName = "WhiteTopic";
        Assert.assertFalse(topicFilter.filterTopic(topicName));
        topicFilter.addTopicToBlackList(topicName);
        Assert.assertTrue(topicFilter.filterTopic(topicName));
    }
}
