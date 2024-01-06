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
package org.apache.rocketmq.apitest.manager;

import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.common.domain.topic.TopicMessageType;
import org.apache.rocketmq.common.domain.topic.TopicAttributes;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.utils.StringUtils;

public class TopicManager {
    private static final String TOPIC_PREFIX = "MQT_";

    public static TopicConfig findTopic(String topic) {
        try {
            String brokerAddr = ConfigManager.getConfig().getString("brokerAddr");

            return ClientManager.getClient().getTopicConfig(brokerAddr, topic);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void deleteTopic(String topic) {
        try {
            String brokerAddr = ConfigManager.getConfig().getString("brokerAddr");
            ClientManager.getClient().deleteTopicInBroker(brokerAddr, topic);

            String nameAddr = ConfigManager.getConfig().getString("nameAddr");
            ClientManager.getClient().deleteTopicInNameServer(nameAddr, topic);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createTopic(String topic) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("+" + TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), TopicMessageType.NORMAL.getValue());
        createTopic(topic, attributes);
    }

    public static void createFIFOTopic(String topic) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("+" + TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), TopicMessageType.FIFO.getValue());
        createTopic(topic, attributes);
    }

    public static void createDelayTopic(String topic) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("+" + TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), TopicMessageType.DELAY.getValue());
        createTopic(topic, attributes);
    }

    public static void createTransactionalTopic(String topic) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("+" + TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), TopicMessageType.TRANSACTION.getValue());
        createTopic(topic, attributes);
    }

    public static void createTopic(String topic, Map<String, String> attributes) {
        try {
            String brokerAddr = ConfigManager.getConfig().getString("brokerAddr");
            TopicConfig topicConfig = new TopicConfig(topic);
            topicConfig.setAttributes(attributes);

            ClientManager.getClient().createTopic(brokerAddr, topicConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String createUniqueTopic() {
        return TOPIC_PREFIX + StringUtils.UUID();
    }
}
