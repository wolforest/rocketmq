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
package org.apache.rocketmq.common.app;

import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.domain.constant.PopConstants;

public class KeyBuilder {
    public static final int POP_ORDER_REVIVE_QUEUE = 999;
    private static final char POP_RETRY_SEPARATOR_V1 = '_';
    private static final char POP_RETRY_SEPARATOR_V2 = '+';
    private static final String POP_RETRY_REGEX_SEPARATOR_V2 = "\\+";

    /**
     * create retryTopicName by original topic, group
     *
     * @param topic original topic
     * @param group original group
     * @return retryTopicName
     */
    public static String buildPopRetryTopic(String topic, String group) {
        return MQConstants.RETRY_GROUP_TOPIC_PREFIX + group + POP_RETRY_SEPARATOR_V2 + topic;
    }

    public static String buildPopRetryTopicV1(String topic, String group) {
        return MQConstants.RETRY_GROUP_TOPIC_PREFIX + group + POP_RETRY_SEPARATOR_V1 + topic;
    }

    public static String parseNormalTopic(String topic, String cid) {
        if (!topic.startsWith(MQConstants.RETRY_GROUP_TOPIC_PREFIX)) {
            return topic;
        }

        if (topic.startsWith(MQConstants.RETRY_GROUP_TOPIC_PREFIX + cid + POP_RETRY_SEPARATOR_V2)) {
            return topic.substring((MQConstants.RETRY_GROUP_TOPIC_PREFIX + cid + POP_RETRY_SEPARATOR_V2).length());
        }
        return topic.substring((MQConstants.RETRY_GROUP_TOPIC_PREFIX + cid + POP_RETRY_SEPARATOR_V1).length());
    }

    public static String parseNormalTopic(String retryTopic) {
        if (!isPopRetryTopicV2(retryTopic)) {
            return retryTopic;
        }

        String[] result = retryTopic.split(POP_RETRY_REGEX_SEPARATOR_V2);
        if (result.length == 2) {
            return result[1];
        }

        return retryTopic;
    }

    public static String parseGroup(String retryTopic) {
        if (!isPopRetryTopicV2(retryTopic)) {
            return retryTopic.substring(MQConstants.RETRY_GROUP_TOPIC_PREFIX.length());
        }

        String[] result = retryTopic.split(POP_RETRY_REGEX_SEPARATOR_V2);
        if (result.length == 2) {
            return result[0].substring(MQConstants.RETRY_GROUP_TOPIC_PREFIX.length());
        }

        return retryTopic.substring(MQConstants.RETRY_GROUP_TOPIC_PREFIX.length());
    }

    public static boolean isPopRetryTopicV2(String retryTopic) {
        return retryTopic.startsWith(MQConstants.RETRY_GROUP_TOPIC_PREFIX) && retryTopic.contains(String.valueOf(POP_RETRY_SEPARATOR_V2));
    }

    /**
     * remove retry topic prefix and group
     * @renamed from parseNormalTopic removeRetryPrefix
     *
     * @param topic topic
     * @param group group
     * @return topic name
     */
    public static String removeRetryPrefix(String topic, String group) {
        if (!topic.startsWith(MQConstants.RETRY_GROUP_TOPIC_PREFIX)) {
            return topic;
        }

        return topic.substring((MQConstants.RETRY_GROUP_TOPIC_PREFIX + group + "_").length());
    }

    /**
     * @renamed from buildPollingKey to buildConsumeKey
     */
    public static String buildConsumeKey(String topic, String group, int queueId) {
        return topic + PopConstants.SPLIT + group + PopConstants.SPLIT + queueId;
    }

    public static String buildPollingNotificationKey(String topic, int queueId) {
        return topic + PopConstants.SPLIT + queueId;
    }

    /**
     * Build cluster revive topic
     *
     * @param clusterName cluster name
     * @return revive topic
     */
    public static String buildClusterReviveTopic(String clusterName) {
        return PopConstants.REVIVE_TOPIC + clusterName;
    }
}
