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
package org.apache.rocketmq.common;

import org.apache.rocketmq.common.constant.MQConstants;

public class KeyBuilder {
    public static final int POP_ORDER_REVIVE_QUEUE = 999;

    /**
     * create retryTopicName by original topic, group
     *
     * @param topic original topic
     * @param group original group
     * @return retryTopicName
     */
    public static String buildPopRetryTopic(String topic, String group) {
        return MQConstants.RETRY_GROUP_TOPIC_PREFIX + group + "_" + topic;
    }

    public static String parseNormalTopic(String topic, String cid) {
        if (topic.startsWith(MQConstants.RETRY_GROUP_TOPIC_PREFIX)) {
            return topic.substring((MQConstants.RETRY_GROUP_TOPIC_PREFIX + cid + "_").length());
        } else {
            return topic;
        }
    }

    /**
     * @renamed from buildPollingKey to buildConsumeKey
     */
    public static String buildConsumeKey(String topic, String group, int queueId) {
        return topic + PopAckConstants.SPLIT + group + PopAckConstants.SPLIT + queueId;
    }

    public static String buildPollingNotificationKey(String topic, int queueId) {
        return topic + PopAckConstants.SPLIT + queueId;
    }
}
