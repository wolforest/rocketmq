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
package org.apache.rocketmq.broker.topic;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.domain.constant.PermName;
import org.apache.rocketmq.common.domain.constant.MQConstants;

public class LmqTopicConfigManager extends TopicConfigManager {
    public LmqTopicConfigManager(BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public TopicConfig selectTopicConfig(final String topic) {
        if (MQConstants.isLmq(topic)) {
            return simpleLmqTopicConfig(topic);
        }
        return super.selectTopicConfig(topic);
    }

    @Override
    public void updateTopicConfig(final TopicConfig topicConfig) {
        if (topicConfig == null || MQConstants.isLmq(topicConfig.getTopicName())) {
            return;
        }
        super.updateTopicConfig(topicConfig);
    }

    private TopicConfig simpleLmqTopicConfig(String topic) {
        return new TopicConfig(topic, 1, 1, PermName.PERM_READ | PermName.PERM_WRITE);
    }

    @Override
    public boolean containsTopic(String topic) {
        if (MQConstants.isLmq(topic)) {
            return true;
        }
        return super.containsTopic(topic);
    }

}
