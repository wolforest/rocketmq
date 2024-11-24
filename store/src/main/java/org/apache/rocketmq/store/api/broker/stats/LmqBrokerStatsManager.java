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
package org.apache.rocketmq.store.api.broker.stats;

import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.MQConstants;

public class LmqBrokerStatsManager extends BrokerStatsManager {

    private final BrokerConfig brokerConfig;

    public LmqBrokerStatsManager(BrokerConfig brokerConfig) {
        super(brokerConfig.getBrokerClusterName(), brokerConfig.isEnableDetailStat());
        this.brokerConfig = brokerConfig;
    }

    @Override
    public void incGroupGetNums(final String group, final String topic, final int incValue) {
        String lmqGroup = group;
        String lmqTopic = topic;
        if (!brokerConfig.isEnableLmqStats()) {
            if (MQConstants.isLmq(group)) {
                lmqGroup = MQConstants.LMQ_PREFIX;
            }
            if (MQConstants.isLmq(topic)) {
                lmqTopic = MQConstants.LMQ_PREFIX;
            }
        }
        super.incGroupGetNums(lmqGroup, lmqTopic, incValue);
    }

    @Override
    public void incGroupGetSize(final String group, final String topic, final int incValue) {
        String lmqGroup = group;
        String lmqTopic = topic;
        if (!brokerConfig.isEnableLmqStats()) {
            if (MQConstants.isLmq(group)) {
                lmqGroup = MQConstants.LMQ_PREFIX;
            }
            if (MQConstants.isLmq(topic)) {
                lmqTopic = MQConstants.LMQ_PREFIX;
            }
        }
        super.incGroupGetSize(lmqGroup, lmqTopic, incValue);
    }

    @Override
    public void incGroupAckNums(final String group, final String topic, final int incValue) {
        String lmqGroup = group;
        String lmqTopic = topic;
        if (!brokerConfig.isEnableLmqStats()) {
            if (MQConstants.isLmq(group)) {
                lmqGroup = MQConstants.LMQ_PREFIX;
            }
            if (MQConstants.isLmq(topic)) {
                lmqTopic = MQConstants.LMQ_PREFIX;
            }
        }
        super.incGroupAckNums(lmqGroup, lmqTopic, incValue);
    }

    @Override
    public void incGroupCkNums(final String group, final String topic, final int incValue) {
        String lmqGroup = group;
        String lmqTopic = topic;
        if (!brokerConfig.isEnableLmqStats()) {
            if (MQConstants.isLmq(group)) {
                lmqGroup = MQConstants.LMQ_PREFIX;
            }
            if (MQConstants.isLmq(topic)) {
                lmqTopic = MQConstants.LMQ_PREFIX;
            }
        }
        super.incGroupCkNums(lmqGroup, lmqTopic, incValue);
    }

    @Override
    public void incGroupGetLatency(final String group, final String topic, final int queueId, final int incValue) {
        String lmqGroup = group;
        String lmqTopic = topic;
        if (!brokerConfig.isEnableLmqStats()) {
            if (MQConstants.isLmq(group)) {
                lmqGroup = MQConstants.LMQ_PREFIX;
            }
            if (MQConstants.isLmq(topic)) {
                lmqTopic = MQConstants.LMQ_PREFIX;
            }
        }
        super.incGroupGetLatency(lmqGroup, lmqTopic, queueId, incValue);
    }

    @Override
    public void incSendBackNums(final String group, final String topic) {
        String lmqGroup = group;
        String lmqTopic = topic;
        if (!brokerConfig.isEnableLmqStats()) {
            if (MQConstants.isLmq(group)) {
                lmqGroup = MQConstants.LMQ_PREFIX;
            }
            if (MQConstants.isLmq(topic)) {
                lmqTopic = MQConstants.LMQ_PREFIX;
            }
        }
        super.incSendBackNums(lmqGroup, lmqTopic);
    }

    @Override
    public double tpsGroupGetNums(final String group, final String topic) {
        String lmqGroup = group;
        String lmqTopic = topic;
        if (!brokerConfig.isEnableLmqStats()) {
            if (MQConstants.isLmq(group)) {
                lmqGroup = MQConstants.LMQ_PREFIX;
            }
            if (MQConstants.isLmq(topic)) {
                lmqTopic = MQConstants.LMQ_PREFIX;
            }
        }
        return super.tpsGroupGetNums(lmqGroup, lmqTopic);
    }

    @Override
    public void recordDiskFallBehindTime(final String group, final String topic, final int queueId,
        final long fallBehind) {
        String lmqGroup = group;
        String lmqTopic = topic;
        if (!brokerConfig.isEnableLmqStats()) {
            if (MQConstants.isLmq(group)) {
                lmqGroup = MQConstants.LMQ_PREFIX;
            }
            if (MQConstants.isLmq(topic)) {
                lmqTopic = MQConstants.LMQ_PREFIX;
            }
        }
        super.recordDiskFallBehindTime(lmqGroup, lmqTopic, queueId, fallBehind);
    }

    @Override
    public void recordDiskFallBehindSize(final String group, final String topic, final int queueId,
        final long fallBehind) {
        String lmqGroup = group;
        String lmqTopic = topic;
        if (!brokerConfig.isEnableLmqStats()) {
            if (MQConstants.isLmq(group)) {
                lmqGroup = MQConstants.LMQ_PREFIX;
            }
            if (MQConstants.isLmq(topic)) {
                lmqTopic = MQConstants.LMQ_PREFIX;
            }
        }
        super.recordDiskFallBehindSize(lmqGroup, lmqTopic, queueId, fallBehind);
    }

}
