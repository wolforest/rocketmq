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
package org.apache.rocketmq.client.impl.consumer;

import java.util.Set;
import org.apache.rocketmq.common.domain.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.domain.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/**
 * Consumer inner interface
 */
public interface MQConsumerInner {
    String groupName();

    MessageModel messageModel();

    ConsumeType consumeType();

    ConsumeFromWhere consumeFromWhere();

    Set<SubscriptionData> subscriptions();

    void doRebalance();

    boolean tryRebalance();

    void persistConsumerOffset();

    void updateTopicSubscribeInfo(final String topic, final Set<MessageQueue> info);

    boolean isSubscribeTopicNeedUpdate(final String topic);

    boolean isUnitMode();

    ConsumerRunningInfo consumerRunningInfo();
}
