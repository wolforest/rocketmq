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

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.Map;

public class ConsumerManager {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerManager.class);

    public static PushConsumer buildPushConsumer(String group, Map<String, FilterExpression> filter, MessageListener listener) {
        return buildPushConsumer(null, group, filter, listener);
    }

    public static PushConsumer buildPushConsumer(String accountName, String group, Map<String, FilterExpression> filter, MessageListener listener) {
        ClientConfiguration clientConfig = ConfigManager.buildClientConfig(accountName);

        try {
            return ClientManager.getProvider()
                .newPushConsumerBuilder()
                .setClientConfiguration(clientConfig)
                .setConsumerGroup(group)
                .setSubscriptionExpressions(filter)
                .setMessageListener(listener)
                .build();
        } catch (Exception e) {
            LOG.warn("can't connect to MQ server");
            return null;
        }
    }
}
