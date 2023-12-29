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
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.apache.rocketmq.client.apis.producer.TransactionChecker;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;


public class ProducerManager {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerManager.class);

    public static Producer buildProducer(String... topics) {
        return buildProducer(null, null, topics);
    }

    public static Producer buildTransactionalProducer(TransactionChecker checker, String... topics) {
        return buildProducer(null, checker, topics);
    }

    public static Producer buildTransactionalProducer(String accountName, TransactionChecker checker, String... topics) {
        return buildProducer(accountName, checker, topics);
    }

    public static Producer buildProducer(String accountName, TransactionChecker checker, String... topics) {
        ClientConfiguration clientConfig = ConfigManager.buildClientConfig(accountName);

        try {
            ProducerBuilder builder = ClientManager.getProvider()
                .newProducerBuilder()
                .setClientConfiguration(clientConfig);

            if (null != topics && topics.length > 0) {
                builder.setTopics(topics);
            }

            if (null != checker) {
                builder.setTransactionChecker(checker);
            }

            return builder.build();
        } catch (Exception e) {
            LOG.warn("can't connect to MQ server");
            return null;
        }
    }
}
