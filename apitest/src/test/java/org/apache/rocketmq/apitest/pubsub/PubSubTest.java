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
package org.apache.rocketmq.apitest.pubsub;

import org.apache.rocketmq.apitest.ApiBaseTest;
import org.apache.rocketmq.apitest.manager.ClientManager;
import org.apache.rocketmq.apitest.manager.ConsumerManager;
import org.apache.rocketmq.apitest.manager.ProducerManager;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class PubSubTest extends ApiBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubTest.class);
    private static final String ACCOUNT_NAME = "default";
    private static final String TOPIC = "MQ_TEST_TOPIC";
    private static final String CONSUMER_GROUP = "MQ_TEST_GROUP";
    private static final String MESSAGE_PREFIX = "MQ_TEST_KEY_PREFIX_";
    private static final String MESSAGE_BODY = "test message body: ";

    private PushConsumer consumer;
    private Producer producer;


    @Before
    public void before() throws Throwable {
        super.before();

        createProducer();
        startConsumer();
    }

    @Test
    public void testSendOk() {
        if (producer == null) {
            return;
        }

        for (int i = 0; i < 100; i++) {
            Message message = createMessage(i);

            try {
                SendReceipt sendReceipt = producer.send(message);
                assertNotNull(sendReceipt);
                LOG.info("pub message: {}", message);
            } catch (Throwable t) {
                LOG.error("Failed to send message: {}", i, t);
            }
        }
    }


    @After
    public void after() {
        super.after();

        try {
            stopConsumer();
            stopProducer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Map<String, FilterExpression> createFilter() {
        FilterExpression expression = new FilterExpression("*");
        return Collections.singletonMap(TOPIC, expression);
    }

    private void createProducer() {
        producer = ProducerManager.buildProducer(TOPIC);
    }

    private void startConsumer() {
        LOG.info("create consumer");
        consumer = ConsumerManager.buildPushConsumer(CONSUMER_GROUP, createFilter(), createListener());
    }

    private void stopConsumer() throws IOException {
        if (consumer == null) {
            return;
        }

        ThreadUtils.sleep(30000);
        LOG.info("stop consumer");

        consumer.close();
    }

    private void stopProducer() throws IOException {
        if (producer == null) {
            return;
        }

        LOG.info("stop producer");
        producer.close();
    }

    private MessageListener createListener() {
        LOG.info("create consume listener");
        return message -> {
            LOG.info("Consume message={}", message);
            return ConsumeResult.SUCCESS;
        };
    }

    private Message createMessage(int i) {
        return ClientManager.getProvider()
            .newMessageBuilder()
            .setTopic(TOPIC)
            .setKeys(MESSAGE_PREFIX + i)
            .setBody((MESSAGE_BODY + i).getBytes(StandardCharsets.UTF_8))
            .build();
    }

}
