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
package org.apache.rocketmq.store.domain.dispatcher;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.domain.message.MessageAccessor;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;

/**
 * MultiDispatch for lmq, not-thread-safe
 */
public class MultiDispatch {
    private final StringBuilder keyBuilder = new StringBuilder();
    private final DefaultMessageStore messageStore;
    private static final short VALUE_OF_EACH_INCREMENT = 1;

    public MultiDispatch(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public String queueKey(String queueName, MessageExtBrokerInner msgInner) {
        keyBuilder.delete(0, keyBuilder.length());
        keyBuilder.append(queueName);
        keyBuilder.append('-');
        int queueId = msgInner.getQueueId();
        if (messageStore.getMessageStoreConfig().isEnableLmq() && MQConstants.isLmq(queueName)) {
            queueId = 0;
        }
        keyBuilder.append(queueId);
        return keyBuilder.toString();
    }

    public void wrapMultiDispatch(final MessageExtBrokerInner msg) {

        String multiDispatchQueue = msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String[] queues = multiDispatchQueue.split(MQConstants.MULTI_DISPATCH_QUEUE_SPLITTER);
        Long[] queueOffsets = new Long[queues.length];
        if (messageStore.getMessageStoreConfig().isEnableLmq()) {
            for (int i = 0; i < queues.length; i++) {
                String key = queueKey(queues[i], msg);
                if (MQConstants.isLmq(key)) {
                    queueOffsets[i] = messageStore.getConsumeQueueStore().getLmqQueueOffset(key);
                }
            }
        }
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET,
                StringUtils.join(queueOffsets, MQConstants.MULTI_DISPATCH_QUEUE_SPLITTER));
        msg.removeWaitStorePropertyString();
    }

    public void updateMultiQueueOffset(final MessageExtBrokerInner msgInner) {
        String multiDispatchQueue = msgInner.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String[] queues = multiDispatchQueue.split(MQConstants.MULTI_DISPATCH_QUEUE_SPLITTER);
        for (String queue : queues) {
            String key = queueKey(queue, msgInner);
            if (messageStore.getMessageStoreConfig().isEnableLmq() && MQConstants.isLmq(key)) {
                messageStore.getConsumeQueueStore().increaseLmqOffset(key, VALUE_OF_EACH_INCREMENT);
            }
        }
    }
}