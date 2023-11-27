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
package org.apache.rocketmq.proxy.grpc.v2.producer;

import apache.rocketmq.v2.SendMessageRequest;
import com.google.common.hash.Hashing;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.QueueSelector;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;

public class SendMessageQueueSelector implements QueueSelector {

    private final SendMessageRequest request;

    public SendMessageQueueSelector(SendMessageRequest request) {
        this.request = request;
    }

    @Override
    public AddressableMessageQueue select(ProxyContext ctx, MessageQueueView messageQueueView) {
        try {
            apache.rocketmq.v2.Message message = request.getMessages(0);
            String shardingKey = null;
            if (request.getMessagesCount() == 1) {
                shardingKey = message.getSystemProperties().getMessageGroup();
            }
            AddressableMessageQueue targetMessageQueue;
            if (StringUtils.isNotEmpty(shardingKey)) {
                // With shardingKey
                List<AddressableMessageQueue> writeQueues = messageQueueView.getWriteSelector().getQueues();
                int bucket = Hashing.consistentHash(shardingKey.hashCode(), writeQueues.size());
                targetMessageQueue = writeQueues.get(bucket);
            } else {
                targetMessageQueue = messageQueueView.getWriteSelector().selectOneByPipeline(false);
            }
            return targetMessageQueue;
        } catch (Exception e) {
            return null;
        }
    }
}
