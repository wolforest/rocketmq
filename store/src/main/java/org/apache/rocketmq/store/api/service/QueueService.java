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
package org.apache.rocketmq.store.api.service;

import org.apache.rocketmq.common.lang.BoundaryType;
import org.apache.rocketmq.store.domain.queue.ConsumeQueueService;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;

public class QueueService {
    private final ConsumeQueueService consumeQueueService;

    public QueueService(DefaultMessageStore context) {
        consumeQueueService = context.getConsumeQueueService();
    }

    public long getMaxOffsetInQueue(String topic, int queueId) {
        return consumeQueueService.getMaxOffsetInQueue(topic, queueId);
    }

    public long getMaxOffsetInQueue(String topic, int queueId, boolean committed) {
        return consumeQueueService.getMaxOffsetInQueue(topic, queueId, committed);
    }

    public long getMinOffsetInQueue(String topic, int queueId) {
        return consumeQueueService.getMinOffsetInQueue(topic, queueId);
    }

    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        return consumeQueueService.getCommitLogOffsetInQueue(topic, queueId, consumeQueueOffset);
    }

    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        return consumeQueueService.getOffsetInQueueByTime(topic, queueId, timestamp);
    }

    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType boundaryType) {
        return consumeQueueService.getOffsetInQueueByTime(topic, queueId, timestamp, boundaryType);
    }
}
