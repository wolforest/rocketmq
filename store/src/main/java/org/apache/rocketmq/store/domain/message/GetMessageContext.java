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
package org.apache.rocketmq.store.domain.message;

import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.api.service.GetMessageService;
import org.apache.rocketmq.store.server.DefaultMessageStore;
import org.apache.rocketmq.store.api.dto.GetMessageResult;
import org.apache.rocketmq.store.api.dto.GetMessageStatus;
import org.apache.rocketmq.store.api.filter.MessageFilter;
import org.apache.rocketmq.store.domain.queue.ConsumeQueueInterface;

public class GetMessageContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private long nextBeginOffset;
    private long maxOffsetPy;
    private long maxPullSize;
    private long maxPhyOffsetPulling;
    private int maxFilterMessageSize;

    private long nextPhyFileStartOffset;

    private DefaultMessageStore messageStore;
    private ConsumeQueueInterface consumeQueue;
    private GetMessageResult getResult;

    private final String group;
    private final String topic;
    private final int queueId;
    private final long offset;
    private final int maxMsgNums;
    private final int maxTotalMsgSize;
    private final MessageFilter messageFilter;

    public GetMessageContext(
        DefaultMessageStore messageStore,
        ConsumeQueueInterface consumeQueue,
        GetMessageResult getResult,
        final String group,
        final String topic,
        final int queueId,
        final long offset,
        final int maxMsgNums,
        final int maxTotalMsgSize,
        final MessageFilter messageFilter) {

        this.messageStore = messageStore;
        this.consumeQueue = consumeQueue;
        this.getResult = getResult;
        this.group = group;
        this.topic = topic;
        this.queueId = queueId;
        this.offset = offset;
        this.maxMsgNums = maxMsgNums;
        this.maxTotalMsgSize = maxTotalMsgSize;
        this.messageFilter = messageFilter;

        init();
    }

    public GetMessageResult toGetResult() {
        getResult.setNextBeginOffset(nextBeginOffset);
        getResult.setMaxOffset(consumeQueue.getMaxOffsetInQueue());
        getResult.setMinOffset(consumeQueue.getMinOffsetInQueue());
        return getResult;
    }

    private void init() {
        this.maxFilterMessageSize = Math.max(16000, maxMsgNums * consumeQueue.getUnitSize());

        this.nextBeginOffset = offset;
        this.maxOffsetPy = messageStore.getCommitLog().getMaxOffset();
        this.maxPhyOffsetPulling = 0;
        initMaxPullSize(topic, queueId, maxTotalMsgSize);

        getResult.setStatus(GetMessageStatus.NO_MATCHED_MESSAGE);
    }

    private void initMaxPullSize(final String topic, final int queueId, final int maxTotalMsgSize) {
        this.maxPullSize = Math.max(maxTotalMsgSize, 100);
        if (maxPullSize <= GetMessageService.MAX_PULL_MSG_SIZE) {
            return;
        }

        LOGGER.warn("The max pull size is too large maxPullSize={} topic={} queueId={}", maxPullSize, topic, queueId);
        this.maxPullSize = GetMessageService.MAX_PULL_MSG_SIZE;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public void setMaxOffsetPy(long maxOffsetPy) {
        this.maxOffsetPy = maxOffsetPy;
    }

    public void setMaxPullSize(long maxPullSize) {
        this.maxPullSize = maxPullSize;
    }

    public void setMaxPhyOffsetPulling(long maxPhyOffsetPulling) {
        this.maxPhyOffsetPulling = maxPhyOffsetPulling;
    }

    public void setMaxFilterMessageSize(int maxFilterMessageSize) {
        this.maxFilterMessageSize = maxFilterMessageSize;
    }

    public long getNextPhyFileStartOffset() {
        return nextPhyFileStartOffset;
    }

    public void setNextPhyFileStartOffset(long nextPhyFileStartOffset) {
        this.nextPhyFileStartOffset = nextPhyFileStartOffset;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public long getMaxOffsetPy() {
        return maxOffsetPy;
    }

    public long getMaxPullSize() {
        return maxPullSize;
    }

    public long getMaxPhyOffsetPulling() {
        return maxPhyOffsetPulling;
    }

    public int getMaxFilterMessageSize() {
        return maxFilterMessageSize;
    }

    public DefaultMessageStore getMessageStore() {
        return messageStore;
    }

    public ConsumeQueueInterface getConsumeQueue() {
        return consumeQueue;
    }

    public GetMessageResult getGetResult() {
        return getResult;
    }

    public String getGroup() {
        return group;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getOffset() {
        return offset;
    }

    public int getMaxMsgNums() {
        return maxMsgNums;
    }

    public int getMaxTotalMsgSize() {
        return maxTotalMsgSize;
    }

    public MessageFilter getMessageFilter() {
        return messageFilter;
    }
}
