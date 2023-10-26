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
package org.apache.rocketmq.store.service;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CleanupPolicy;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.CleanupPolicyUtils;
import org.apache.rocketmq.common.utils.MQUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.logfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.StoreUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.ReferredIterator;
import org.rocksdb.RocksDBException;

public class GetMessageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // Max pull msg size
    public final static int MAX_PULL_MSG_SIZE = 128 * 1024 * 1024;

    private final DefaultMessageStore messageStore;

    public GetMessageService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
        final int maxMsgNums, final MessageFilter messageFilter) {
        return getMessage(group, topic, queueId, offset, maxMsgNums, MAX_PULL_MSG_SIZE, messageFilter);
    }

    public CompletableFuture<GetMessageResult> getMessageAsync(String group, String topic,
        int queueId, long offset, int maxMsgNums, MessageFilter messageFilter) {
        return CompletableFuture.completedFuture(getMessage(group, topic, queueId, offset, maxMsgNums, messageFilter));
    }

    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums, final int maxTotalMsgSize, final MessageFilter messageFilter) {
        if (!allowAccess()) {
            return null;
        }

        GetMessageResult compactionResult = getMessageFromCompactionStore(group, topic, queueId, offset, maxMsgNums, maxTotalMsgSize);
        if (compactionResult != null) {
            return compactionResult;
        }

        long beginTime = messageStore.getSystemClock().now();
        GetMessageResult getResult = getMessageFromQueue(group, topic, queueId, offset, maxMsgNums, maxTotalMsgSize, messageFilter);

        setMonitorMatrix(getResult.getStatus(), beginTime);
        return getResult;
    }

    public CompletableFuture<GetMessageResult> getMessageAsync(String group, String topic,
        int queueId, long offset, int maxMsgNums, int maxTotalMsgSize, MessageFilter messageFilter) {
        return CompletableFuture.completedFuture(getMessage(group, topic, queueId, offset, maxMsgNums, maxTotalMsgSize, messageFilter));
    }

    private boolean allowAccess() {
        if (messageStore.isShutdown()) {
            LOGGER.warn("message store has shutdown, so getMessage is forbidden");
            return false;
        }

        if (!messageStore.getRunningFlags().isReadable()) {
            LOGGER.warn("message store is not readable, so getMessage is forbidden " + messageStore.getRunningFlags().getFlagBits());
            return false;
        }

        return true;
    }

    private GetMessageResult getMessageFromCompactionStore(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums, final int maxTotalMsgSize) {
        Optional<TopicConfig> topicConfig = messageStore.getTopicConfig(topic);
        CleanupPolicy policy = CleanupPolicyUtils.getDeletePolicy(topicConfig);
        //check request topic flag
        if (Objects.equals(policy, CleanupPolicy.COMPACTION) && messageStore.getMessageStoreConfig().isEnableCompaction()) {
            return messageStore.getCompactionStore().getMessage(group, topic, queueId, offset, maxMsgNums, maxTotalMsgSize);
        }

        return null;
    }

    private GetMessageResult handleQueueNotFound(GetMessageResult getResult, final long offset) {
        getResult.setStatus(GetMessageStatus.NO_MATCHED_LOGIC_QUEUE);
        getResult.setNextBeginOffset(nextOffsetCorrection(offset, 0));
        getResult.setMaxOffset(0);
        getResult.setMinOffset(0);
        return getResult;
    }

    private GetMessageResult handleNoMessageInQueue(GetMessageResult getResult, final long offset) {
        getResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
        getResult.setNextBeginOffset(nextOffsetCorrection(offset, 0));
        getResult.setMaxOffset(0);
        getResult.setMinOffset(0);
        return getResult;
    }

    private GetMessageResult handleOffsetTooSmall(GetMessageResult getResult, final long offset, ConsumeQueueInterface consumeQueue) {
        getResult.setStatus(GetMessageStatus.OFFSET_TOO_SMALL);
        getResult.setNextBeginOffset(nextOffsetCorrection(offset, consumeQueue.getMinOffsetInQueue()));
        getResult.setMaxOffset(0);
        getResult.setMinOffset(0);
        return getResult;
    }

    private GetMessageResult handleOffsetOverflowOne(GetMessageResult getResult, final long offset) {
        getResult.setStatus(GetMessageStatus.OFFSET_OVERFLOW_ONE);
        getResult.setNextBeginOffset(nextOffsetCorrection(offset, offset));
        getResult.setMaxOffset(0);
        getResult.setMinOffset(0);
        return getResult;
    }

    private GetMessageResult handleOffsetOverflowBadly(GetMessageResult getResult, final long offset, ConsumeQueueInterface consumeQueue) {
        getResult.setStatus(GetMessageStatus.OFFSET_OVERFLOW_BADLY);
        getResult.setNextBeginOffset(nextOffsetCorrection(offset, consumeQueue.getMaxOffsetInQueue()));
        getResult.setMaxOffset(0);
        getResult.setMinOffset(0);
        return getResult;
    }

    private GetMessageResult handleOffsetException(GetMessageResult getResult, final long offset, ConsumeQueueInterface consumeQueue) {
        if (consumeQueue == null) {
            return handleQueueNotFound(getResult, offset);
        }

        if (consumeQueue.getMaxOffsetInQueue() == 0) {
            return handleNoMessageInQueue(getResult, offset);
        }

        if (offset < consumeQueue.getMinOffsetInQueue()) {
            return handleOffsetTooSmall(getResult, offset, consumeQueue);
        }

        if (offset == consumeQueue.getMaxOffsetInQueue()) {
            return handleOffsetOverflowOne(getResult, offset);
        }

        if (offset > consumeQueue.getMaxOffsetInQueue()) {
            return handleOffsetOverflowBadly(getResult, offset, consumeQueue);
        }

        return null;
    }

    private void handleNoBufferQueue(GetMessageContext context) {
        context.getGetResult().setStatus(GetMessageStatus.OFFSET_FOUND_NULL);

        long tmpOffset = nextOffsetCorrection(context.getNextBeginOffset(), messageStore.getConsumeQueueStore().rollNextFile(context.getConsumeQueue(), context.getNextBeginOffset()));
        context.setNextBeginOffset(tmpOffset);
        LOGGER.warn("consumer request topic: " + context.getTopic() + "offset: " + context.getOffset() + " minOffset: " + context.getConsumeQueue().getMinOffsetInQueue() + " maxOffset: "
            + context.getConsumeQueue().getMaxOffsetInQueue() + ", but access logic queue failed. Correct nextBeginOffset to " + context.getNextBeginOffset());
    }

    private void handleBufferQueue(GetMessageContext context, ReferredIterator<CqUnit> bufferConsumeQueue) {
        try {
            context.setNextPhyFileStartOffset(Long.MIN_VALUE);
            while (bufferConsumeQueue.hasNext() && context.getNextBeginOffset() < context.getConsumeQueue().getMaxOffsetInQueue()) {
                CqUnit cqUnit = bufferConsumeQueue.next();
                boolean isInMem = estimateInMemByCommitOffset(cqUnit.getPos(), context.getMaxOffsetPy());

                if ((cqUnit.getQueueOffset() - context.getOffset()) * context.getConsumeQueue().getUnitSize() > context.getMaxFilterMessageSize()) {
                    break;
                }

                if (isTheBatchFull(cqUnit.getSize(), cqUnit.getBatchNum(), context.getMaxMsgNums(), context.getMaxPullSize(), context.getGetResult().getBufferTotalSize(), context.getGetResult().getMessageCount(), isInMem)) {
                    break;
                }

                if (context.getGetResult().getBufferTotalSize() >= context.getMaxPullSize()) {
                    break;
                }

                handleBufferQueueItem(context, cqUnit);
            }
        } finally {
            bufferConsumeQueue.release();
        }
    }

    private void handleBufferQueueItem(GetMessageContext context, CqUnit cqUnit) {
        context.setMaxPhyOffsetPulling(cqUnit.getPos());
        //Be careful, here should before the isTheBatchFull
        context.setNextBeginOffset(cqUnit.getQueueOffset() + cqUnit.getBatchNum());

        if (context.getNextPhyFileStartOffset() != Long.MIN_VALUE) {
            if (cqUnit.getPos() < context.getNextPhyFileStartOffset()) {
                return;
            }
        }

        if (context.getMessageFilter() != null
            && !context.getMessageFilter().isMatchedByConsumeQueue(cqUnit.getValidTagsCodeAsLong(), cqUnit.getCqExtUnit())) {
            if (context.getGetResult().getBufferTotalSize() == 0) {
                context.getGetResult().setStatus(GetMessageStatus.NO_MATCHED_MESSAGE);
            }

            return;
        }

        SelectMappedBufferResult selectResult = messageStore.getCommitLog().getMessage(cqUnit.getPos(), cqUnit.getSize());
        if (null == selectResult) {
            if (context.getGetResult().getBufferTotalSize() == 0) {
                context.getGetResult().setStatus(GetMessageStatus.MESSAGE_WAS_REMOVING);
            }

            context.setNextPhyFileStartOffset(messageStore.getCommitLog().rollNextFile(cqUnit.getPos()));
            return;
        }

        if (messageStore.getMessageStoreConfig().isColdDataFlowControlEnable() && !MQUtils.isSysConsumerGroupForNoColdReadLimit(context.getGroup()) && !selectResult.isInCache()) {
            context.getGetResult().setColdDataSum(context.getGetResult().getColdDataSum() + cqUnit.getSize());
        }

        if (context.getMessageFilter() != null
            && !context.getMessageFilter().isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {
            if (context.getGetResult().getBufferTotalSize() == 0) {
                context.getGetResult().setStatus(GetMessageStatus.NO_MATCHED_MESSAGE);
            }
            // release...
            selectResult.release();
            return;
        }
        messageStore.getStoreStatsService().getGetMessageTransferredMsgCount().add(cqUnit.getBatchNum());
        context.getGetResult().addMessage(selectResult, cqUnit.getQueueOffset(), cqUnit.getBatchNum());
        context.getGetResult().setStatus(GetMessageStatus.FOUND);
        context.setNextPhyFileStartOffset(Long.MIN_VALUE);
    }

    public GetMessageResult getMessageFromQueue(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums, final int maxTotalMsgSize, final MessageFilter messageFilter) {
        GetMessageResult getResult = new GetMessageResult();
        getResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);

        ConsumeQueueInterface consumeQueue = messageStore.findConsumeQueue(topic, queueId);
        GetMessageResult offsetResult = handleOffsetException(getResult, offset, consumeQueue);
        if (null != offsetResult) {
            return offsetResult;
        }

        GetMessageContext context = new GetMessageContext(messageStore, consumeQueue, getResult, group, topic, queueId, offset, maxMsgNums, maxTotalMsgSize, messageFilter);

        int cqFileNum = 0;
        while (getResult.getBufferTotalSize() <= 0 && context.getNextBeginOffset() < consumeQueue.getMaxOffsetInQueue() && cqFileNum++ < messageStore.getMessageStoreConfig().getTravelCqFileNumWhenGetMessage()) {
            ReferredIterator<CqUnit> bufferConsumeQueue = null;
            try {
                bufferConsumeQueue = consumeQueue.iterateFrom(context.getNextBeginOffset(), maxMsgNums);
                if (bufferConsumeQueue == null) {
                    handleNoBufferQueue(context);
                    break;
                }

                handleBufferQueue(context, bufferConsumeQueue);

            } catch (RocksDBException e) {
                LOGGER.error("getMessage Failed. cid: {}, topic: {}, queueId: {}, offset: {},  {}",
                    group, topic, queueId, offset,  e.getMessage());
            } finally {
                if (bufferConsumeQueue != null) {
                    bufferConsumeQueue.release();
                }
            }
        }

        recordDiskFallBehindSize(group, topic, queueId, context.getMaxOffsetPy(), context.getMaxPhyOffsetPulling());
        setSuggestPullingFromSlave(getResult, context.getMaxOffsetPy(), context.getMaxPhyOffsetPulling());

        return context.toGetResult();
    }

    private void recordDiskFallBehindSize(final String group, final String topic, final int queueId, long maxOffsetPy, long maxPhyOffsetPulling) {
        final boolean diskFallRecorded = messageStore.getMessageStoreConfig().isDiskFallRecorded();
        if (!diskFallRecorded) {
            return;
        }

        long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
        messageStore.getBrokerStatsManager().recordDiskFallBehindSize(group, topic, queueId, fallBehind);
    }

    private void setSuggestPullingFromSlave(GetMessageResult getResult, long maxOffsetPy, long maxPhyOffsetPulling) {
        long diff = maxOffsetPy - maxPhyOffsetPulling;
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (messageStore.getMessageStoreConfig().getAccessMessageInMemoryMaxRatio() / 100.0));
        getResult.setSuggestPullingFromSlave(diff > memory);
    }

    private void setMonitorMatrix(GetMessageStatus status, long beginTime) {
        if (GetMessageStatus.FOUND == status) {
            messageStore.getStoreStatsService().getGetMessageTimesTotalFound().add(1);
        } else {
            messageStore.getStoreStatsService().getGetMessageTimesTotalMiss().add(1);
        }
        long elapsedTime = messageStore.getSystemClock().now() - beginTime;
        messageStore.getStoreStatsService().setGetMessageEntireTimeMax(elapsedTime);
    }

    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        if (messageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE ||
            messageStore.getMessageStoreConfig().isOffsetCheckInSlave()) {
            nextOffset = newOffset;
        }
        return nextOffset;
    }

    private boolean estimateInMemByCommitOffset(long offsetPy, long maxOffsetPy) {
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (messageStore.getMessageStoreConfig().getAccessMessageInMemoryMaxRatio() / 100.0));
        return (maxOffsetPy - offsetPy) <= memory;
    }

    private boolean isTheBatchFull(int sizePy, int unitBatchNum, int maxMsgNums, long maxMsgSize, int bufferTotal,
        int messageTotal, boolean isInMem) {

        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        if (messageTotal + unitBatchNum > maxMsgNums) {
            return true;
        }

        if (bufferTotal + sizePy > maxMsgSize) {
            return true;
        }

        if (isInMem) {
            if ((bufferTotal + sizePy) > messageStore.getMessageStoreConfig().getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            return messageTotal > messageStore.getMessageStoreConfig().getMaxTransferCountOnMessageInMemory() - 1;
        } else {
            if ((bufferTotal + sizePy) > messageStore.getMessageStoreConfig().getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }

            return messageTotal > messageStore.getMessageStoreConfig().getMaxTransferCountOnMessageInDisk() - 1;
        }
    }

}
