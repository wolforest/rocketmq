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
package org.apache.rocketmq.store.queue;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.logfile.MappedFileQueue;
import org.apache.rocketmq.store.logfile.SelectMappedBufferResult;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.rocketmq.store.timer.TimerState.TIMER_TOPIC;

public class ConsumeQueue implements ConsumeQueueInterface, FileQueueLifeCycle {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * ConsumeQueue's store unit. Format:
     * <pre>
     * ┌───────────────────────────────┬───────────────────┬───────────────────────────────┐
     * │    CommitLog Physical Offset  │      Body Size    │            Tag HashCode       │
     * │          (8 Bytes)            │      (4 Bytes)    │             (8 Bytes)         │
     * ├───────────────────────────────┴───────────────────┴───────────────────────────────┤
     * │                                     Store Unit                                    │
     * │                                                                                   │
     * </pre>
     * ConsumeQueue's store unit. Size: CommitLog Physical Offset(8) + Body Size(4) + Tag HashCode(8) = 20 Bytes
     */
    public static final int CQ_STORE_UNIT_SIZE = 20;
    public static final int MSG_TAG_OFFSET_INDEX = 12;
    private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private final MessageStore messageStore;

    private final MappedFileQueue mappedFileQueue;
    private final String topic;
    private final int queueId;

    /**
     * consume queue item write cache
     * size = ConsumeQueue's store unit (CQ_STORE_UNIT_SIZE)
     * @renamed from byteBufferIndex to writeCache
     */
    private final ByteBuffer byteBufferIndex;

    private final int mappedFileSize;
    private long maxPhysicOffset = -1;

    /**
     * Minimum offset of the consume file queue that points to valid commit log record.
     */
    private volatile long minLogicOffset = 0;
    private ConsumeQueueExt consumeQueueExt = null;

    public ConsumeQueue(
            final String topic,
            final int queueId,
            final String storePath,
            final int mappedFileSize,
            final MessageStore messageStore) {
        this.mappedFileSize = mappedFileSize;
        this.messageStore = messageStore;

        this.topic = topic;
        this.queueId = queueId;

        String queueDir = storePath
                + File.separator + topic
                + File.separator + queueId;

        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);

        if (messageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            this.consumeQueueExt = new ConsumeQueueExt(
                    topic,
                    queueId,
                    StorePathConfigHelper.getStorePathConsumeQueueExt(messageStore.getMessageStoreConfig().getStorePathRootDir()),
                    messageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
                    messageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt()
            );
        }
    }

    @Override
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        if (isExtReadEnable()) {
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    @Override
    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (mappedFiles.isEmpty()) {
            return;
        }
        int index = mappedFiles.size() - 3;
        if (index < 0) {
            index = 0;
        }

        int mappedFileSizeLogics = this.mappedFileSize;
        MappedFile mappedFile = mappedFiles.get(index);
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        long processOffset = mappedFile.getFileFromOffset();
        long mappedFileOffset = 0;
        long maxExtAddr = 1;
        while (true) {
            for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                long tagsCode = byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                    this.setMaxPhysicOffset(offset + size);
                    if (isExtAddr(tagsCode)) {
                        maxExtAddr = tagsCode;
                    }
                } else {
                    log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                            + offset + " " + size + " " + tagsCode);
                    break;
                }
            }

            if (mappedFileOffset == mappedFileSizeLogics) {
                index++;
                if (index >= mappedFiles.size()) {

                    log.info("recover last consume queue file over, last mapped file "
                            + mappedFile.getFileName());
                    break;
                } else {
                    mappedFile = mappedFiles.get(index);
                    byteBuffer = mappedFile.sliceByteBuffer();
                    processOffset = mappedFile.getFileFromOffset();
                    mappedFileOffset = 0;
                    log.info("recover next consume queue file, " + mappedFile.getFileName());
                }
            } else {
                log.info("recover current consume queue over " + mappedFile.getFileName() + " "
                        + (processOffset + mappedFileOffset));
                break;
            }
        }

        processOffset += mappedFileOffset;
        this.mappedFileQueue.setFlushedWhere(processOffset);
        this.mappedFileQueue.setCommittedWhere(processOffset);
        this.mappedFileQueue.truncateDirtyFiles(processOffset);

        if (isExtReadEnable()) {
            this.consumeQueueExt.recover();
            log.info("Truncate consume queue extend file by max {}", maxExtAddr);
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    @Override
    public long getTotalSize() {
        long totalSize = this.mappedFileQueue.getTotalFileSize();
        if (isExtReadEnable()) {
            totalSize += this.consumeQueueExt.getTotalSize();
        }
        return totalSize;
    }

    @Override
    public int getUnitSize() {
        return CQ_STORE_UNIT_SIZE;
    }

    @Deprecated
    @Override
    public long getOffsetInQueueByTime(final long timestamp) {
        MappedFile mappedFile = this.mappedFileQueue.getConsumeQueueMappedFileByTime(timestamp,
                messageStore.getCommitLog(), BoundaryType.LOWER);
        return binarySearchInQueueByTime(mappedFile, timestamp, BoundaryType.LOWER);
    }

    @Override
    public long getOffsetInQueueByTime(final long timestamp, final BoundaryType boundaryType) {
        MappedFile mappedFile = this.mappedFileQueue.getConsumeQueueMappedFileByTime(timestamp,
                messageStore.getCommitLog(), boundaryType);
        return binarySearchInQueueByTime(mappedFile, timestamp, boundaryType);
    }

    private long binarySearchInQueueByTime(final MappedFile mappedFile, final long timestamp,
                                           BoundaryType boundaryType) {
        if (null == mappedFile) {
            return 0;
        }

        long offset = 0;
        int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
        int high = 0;
        int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
        long minPhysicOffset = this.messageStore.getMinPhyOffset();
        int range = mappedFile.getFileSize();
        if (mappedFile.getWrotePosition() != 0 && mappedFile.getWrotePosition() != mappedFile.getFileSize()) {
            // mappedFile is the last one and is currently being written.
            range = mappedFile.getWrotePosition();
        }
        SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0, range);
        if (null != sbr) {
            ByteBuffer byteBuffer = sbr.getByteBuffer();
            int ceiling = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
            int floor = low;
            high = ceiling;
            try {
                // Handle the following corner cases first:
                // 1. store time of (high) < timestamp
                // 2. store time of (low) > timestamp
                long storeTime;
                long phyOffset;
                int size;
                // Handle case 1
                byteBuffer.position(ceiling);
                phyOffset = byteBuffer.getLong();
                size = byteBuffer.getInt();
                storeTime = messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                if (storeTime < timestamp) {
                    switch (boundaryType) {
                        case LOWER:
                            return (mappedFile.getFileFromOffset() + ceiling + CQ_STORE_UNIT_SIZE) / CQ_STORE_UNIT_SIZE;
                        case UPPER:
                            return (mappedFile.getFileFromOffset() + ceiling) / CQ_STORE_UNIT_SIZE;
                        default:
                            log.warn("Unknown boundary type");
                            break;
                    }
                }
                // Handle case 2
                byteBuffer.position(floor);
                phyOffset = byteBuffer.getLong();
                size = byteBuffer.getInt();
                storeTime = messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                if (storeTime > timestamp) {
                    switch (boundaryType) {
                        case LOWER:
                            return mappedFile.getFileFromOffset() / CQ_STORE_UNIT_SIZE;
                        case UPPER:
                            return 0;
                        default:
                            log.warn("Unknown boundary type");
                            break;
                    }
                }

                // Perform binary search
                while (high >= low) {
                    midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                    byteBuffer.position(midOffset);
                    phyOffset = byteBuffer.getLong();
                    size = byteBuffer.getInt();
                    if (phyOffset < minPhysicOffset) {
                        low = midOffset + CQ_STORE_UNIT_SIZE;
                        leftOffset = midOffset;
                        continue;
                    }

                    storeTime = this.messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                    if (storeTime < 0) {
                        log.warn("Failed to query store timestamp for commit log offset: {}", phyOffset);
                        return 0;
                    } else if (storeTime == timestamp) {
                        targetOffset = midOffset;
                        break;
                    } else if (storeTime > timestamp) {
                        high = midOffset - CQ_STORE_UNIT_SIZE;
                        rightOffset = midOffset;
                    } else {
                        low = midOffset + CQ_STORE_UNIT_SIZE;
                        leftOffset = midOffset;
                    }
                }

                if (targetOffset != -1) {
                    // We just found ONE matched record. These next to it might also share the same store-timestamp.
                    offset = targetOffset;
                    switch (boundaryType) {
                        case LOWER: {
                            int previousAttempt = targetOffset;
                            while (true) {
                                int attempt = previousAttempt - CQ_STORE_UNIT_SIZE;
                                if (attempt < floor) {
                                    break;
                                }
                                byteBuffer.position(attempt);
                                long physicalOffset = byteBuffer.getLong();
                                int messageSize = byteBuffer.getInt();
                                long messageStoreTimestamp = messageStore.getCommitLog()
                                        .pickupStoreTimestamp(physicalOffset, messageSize);
                                if (messageStoreTimestamp == timestamp) {
                                    previousAttempt = attempt;
                                    continue;
                                }
                                break;
                            }
                            offset = previousAttempt;
                            break;
                        }
                        case UPPER: {
                            int previousAttempt = targetOffset;
                            while (true) {
                                int attempt = previousAttempt + CQ_STORE_UNIT_SIZE;
                                if (attempt > ceiling) {
                                    break;
                                }
                                byteBuffer.position(attempt);
                                long physicalOffset = byteBuffer.getLong();
                                int messageSize = byteBuffer.getInt();
                                long messageStoreTimestamp = messageStore.getCommitLog()
                                        .pickupStoreTimestamp(physicalOffset, messageSize);
                                if (messageStoreTimestamp == timestamp) {
                                    previousAttempt = attempt;
                                    continue;
                                }
                                break;
                            }
                            offset = previousAttempt;
                            break;
                        }
                        default: {
                            log.warn("Unknown boundary type");
                            break;
                        }
                    }
                } else {
                    // Given timestamp does not have any message records. But we have a range enclosing the
                    // timestamp.
                    /*
                     * Consider the follow case: t2 has no consume queue entry and we are searching offset of
                     * t2 for lower and upper boundaries.
                     *  --------------------------
                     *   timestamp   Consume Queue
                     *       t1          1
                     *       t1          2
                     *       t1          3
                     *       t3          4
                     *       t3          5
                     *   --------------------------
                     * Now, we return 3 as upper boundary of t2 and 4 as its lower boundary. It looks
                     * contradictory at first sight, but it does make sense when performing range queries.
                     */
                    switch (boundaryType) {
                        case LOWER: {
                            offset = rightOffset;
                            break;
                        }

                        case UPPER: {
                            offset = leftOffset;
                            break;
                        }
                        default: {
                            log.warn("Unknown boundary type");
                            break;
                        }
                    }
                }
                return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
            } finally {
                sbr.release();
            }
        }
        return 0;
    }

    @Override
    public void truncateDirtyLogicFiles(long phyOffset) {
        truncateDirtyLogicFiles(phyOffset, true);
    }

    public void truncateDirtyLogicFiles(long phyOffset, boolean deleteFile) {
        int logicFileSize = this.mappedFileSize;

        this.setMaxPhysicOffset(phyOffset);
        long maxExtAddr = 1;
        boolean shouldDeleteFile = false;
        while (true) {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (null == mappedFile) {
                break;
            }

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

            mappedFile.setWrotePosition(0);
            mappedFile.setCommittedPosition(0);
            mappedFile.setFlushedPosition(0);

            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                long tagsCode = byteBuffer.getLong();

                if (0 == i) {
                    if (offset >= phyOffset) {
                        shouldDeleteFile = true;
                        break;
                    } else {
                        int pos = i + CQ_STORE_UNIT_SIZE;
                        mappedFile.setWrotePosition(pos);
                        mappedFile.setCommittedPosition(pos);
                        mappedFile.setFlushedPosition(pos);
                        this.setMaxPhysicOffset(offset + size);
                        // This maybe not take effect, when not every consume queue has extend file.
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }
                    }
                } else {

                    if (offset >= 0 && size > 0) {

                        if (offset >= phyOffset) {
                            return;
                        }

                        int pos = i + CQ_STORE_UNIT_SIZE;
                        mappedFile.setWrotePosition(pos);
                        mappedFile.setCommittedPosition(pos);
                        mappedFile.setFlushedPosition(pos);
                        this.setMaxPhysicOffset(offset + size);
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }

                        if (pos == logicFileSize) {
                            return;
                        }
                    } else {
                        return;
                    }
                }
            }

            if (shouldDeleteFile) {
                if (deleteFile) {
                    this.mappedFileQueue.deleteLastMappedFile();
                } else {
                    this.mappedFileQueue.deleteExpiredFile(Collections.singletonList(this.mappedFileQueue.getLastMappedFile()));
                }
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    @Override
    public long getLastOffset() {
        long lastOffset = -1;

        int logicFileSize = this.mappedFileSize;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (null == mappedFile) {
            return lastOffset;
        }

        int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
        if (position < 0)
            position = 0;

        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        byteBuffer.position(position);
        for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
            long offset = byteBuffer.getLong();
            int size = byteBuffer.getInt();
            byteBuffer.getLong();

            if (offset >= 0 && size > 0) {
                lastOffset = offset + size;
            } else {
                break;
            }
        }

        return lastOffset;
    }

    @Override
    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    @Override
    public int deleteExpiredFile(long offset) {
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        this.correctMinOffset(offset);
        return cnt;
    }

    /**
     * Update minLogicOffset such that entries after it would point to valid commit log address.
     *
     * @param minCommitLogOffset Minimum commit log offset
     */
    @Override
    public void correctMinOffset(long minCommitLogOffset) {
        // Check if the consume queue is the state of deprecation.
        if (minLogicOffset >= mappedFileQueue.getMaxOffset()) {
            log.info("ConsumeQueue[Topic={}, queue-id={}] contains no valid entries", topic, queueId);
            return;
        }

        // Check whether the consume queue maps no valid data at all. This check may cost 1 IO operation.
        // The rationale is that consume queue always preserves the last file. In case there are many deprecated topics,
        // This check would save a lot of efforts.
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile();
        if (null == lastMappedFile) {
            return;
        }

        SelectMappedBufferResult lastRecord = null;
        try {
            int maxReadablePosition = lastMappedFile.getReadPosition();
            lastRecord = lastMappedFile.selectMappedBuffer(maxReadablePosition - ConsumeQueue.CQ_STORE_UNIT_SIZE,
                    ConsumeQueue.CQ_STORE_UNIT_SIZE);
            if (null != lastRecord) {
                ByteBuffer buffer = lastRecord.getByteBuffer();
                long commitLogOffset = buffer.getLong();
                if (commitLogOffset < minCommitLogOffset) {
                    // Keep the largest known consume offset, even if this consume-queue contains no valid entries at
                    // all. Let minLogicOffset point to a future slot.
                    this.minLogicOffset = lastMappedFile.getFileFromOffset() + maxReadablePosition;
                    log.info("ConsumeQueue[topic={}, queue-id={}] contains no valid entries. Min-offset is assigned as: {}.",
                            topic, queueId, getMinOffsetInQueue());
                    return;
                }
            }
        } finally {
            if (null != lastRecord) {
                lastRecord.release();
            }
        }

        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            // Search from previous min logical offset. Typically, a consume queue file segment contains 300,000 entries
            // searching from previous position saves significant amount of comparisons and IOs
            boolean intact = true; // Assume previous value is still valid
            long start = this.minLogicOffset - mappedFile.getFileFromOffset();
            if (start < 0) {
                intact = false;
                start = 0;
            }

            if (start > mappedFile.getReadPosition()) {
                log.error("[Bug][InconsistentState] ConsumeQueue file {} should have been deleted",
                        mappedFile.getFileName());
                return;
            }

            SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) start);
            if (result == null) {
                log.warn("[Bug] Failed to scan consume queue entries from file on correcting min offset: {}",
                        mappedFile.getFileName());
                return;
            }

            try {
                // No valid consume entries
                if (result.getSize() == 0) {
                    log.debug("ConsumeQueue[topic={}, queue-id={}] contains no valid entries", topic, queueId);
                    return;
                }

                ByteBuffer buffer = result.getByteBuffer().slice();
                // Verify whether the previous value is still valid or not before conducting binary search
                long commitLogOffset = buffer.getLong();
                if (intact && commitLogOffset >= minCommitLogOffset) {
                    log.info("Abort correction as previous min-offset points to {}, which is greater than {}",
                            commitLogOffset, minCommitLogOffset);
                    return;
                }

                // Binary search between range [previous_min_logic_offset, first_file_from_offset + file_size)
                // Note the consume-queue deletion procedure ensures the last entry points to somewhere valid.
                int low = 0;
                int high = result.getSize() - ConsumeQueue.CQ_STORE_UNIT_SIZE;
                while (true) {
                    if (high - low <= ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        break;
                    }
                    int mid = (low + high) / 2 / ConsumeQueue.CQ_STORE_UNIT_SIZE * ConsumeQueue.CQ_STORE_UNIT_SIZE;
                    buffer.position(mid);
                    commitLogOffset = buffer.getLong();
                    if (commitLogOffset > minCommitLogOffset) {
                        high = mid;
                    } else if (commitLogOffset == minCommitLogOffset) {
                        low = mid;
                        high = mid;
                        break;
                    } else {
                        low = mid;
                    }
                }

                // Examine the last one or two entries
                for (int i = low; i <= high; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                    buffer.position(i);
                    long offsetPy = buffer.getLong();
                    buffer.position(i + 12);
                    long tagsCode = buffer.getLong();

                    if (offsetPy >= minCommitLogOffset) {
                        this.minLogicOffset = mappedFile.getFileFromOffset() + start + i;
                        log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                                this.getMinOffsetInQueue(), this.topic, this.queueId);
                        // This maybe not take effect, when not every consume queue has an extended file.
                        if (isExtAddr(tagsCode)) {
                            minExtAddr = tagsCode;
                        }
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("Exception thrown when correctMinOffset", e);
            } finally {
                result.release();
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    @Override
    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    /**
     * consume queue api from commitLog dispatching, the process is below:
     * ReputMessageService -> messageStore.doDispatch() -> CommitLogDispatcherBuildConsumeQueue.dispatch()
     *
     * @param request the request containing dispatch information.
     */
    @Override
    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        final int maxRetries = 30;
        boolean canWrite = this.messageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            long tagsCode = request.getTagsCode();
            tagsCode = putMessageToExt(tagsCode, request);

            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(), request.getMsgSize(), tagsCode, request.getConsumeQueueOffset());
            if (result) {
                handleSuccessResult(request, maxRetries);
                return;
            }

            handleFailureResult(request, i);
        }

        // XXX: warn and notify me
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.messageStore.getRunningFlags().makeLogicsQueueError();
    }

    private long putMessageToExt(long tagsCode, DispatchRequest request) {
        if (!isExtWriteEnable()) {
            return tagsCode;
        }

        CqExtUnit cqExtUnit = new CqExtUnit();
        cqExtUnit.setFilterBitMap(request.getBitMap());
        cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
        cqExtUnit.setTagsCode(request.getTagsCode());

        long extAddr = this.consumeQueueExt.put(cqExtUnit);
        if (isExtAddr(extAddr)) {
            tagsCode = extAddr;
        } else {
            log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit,
                topic, queueId, request.getCommitLogOffset());
        }

        return tagsCode;
    }

    private void handleSuccessResult(DispatchRequest request, int maxRetries) {
        if (this.messageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE ||
            this.messageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {
            this.messageStore.getStoreCheckpoint().setPhysicMsgTimestamp(request.getStoreTimestamp());
        }
        this.messageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
        if (checkMultiDispatchQueue(request)) {
            multiDispatchLmqQueue(request, maxRetries);
        }
    }

    private void handleFailureResult(DispatchRequest request, int i) {
        // XXX: warn and notify me
        log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
            + " failed, retry " + i + " times");

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            log.warn("", e);
        }
    }

    private boolean checkMultiDispatchQueue(DispatchRequest dispatchRequest) {
        if (!this.messageStore.getMessageStoreConfig().isEnableMultiDispatch()
                || dispatchRequest.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)
                || dispatchRequest.getTopic().equals(TIMER_TOPIC)
                || dispatchRequest.getTopic().equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)) {
            return false;
        }
        Map<String, String> prop = dispatchRequest.getPropertiesMap();
        if (prop == null || prop.isEmpty()) {
            return false;
        }
        String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        if (StringUtils.isBlank(multiDispatchQueue) || StringUtils.isBlank(multiQueueOffset)) {
            return false;
        }
        return true;
    }

    private void multiDispatchLmqQueue(DispatchRequest request, int maxRetries) {
        Map<String, String> prop = request.getPropertiesMap();
        String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        String[] queueOffsets = multiQueueOffset.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        if (queues.length != queueOffsets.length) {
            log.error("[bug] queues.length!=queueOffsets.length ", request.getTopic());
            return;
        }
        for (int i = 0; i < queues.length; i++) {
            String queueName = queues[i];
            if (StringUtils.contains(queueName, File.separator)) {
                continue;
            }
            long queueOffset = Long.parseLong(queueOffsets[i]);
            int queueId = request.getQueueId();
            if (this.messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
                queueId = 0;
            }
            doDispatchLmqQueue(request, maxRetries, queueName, queueOffset, queueId);

        }
        return;
    }

    private void doDispatchLmqQueue(DispatchRequest request, int maxRetries, String queueName, long queueOffset,
                                    int queueId) {
        ConsumeQueueInterface cq = this.messageStore.findConsumeQueue(queueName, queueId);
        boolean canWrite = this.messageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            boolean result = ((ConsumeQueue) cq).putMessagePositionInfo(request.getCommitLogOffset(), request.getMsgSize(),
                    request.getTagsCode(),
                    queueOffset);
            if (result) {
                break;
            } else {
                log.warn("[BUG]put commit log position info to " + queueName + ":" + queueId + " " + request.getCommitLogOffset()
                        + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }
    }

    @Override
    public void assignQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg) {
        String topicQueueKey = getTopic() + "-" + getQueueId();
        long queueOffset = queueOffsetOperator.getQueueOffset(topicQueueKey);
        msg.setQueueOffset(queueOffset);


        // Handling the multi dispatch message. In the context of a light message queue (as defined in RIP-28),
        // light message queues are constructed based on message properties, which requires special handling of queue offset of the light message queue.
        if (!isNeedHandleMultiDispatch(msg)) {
            return;
        }
        String multiDispatchQueue = msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.isBlank(multiDispatchQueue)) {
            return;
        }
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        Long[] queueOffsets = new Long[queues.length];
        for (int i = 0; i < queues.length; i++) {
            String key = queueKey(queues[i], msg);
            if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(key)) {
                queueOffsets[i] = queueOffsetOperator.getLmqOffset(key);
            }
        }
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET,
                StringUtils.join(queueOffsets, MixAll.MULTI_DISPATCH_QUEUE_SPLITTER));
        removeWaitStorePropertyString(msg);
    }

    @Override
    public void increaseQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg,
                                    short messageNum) {
        String topicQueueKey = getTopic() + "-" + getQueueId();
        queueOffsetOperator.increaseQueueOffset(topicQueueKey, messageNum);

        // Handling the multi dispatch message. In the context of a light message queue (as defined in RIP-28),
        // light message queues are constructed based on message properties, which requires special handling of queue offset of the light message queue.
        if (!isNeedHandleMultiDispatch(msg)) {
            return;
        }
        String multiDispatchQueue = msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.isBlank(multiDispatchQueue)) {
            return;
        }
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        for (int i = 0; i < queues.length; i++) {
            String key = queueKey(queues[i], msg);
            if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(key)) {
                queueOffsetOperator.increaseLmqOffset(key, (short) 1);
            }
        }
    }

    public boolean isNeedHandleMultiDispatch(MessageExtBrokerInner msg) {
        return messageStore.getMessageStoreConfig().isEnableMultiDispatch()
                && !msg.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)
                && !msg.getTopic().equals(TIMER_TOPIC)
                && !msg.getTopic().equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC);
    }

    public String queueKey(String queueName, MessageExtBrokerInner msgInner) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(queueName);
        keyBuilder.append('-');
        int queueId = msgInner.getQueueId();
        if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
            queueId = 0;
        }
        keyBuilder.append(queueId);
        return keyBuilder.toString();
    }

    private void removeWaitStorePropertyString(MessageExtBrokerInner msgInner) {
        if (msgInner.getProperties().containsKey(MessageConst.PROPERTY_WAIT_STORE_MSG_OK)) {
            // There is no need to store "WAIT=true", remove it from propertiesString to save 9 bytes for each message.
            // It works for most case. In some cases msgInner.setPropertiesString invoked later and replace it.
            String waitStoreMsgOKValue = msgInner.getProperties().remove(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            // Reput to properties, since msgInner.isWaitStoreMsgOK() will be invoked later
            msgInner.getProperties().put(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, waitStoreMsgOKValue);
        } else {
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        }
    }

    /**
     * load message from commitLog and add messages to consume queue
     *
     * @param offset CommitLog offset
     * @param size   size of the message in the CommitLog with the offset
     * @param tagsCode DispatchRequest.tagsCode, from message.propertiesMap
     * @param cqOffset consumeQueue offset
     * @return put status
     */
    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode, final long cqOffset) {
        if (offset + size <= this.getMaxPhysicOffset()) {
            log.warn("Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}", maxPhysicOffset, offset);
            return true;
        }

        setWriteCache(offset, size, tagsCode);

        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (null == mappedFile) {
            return false;
        }
        warmMappedFile(mappedFile, cqOffset, expectLogicOffset);

        if (isOffsetInvalid(mappedFile, cqOffset, expectLogicOffset)) {
            return true;
        }

        this.setMaxPhysicOffset(offset + size);
        return mappedFile.appendMessage(this.byteBufferIndex.array());
    }

    /**
     * set write cache while enqueue the message
     * @param offset commitLog offset
     * @param size msg size
     * @param tagsCode tags hashCode
     */
    private void setWriteCache(final long offset, final int size, final long tagsCode) {
        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);
    }

    /**
     * warm MappedFile if it's new
     * @param mappedFile mappedFile
     * @param cqOffset consume queue offset
     * @param expectLogicOffset expectOffset
     */
    private void warmMappedFile(MappedFile mappedFile, long cqOffset, long expectLogicOffset) {
        if (!mappedFile.isFirstCreateInQueue() || cqOffset == 0 && mappedFile.getWrotePosition() != 0) {
            return;
        }

        this.minLogicOffset = expectLogicOffset;
        this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
        this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
        this.fillPreBlank(mappedFile, expectLogicOffset);
        log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " " + mappedFile.getWrotePosition());
    }

    private boolean isOffsetInvalid(MappedFile mappedFile, long cqOffset, long expectLogicOffset) {
        if (cqOffset == 0) {
            return false;
        }

        long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();

        if (expectLogicOffset < currentLogicOffset) {
            log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
            return true;
        }

        if (expectLogicOffset != currentLogicOffset) {
            LOG_ERROR.warn("[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset
            );
        }

        return false;
    }

    /**
     * zero filling the MappedFile to offset
     * @renamed from fillPreBlank to zeroFill
     *
     * @param mappedFile mapped file
     * @param untilWhere the max offset
     */
    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }

    /**
     * get consume queue item by index, and return ByteBuffer format item
     * @renamed from getIndexBuffer to getItemBufferByIndex or getItemBuffer
     *
     * @param startIndex consume queue item index
     * @return consume queue item related MappedFile part
     */
    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        long offset = startIndex * CQ_STORE_UNIT_SIZE;
        if (offset >= this.getMinLogicOffset()) {
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if (mappedFile != null) {
                return mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
            }
        }
        return null;
    }

    @Override
    public ReferredIterator<CqUnit> iterateFrom(long startOffset) {
        SelectMappedBufferResult sbr = getIndexBuffer(startOffset);
        if (sbr == null) {
            return null;
        }
        return new ConsumeQueueIterator(sbr, this);
    }

    @Override
    public CqUnit get(long offset) {
        ReferredIterator<CqUnit> it = iterateFrom(offset);
        if (it == null) {
            return null;
        }
        return it.nextAndRelease();
    }

    @Override
    public CqUnit getEarliestUnit() {
        /**
         * here maybe should not return null
         */
        ReferredIterator<CqUnit> it = iterateFrom(minLogicOffset / CQ_STORE_UNIT_SIZE);
        if (it == null) {
            return null;
        }
        return it.nextAndRelease();
    }

    @Override
    public CqUnit getLatestUnit() {
        ReferredIterator<CqUnit> it = iterateFrom((mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE) - 1);
        if (it == null) {
            return null;
        }
        return it.nextAndRelease();
    }

    @Override
    public boolean isFirstFileAvailable() {
        return false;
    }

    @Override
    public boolean isFirstFileExist() {
        return false;
    }


    public CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    public boolean getExt(final long offset, CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    @Override
    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    @Override
    public long rollNextFile(final long nextBeginOffset) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        return nextBeginOffset + totalUnitsInFile - nextBeginOffset % totalUnitsInFile;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public int getQueueId() {
        return queueId;
    }

    @Override
    public CQType getCQType() {
        return CQType.SimpleCQ;
    }

    @Override
    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    @Override
    public void destroy() {
        this.setMaxPhysicOffset(-1);
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    @Override
    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    @Override
    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    @Override
    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null
                && this.messageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
        mappedFileQueue.swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {
        mappedFileQueue.cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    @Override
    public long estimateMessageCount(long from, long to, MessageFilter filter) {
        long physicalOffsetFrom = from * CQ_STORE_UNIT_SIZE;
        long physicalOffsetTo = to * CQ_STORE_UNIT_SIZE;
        List<MappedFile> mappedFiles = mappedFileQueue.range(physicalOffsetFrom, physicalOffsetTo);
        if (mappedFiles.isEmpty()) {
            return -1;
        }

        boolean sample = false;
        long match = 0;
        long raw = 0;

        for (MappedFile mappedFile : mappedFiles) {
            int start = 0;
            int len = mappedFile.getFileSize();

            // calculate start and len for first segment and last segment to reduce scanning
            // first file segment
            if (mappedFile.getFileFromOffset() <= physicalOffsetFrom) {
                start = (int) (physicalOffsetFrom - mappedFile.getFileFromOffset());
                if (mappedFile.getFileFromOffset() + mappedFile.getFileSize() >= physicalOffsetTo) {
                    // current mapped file covers search range completely.
                    len = (int) (physicalOffsetTo - physicalOffsetFrom);
                } else {
                    len = mappedFile.getFileSize() - start;
                }
            }

            // last file segment
            if (0 == start && mappedFile.getFileFromOffset() + mappedFile.getFileSize() > physicalOffsetTo) {
                len = (int) (physicalOffsetTo - mappedFile.getFileFromOffset());
            }

            // select partial data to scan
            SelectMappedBufferResult slice = mappedFile.selectMappedBuffer(start, len);
            if (null != slice) {
                try {
                    ByteBuffer buffer = slice.getByteBuffer();
                    int current = 0;
                    while (current < len) {
                        // skip physicalOffset and message length fields.
                        buffer.position(current + MSG_TAG_OFFSET_INDEX);
                        long tagCode = buffer.getLong();
                        CqExtUnit ext = null;
                        if (isExtWriteEnable()) {
                            ext = consumeQueueExt.get(tagCode);
                            tagCode = ext.getTagsCode();
                        }
                        if (filter.isMatchedByConsumeQueue(tagCode, ext)) {
                            match++;
                        }
                        raw++;
                        current += CQ_STORE_UNIT_SIZE;

                        if (raw >= messageStore.getMessageStoreConfig().getMaxConsumeQueueScan()) {
                            sample = true;
                            break;
                        }

                        if (match > messageStore.getMessageStoreConfig().getSampleCountThreshold()) {
                            sample = true;
                            break;
                        }
                    }
                } finally {
                    slice.release();
                }
            }
            // we have scanned enough entries, now is the time to return an educated guess.
            if (sample) {
                break;
            }
        }

        long result = match;
        if (sample) {
            if (0 == raw) {
                log.error("[BUG]. Raw should NOT be 0");
                return 0;
            }
            result = (long) (match * (to - from) * 1.0 / raw);
        }
        log.debug("Result={}, raw={}, match={}, sample={}", result, raw, match, sample);
        return result;
    }
}
