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
package org.apache.rocketmq.store.domain.commitlog;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.domain.consumer.CQType;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageExtBatch;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.domain.message.MessageVersion;
import org.apache.rocketmq.common.domain.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.domain.topic.TopicValidator;
import org.apache.rocketmq.common.utils.BinaryUtils;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.common.utils.SystemUtils;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.common.utils.TimeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.domain.commitlog.dto.DelayLevel;
import org.apache.rocketmq.store.domain.commitlog.service.DefaultAppendMessageCallback;
import org.apache.rocketmq.store.domain.commitlog.service.DefaultFlushManager;
import org.apache.rocketmq.store.domain.commitlog.service.FlushDiskWatcher;
import org.apache.rocketmq.store.domain.commitlog.service.FlushManager;
import org.apache.rocketmq.store.infra.mappedfile.AppendMessageCallback;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.domain.dispatcher.DispatchRequest;
import org.apache.rocketmq.store.domain.message.MessageExtEncoder;
import org.apache.rocketmq.store.domain.message.MessageExtEncoder.PutMessageThreadLocal;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.domain.message.PutMessageLock;
import org.apache.rocketmq.store.domain.message.PutMessageReentrantLock;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.domain.message.PutMessageSpinLock;
import org.apache.rocketmq.store.infra.memory.Swappable;
import org.apache.rocketmq.store.domain.queue.TopicQueueLock;
import org.apache.rocketmq.store.domain.commitlog.service.ColdDataCheckThread;
import org.apache.rocketmq.store.domain.commitlog.service.CommitLogPutService;
import org.apache.rocketmq.store.domain.commitlog.service.CommitLogRecoverService;
import org.apache.rocketmq.store.server.config.BrokerRole;
import org.apache.rocketmq.store.server.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.infra.mappedfile.MappedFile;
import org.apache.rocketmq.store.infra.mappedfile.MappedFileQueue;
import org.apache.rocketmq.store.infra.mappedfile.MultiPathMappedFileQueue;
import org.apache.rocketmq.store.infra.mappedfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.infra.mappedfile.SelectMappedFileResult;
import org.apache.rocketmq.store.infra.memory.LibC;
import org.rocksdb.RocksDBException;
import sun.nio.ch.DirectBuffer;

/**
 * Store all metadata downtime for recovery, data protection reliability
 */
public class CommitLog implements Swappable {
    // Message's MAGIC CODE daa320a7
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    public static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // End of file empty MAGIC CODE cbd43194
    public final static int BLANK_MAGIC_CODE = -875286124;
    /**
     * CRC32 Format: [PROPERTY_CRC32 + NAME_VALUE_SEPARATOR + 10-digit fixed-length string + PROPERTY_SEPARATOR]
     */
    public static final int CRC32_RESERVED_LEN = MessageConst.PROPERTY_CRC32.length() + 1 + 10 + 1;

    protected MappedFileQueue mappedFileQueue;

    protected final DefaultMessageStore defaultMessageStore;

    private final FlushManager flushManager;
    private final ColdDataCheckThread coldDataCheckThread;
    private final CommitLogRecoverService commitLogRecoverService;

    private final AppendMessageCallback appendMessageCallback;
    private ThreadLocal<PutMessageThreadLocal> putMessageThreadLocal;

    /**
     * only set while DefaultMessageStore.recover()
     * while recover used by ReputMessageService
     */
    protected volatile long confirmOffset = -1L;
    private volatile long beginTimeInLock = 0;
    protected int commitLogSize;

    protected final PutMessageLock putMessageLock;
    protected final TopicQueueLock topicQueueLock;

    private volatile Set<String> fullStorePaths = Collections.emptySet();
    private final FlushDiskWatcher flushDiskWatcher;
    private final DelayLevel delayLevel;

    public CommitLog(final DefaultMessageStore messageStore) {
        initMappedFileQueue(messageStore);

        this.defaultMessageStore = messageStore;
        this.flushManager = new DefaultFlushManager(messageStore, this);
        this.coldDataCheckThread = new ColdDataCheckThread(messageStore);
        this.commitLogRecoverService = new CommitLogRecoverService(messageStore, this);
        this.appendMessageCallback = new DefaultAppendMessageCallback(messageStore, this);

        initPutMessageThreadLocal();

        this.putMessageLock = messageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();
        this.flushDiskWatcher = new FlushDiskWatcher();
        this.topicQueueLock = new TopicQueueLock(messageStore.getMessageStoreConfig().getTopicQueueLockNum());
        this.commitLogSize = messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        this.delayLevel = new DelayLevel(messageStore.getMessageStoreConfig());
    }

    public static boolean isMultiDispatchMsg(MessageExtBrokerInner msg) {
        return StringUtils.isNoneBlank(msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH)) && !msg.getTopic().startsWith(MQConstants.RETRY_GROUP_TOPIC_PREFIX);
    }

    public void setFullStorePaths(Set<String> fullStorePaths) {
        this.fullStorePaths = fullStorePaths;
    }

    public Set<String> getFullStorePaths() {
        return fullStorePaths;
    }

    public long getTotalSize() {
        return this.mappedFileQueue.getTotalFileSize();
    }

    public ThreadLocal<PutMessageThreadLocal> getPutMessageThreadLocal() {
        return putMessageThreadLocal;
    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        if (result && !defaultMessageStore.getMessageStoreConfig().isDataReadAheadEnable()) {
            scanFileAndSetReadMode(LibC.MADV_RANDOM);
        }
        this.mappedFileQueue.checkSelf();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void start() {
        this.flushManager.start();
        log.info("start commitLog successfully. storeRoot: {}", this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        flushDiskWatcher.setDaemon(true);
        flushDiskWatcher.start();
        if (this.coldDataCheckThread != null) {
            this.coldDataCheckThread.start();
        }
    }

    public void shutdown() {
        this.flushManager.shutdown();
        log.info("shutdown commitLog successfully. storeRoot: {}", this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        flushDiskWatcher.shutdown(true);
        if (this.coldDataCheckThread != null) {
            this.coldDataCheckThread.shutdown();
        }
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedPosition();
    }

    public long getFlushedWhere() {
        return this.mappedFileQueue.getFlushedPosition();
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    public int deleteExpiredFile(
        final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately
    ) {
        return deleteExpiredFile(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately, 0);
    }

    public int deleteExpiredFile(
        final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately,
        final int deleteFileBatchMax
    ) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately, deleteFileBatchMax);
    }

    /**
     * Read CommitLog data, use data replication
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile == null) {
            return null;
        }

        int pos = (int) (offset % mappedFileSize);
        return mappedFile.selectMappedBuffer(pos);
    }

    public boolean getData(final long offset, final int size, final ByteBuffer byteBuffer) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile == null) {
            return false;
        }

        int pos = (int) (offset % mappedFileSize);
        return mappedFile.getData(pos, size, byteBuffer);
    }

    public List<SelectMappedBufferResult> getBulkData(final long offset, final int size) {
        List<SelectMappedBufferResult> bufferResultList = new ArrayList<>();

        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        int remainSize = size;
        long startOffset = offset;
        long maxOffset = this.getMaxOffset();
        if (offset + size > maxOffset) {
            remainSize = (int) (maxOffset - offset);
            log.warn("get bulk data size out of range, correct to max offset. offset: {}, size: {}, max: {}", offset, remainSize, maxOffset);
        }

        while (remainSize > 0) {
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(startOffset, startOffset == 0);
            if (mappedFile == null) {
                continue;
            }

            int pos = (int) (startOffset % mappedFileSize);
            int readableSize = mappedFile.getWroteOrCommitPosition() - pos;
            int readSize = Math.min(remainSize, readableSize);

            SelectMappedBufferResult bufferResult = mappedFile.selectMappedBuffer(pos, readSize);
            if (bufferResult == null) {
                break;
            }
            bufferResultList.add(bufferResult);
            remainSize -= readSize;
            startOffset += readSize;
        }

        return bufferResultList;
    }

    public SelectMappedFileResult getFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile == null) {
            return null;
        }

        int size = (int) (mappedFile.getWroteOrCommitPosition() - offset % mappedFileSize);
        if (size > 0) {
            return new SelectMappedFileResult(size, mappedFile);
        }
        return null;
    }

    //Create new mappedFile if not exits.
    public boolean getLastMappedFile(final long startOffset) {
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
        if (null == lastMappedFile) {
            log.error("getLastMappedFile error. offset:{}", startOffset);
            return false;
        }

        return true;
    }

    /**
     * When the normal exit, data recovery, all memory data have been flush
     *
     * @throws RocksDBException only in rocksdb mode
     */
    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) throws RocksDBException {
        this.commitLogRecoverService.recoverNormally(maxPhyOffsetOfConsumeQueue);
    }

    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
        final boolean checkDupInfo) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, checkDupInfo, true);
    }

    /**
     * check the message and returns the message size
     *
     * @return 0 Come the end of the file // >0 Normal messages // -1 Message checksum failure
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
        final boolean checkDupInfo, final boolean readBody) {
        try {
            // 1 TOTAL SIZE
            int totalSize = byteBuffer.getInt();

            // 2 MAGIC CODE
            int magicCode = byteBuffer.getInt();
            DispatchRequest codeResult = checkMagicCode(magicCode);
            if (codeResult != null) {
                return codeResult;
            }

            MessageVersion messageVersion = MessageVersion.valueOfMagicCode(magicCode);
            byte[] bytesContent = new byte[totalSize];

            int bodyCRC = byteBuffer.getInt();
            int queueId = byteBuffer.getInt();
            int flag = byteBuffer.getInt();
            long queueOffset = byteBuffer.getLong();
            long physicOffset = byteBuffer.getLong();
            int sysFlag = byteBuffer.getInt();
            long bornTimeStamp = byteBuffer.getLong();

            ByteBuffer byteBuffer1 = getByteBuffer(byteBuffer, sysFlag, bytesContent, MessageSysFlag.BORNHOST_V6_FLAG);
            long storeTimestamp = byteBuffer.getLong();
            ByteBuffer byteBuffer2 = getByteBuffer(byteBuffer, sysFlag, bytesContent, MessageSysFlag.STOREHOSTADDRESS_V6_FLAG);

            int reconsumeTimes = byteBuffer.getInt();
            long preparedTransactionOffset = byteBuffer.getLong();

            int bodyLen = byteBuffer.getInt();
            DispatchRequest bodyResult = checkBody(byteBuffer, bodyLen, bodyCRC, bytesContent, readBody, checkCRC);
            if (bodyResult != null) {
                return bodyResult;
            }

            int topicLen = messageVersion.getTopicLength(byteBuffer);
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);

            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;

            short propertiesLength = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            if (propertiesLength > 0) {
                byteBuffer.get(bytesContent, 0, propertiesLength);
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
                propertiesMap = MessageDecoder.string2messageProperties(properties);

                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);
                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);

                DispatchRequest duplicateResult = checkDuplicate(checkDupInfo, propertiesMap);
                if (duplicateResult != null) {
                    return duplicateResult;
                }

                tagsCode = getTagsCode(tagsCode, propertiesMap, topic, storeTimestamp, sysFlag);
            }

            if (checkCRC) {
                /*
                 * When the forceVerifyPropCRC = true,
                 * Crc verification needs to be performed on the entire message data (excluding the length reserved at the tail)
                 */
                if (this.defaultMessageStore.getMessageStoreConfig().isForceVerifyPropCRC()) {
                    int expectedCRC = -1;
                    if (propertiesMap != null) {
                        String crc32Str = propertiesMap.get(MessageConst.PROPERTY_CRC32);
                        if (crc32Str != null) {
                            expectedCRC = 0;
                            for (int i = crc32Str.length() - 1; i >= 0; i--) {
                                int num = crc32Str.charAt(i) - '0';
                                expectedCRC *= 10;
                                expectedCRC += num;
                            }
                        }
                    }
                    if (expectedCRC > 0) {
                        ByteBuffer tmpBuffer = byteBuffer.duplicate();
                        tmpBuffer.position(tmpBuffer.position() - totalSize);
                        tmpBuffer.limit(tmpBuffer.position() + totalSize - CommitLog.CRC32_RESERVED_LEN);
                        int crc = BinaryUtils.crc32(tmpBuffer);
                        if (crc != expectedCRC) {
                            log.warn(
                                "CommitLog#checkAndDispatchMessage: failed to check message CRC, expected "
                                    + "CRC={}, actual CRC={}", bodyCRC, crc);
                            return new DispatchRequest(-1, false/* success */);
                        }
                    } else {
                        log.warn(
                            "CommitLog#checkAndDispatchMessage: failed to check message CRC, not found CRC in properties");
                        return new DispatchRequest(-1, false/* success */);
                    }
                }
            }

            int readLength = MessageExtEncoder.calMsgLength(messageVersion, sysFlag, bodyLen, topicLen, propertiesLength);
            if (totalSize != readLength) {
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error(
                    "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}",
                    totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false/* success */);
            }

            DispatchRequest dispatchRequest = new DispatchRequest(
                topic,
                queueId,
                physicOffset,
                totalSize,
                tagsCode,
                storeTimestamp,
                queueOffset,
                keys,
                uniqKey,
                sysFlag,
                preparedTransactionOffset,
                propertiesMap
            );

            setBatchSizeIfNeeded(propertiesMap, dispatchRequest);

            return dispatchRequest;
        } catch (Exception ignored) {
        }

        return new DispatchRequest(-1, false /* success */);
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj == null) {
            return;
        }

        log.debug(String.valueOf(obj.hashCode()));
    }

    private DispatchRequest checkMagicCode(int magicCode) {
        switch (magicCode) {
            case MessageDecoder.MESSAGE_MAGIC_CODE:
            case MessageDecoder.MESSAGE_MAGIC_CODE_V2:
                break;
            case BLANK_MAGIC_CODE:
                return new DispatchRequest(0, true /* success */);
            default:
                log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                return new DispatchRequest(-1, false /* success */);
        }

        return null;
    }

    private ByteBuffer getByteBuffer(java.nio.ByteBuffer byteBuffer, int sysFlag, byte[] bytesContent, int flag) {
        ByteBuffer byteBuffer1;
        if ((sysFlag & flag) == 0) {
            byteBuffer1 = byteBuffer.get(bytesContent, 0, 4 + 4);
        } else {
            byteBuffer1 = byteBuffer.get(bytesContent, 0, 16 + 4);
        }

        return byteBuffer1;
    }

    private DispatchRequest checkBody(java.nio.ByteBuffer byteBuffer, int bodyLen, int bodyCRC, byte[] bytesContent, boolean readBody, boolean checkCRC) {
        if (bodyLen <= 0) {
            return null;
        }

        if (!readBody) {
            byteBuffer.position(byteBuffer.position() + bodyLen);
            return null;
        }

        byteBuffer.get(bytesContent, 0, bodyLen);
        if (!checkCRC) {
            return null;
        }

        /*
         * When the forceVerifyPropCRC = false,
         * use original bodyCrc validation.
         */
        if (this.defaultMessageStore.getMessageStoreConfig().isForceVerifyPropCRC()) {
            return null;
        }

        int crc = BinaryUtils.crc32(bytesContent, 0, bodyLen);
        if (crc == bodyCRC) {
            return null;
        }

        log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
        return new DispatchRequest(-1, false/* success */);
    }

    private DispatchRequest checkDuplicate(boolean checkDupInfo, Map<String, String> propertiesMap) {
        if (!checkDupInfo) {
            return null;
        }

        String dupInfo = propertiesMap.get(MessageConst.DUP_INFO);
        if (null == dupInfo || dupInfo.split("_").length != 2) {
            log.warn("DupInfo in properties check failed. dupInfo={}", dupInfo);
            return new DispatchRequest(-1, false);
        }

        return null;
    }

    private long getTagsCode(long tagsCode, Map<String, String> propertiesMap, String topic, long storeTimestamp, int sysFlag) {
        String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
        if (tags != null && !tags.isEmpty()) {
            tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
        }

        // Timing message processing
        String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        if (!TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(topic) || t == null) {
            return tagsCode;
        }

        int level = Integer.parseInt(t);
        if (level > delayLevel.getMaxDelayLevel()) {
            level = delayLevel.getMaxDelayLevel();
        }

        if (level > 0) {
            tagsCode = delayLevel.computeDeliverTimestamp(level, storeTimestamp);
        }

        return tagsCode;
    }

    private void setBatchSizeIfNeeded(Map<String, String> propertiesMap, DispatchRequest dispatchRequest) {
        if (null != propertiesMap && propertiesMap.containsKey(MessageConst.PROPERTY_INNER_NUM) && propertiesMap.containsKey(MessageConst.PROPERTY_INNER_BASE)) {
            dispatchRequest.setMsgBaseOffset(Long.parseLong(propertiesMap.get(MessageConst.PROPERTY_INNER_BASE)));
            dispatchRequest.setBatchSize(Short.parseShort(propertiesMap.get(MessageConst.PROPERTY_INNER_NUM)));
        }
    }

    // Fetch and compute the newest confirmOffset.
    // Even if it is just inited.
    public long getConfirmOffset() {
        if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            return getConfirmOffsetInControllerMode(false);
        } else if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return this.confirmOffset;
        } else {
            return getMaxOffset();
        }
    }

    // Fetch the original confirmOffset's value.
    // Without checking and re-computing.
    public long getConfirmOffsetDirectly() {
        if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            return getConfirmOffsetInControllerMode(true);
        } else if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return this.confirmOffset;
        } else {
            return getMaxOffset();
        }
    }

    private long getConfirmOffsetInControllerMode(boolean directly) {
        if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE && !this.defaultMessageStore.getRunningFlags().isFenced()) {
            if (((AutoSwitchHAService) this.defaultMessageStore.getHaService()).getLocalSyncStateSet().size() == 1
                || !this.defaultMessageStore.getMessageStoreConfig().isAllAckInSyncStateSet()) {
                return this.defaultMessageStore.getMaxPhyOffset();
            }
            // First time it will compute the confirmOffset.
            if (this.confirmOffset < 0) {
                setConfirmOffset(((AutoSwitchHAService) this.defaultMessageStore.getHaService()).computeConfirmOffset());
                log.info("Init the confirmOffset to {}.", this.confirmOffset);
            }
        }
        return this.confirmOffset;
    }

    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
        this.defaultMessageStore.getStoreCheckpoint().setConfirmPhyOffset(confirmOffset);
    }

    public long getLastFileFromOffset() {
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile();
        if (lastMappedFile == null) {
            return -1;
        }

        if (lastMappedFile.isAvailable()) {
            return lastMappedFile.getOffsetInFileName();
        }

        return -1;
    }

    /**
     * @throws RocksDBException only in rocksdb mode
     */
    @Deprecated
    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) throws RocksDBException {
        this.commitLogRecoverService.recoverAbnormally(maxPhyOffsetOfConsumeQueue);
    }

    public void truncateDirtyFiles(long phyOffset) {
        if (phyOffset <= this.getFlushedWhere()) {
            this.mappedFileQueue.setFlushedPosition(phyOffset);
        }

        if (phyOffset <= this.mappedFileQueue.getCommittedPosition()) {
            this.mappedFileQueue.setCommittedPosition(phyOffset);
        }

        this.mappedFileQueue.truncateDirtyFiles(phyOffset);
        if (this.confirmOffset > phyOffset) {
            this.setConfirmOffset(phyOffset);
        }
    }

    public boolean isMappedFileMatchedRecover(final MappedFile mappedFile) throws RocksDBException {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSITION);
        if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE && magicCode != MessageDecoder.MESSAGE_MAGIC_CODE_V2) {
            return false;
        }

        if (this.defaultMessageStore.getMessageStoreConfig().isEnableRocksDBStore()) {
            final long maxPhyOffsetInConsumeQueue = this.defaultMessageStore.getConsumeQueueStore().getMaxPhyOffsetInConsumeQueue();
            long phyOffset = byteBuffer.getLong(MessageDecoder.MESSAGE_PHYSIC_OFFSET_POSITION);
            if (phyOffset <= maxPhyOffsetInConsumeQueue) {
                log.info("find check. beginPhyOffset: {}, maxPhyOffsetInConsumeQueue: {}", phyOffset, maxPhyOffsetInConsumeQueue);
                return true;
            }
        } else {
            int sysFlag = byteBuffer.getInt(MessageDecoder.SYSFLAG_POSITION);
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
            int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornHostLength;
            long storeTimestamp = byteBuffer.getLong(msgStoreTimePos);
            if (0 == storeTimestamp) {
                return false;
            }

            if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()
                && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
                if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                    log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        TimeUtils.timeMillisToHumanString(storeTimestamp));
                    return true;
                }
            } else {
                if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                    log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        TimeUtils.timeMillisToHumanString(storeTimestamp));
                    return true;
                }
            }
        }

        return false;
    }

    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }

    public void setBeginTimeInLock(long beginTimeInLock) {
        this.beginTimeInLock = beginTimeInLock;
    }

    public AppendMessageCallback getAppendMessageCallback() {
        return appendMessageCallback;
    }

    public PutMessageLock getPutMessageLock() {
        return putMessageLock;
    }

    public TopicQueueLock getTopicQueueLock() {
        return topicQueueLock;
    }

    public void setMappedFileQueueOffset(final long phyOffset) {
        this.mappedFileQueue.setFlushedPosition(phyOffset);
        this.mappedFileQueue.setCommittedPosition(phyOffset);
    }

    public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        CommitLogPutService commitLogPutService = new CommitLogPutService(this);
        return commitLogPutService.asyncPutMessage(msg);
    }

    public CompletableFuture<PutMessageResult> asyncPutMessages(final MessageExtBatch messageExtBatch) {
        CommitLogPutService commitLogPutService = new CommitLogPutService(this);
        return commitLogPutService.asyncPutMessages(messageExtBatch);
    }

    private void initMappedFileQueue(final DefaultMessageStore messageStore) {
        String storePath = messageStore.getMessageStoreConfig().getStorePathCommitLog();
        if (storePath.contains(IOUtils.MULTI_PATH_SPLITTER)) {
            this.mappedFileQueue = new MultiPathMappedFileQueue(messageStore.getMessageStoreConfig(),
                messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(),
                messageStore.getAllocateMappedFileService(), this::getFullStorePaths);
        } else {
            this.mappedFileQueue = new MappedFileQueue(storePath,
                messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(),
                messageStore.getAllocateMappedFileService());
        }
    }

    private void initPutMessageThreadLocal() {
        putMessageThreadLocal = new ThreadLocal<PutMessageThreadLocal>() {
            @Override
            protected PutMessageThreadLocal initialValue() {
                return new PutMessageThreadLocal(defaultMessageStore.getMessageStoreConfig());
            }
        };
    }

    /**
     * According to receive certain message or offset storage time if an error occurs, it returns -1
     */
    public long pickupStoreTimestamp(final long offset, final int size) {
        if (offset < this.getMinOffset() || offset + size > this.getMaxOffset()) {
            return -1;
        }

        SelectMappedBufferResult result = this.getMessage(offset, size);
        if (result == null) {
            return -1;
        }

        try {
            int sysFlag = result.getByteBuffer().getInt(MessageDecoder.SYSFLAG_POSITION);
            int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
            int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
            return result.getByteBuffer().getLong(msgStoreTimePos);
        } finally {
            result.release();
        }
    }

    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile == null) {
            return -1;
        }

        if (mappedFile.isAvailable()) {
            return mappedFile.getOffsetInFileName();
        }

        return this.rollNextFile(mappedFile.getOffsetInFileName());
    }

    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile == null) {
            return null;
        }

        int pos = (int) (offset % mappedFileSize);
        SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(pos, size);
        if (null == selectMappedBufferResult) {
            return null;
        }

        selectMappedBufferResult.setInCache(coldDataCheckThread.isDataInPageCache(offset));
        return selectMappedBufferResult;
    }

    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    public boolean appendData(long startOffset, byte[] data, int dataStart, int dataLength) {
        putMessageLock.lock();
        try {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            return mappedFile.appendMessage(data, dataStart, dataLength);
        } finally {
            putMessageLock.unlock();
        }
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInLock;
        if (begin > 0) {
            diff = TimeUtils.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    public short getMessageNum(MessageExtBrokerInner msgInner) {
        short messageNum = 1;
        // IF inner batch, build batchQueueOffset and batchNum property.
        CQType cqType = getCqType(msgInner);

        if (!MessageSysFlag.check(msgInner.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG) && !CQType.BatchCQ.equals(cqType)) {
            return messageNum;
        }

        if (msgInner.getProperty(MessageConst.PROPERTY_INNER_NUM) == null) {
            return messageNum;
        }

        messageNum = Short.parseShort(msgInner.getProperty(MessageConst.PROPERTY_INNER_NUM));
        messageNum = messageNum >= 1 ? messageNum : 1;

        return messageNum;
    }

    private CQType getCqType(MessageExtBrokerInner msgInner) {
        Optional<TopicConfig> topicConfig = this.defaultMessageStore.getTopicConfig(msgInner.getTopic());
        return QueueTypeUtils.getCQType(topicConfig);
    }


    public int getCommitLogSize() {
        return commitLogSize;
    }

    public MappedFileQueue getMappedFileQueue() {
        return mappedFileQueue;
    }

    public MessageStore getMessageStore() {
        return defaultMessageStore;
    }

    public FlushDiskWatcher getFlushDiskWatcher() {
        return flushDiskWatcher;
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
        this.getMappedFileQueue().swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    public boolean isMappedFilesEmpty() {
        return this.mappedFileQueue.isMappedFilesEmpty();
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {
        this.getMappedFileQueue().cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    public FlushManager getFlushManager() {
        return flushManager;
    }

    public void scanFileAndSetReadMode(int mode) {
        if (SystemUtils.isWindows()) {
            log.info("windows os stop scanFileAndSetReadMode");
            return;
        }
        try {
            log.info("scanFileAndSetReadMode mode: {}", mode);
            mappedFileQueue.getMappedFiles().forEach(mappedFile -> {
                setFileReadMode(mappedFile, mode);
            });
        } catch (Exception e) {
            log.error("scanFileAndSetReadMode exception", e);
        }
    }

    public int setFileReadMode(MappedFile mappedFile, int mode) {
        if (null == mappedFile) {
            log.error("setFileReadMode mappedFile is null");
            return -1;
        }
        final long address = ((DirectBuffer) mappedFile.getMappedByteBuffer()).address();
        int madvise = LibC.INSTANCE.madvise(new Pointer(address), new NativeLong(mappedFile.getFileSize()), mode);
        if (madvise != 0) {
            log.error("setFileReadMode error fileName: {}, madvise: {}, mode:{}", mappedFile.getFileName(), madvise, mode);
        }
        return madvise;
    }

    public ColdDataCheckThread getColdDataCheckService() {
        return coldDataCheckThread;
    }
}
