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
package org.apache.rocketmq.store.commitlog;

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
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageVersion;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.common.utils.TimeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageExtEncoder;
import org.apache.rocketmq.store.MessageExtEncoder.PutMessageThreadLocal;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageLock;
import org.apache.rocketmq.store.PutMessageReentrantLock;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageSpinLock;
import org.apache.rocketmq.store.Swappable;
import org.apache.rocketmq.store.TopicQueueLock;
import org.apache.rocketmq.store.commitlog.service.ColdDataCheckService;
import org.apache.rocketmq.store.commitlog.service.CommitLogPutService;
import org.apache.rocketmq.store.commitlog.service.CommitLogRecoverService;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.logfile.MappedFileQueue;
import org.apache.rocketmq.store.logfile.MultiPathMappedFileQueue;
import org.apache.rocketmq.store.logfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.logfile.SelectMappedFileResult;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * Store all metadata downtime for recovery, data protection reliability
 */
public class CommitLog implements Swappable {
    // Message's MAGIC CODE daa320a7
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // End of file empty MAGIC CODE cbd43194
    public final static int BLANK_MAGIC_CODE = -875286124;
    protected MappedFileQueue mappedFileQueue;

    protected final DefaultMessageStore defaultMessageStore;

    private final FlushManager flushManager;
    private final ColdDataCheckService coldDataCheckService;
    private final CommitLogRecoverService commitLogRecoverService;

    private final AppendMessageCallback appendMessageCallback;
    private ThreadLocal<PutMessageThreadLocal> putMessageThreadLocal;

    protected volatile long confirmOffset = -1L;
    private volatile long beginTimeInLock = 0;
    protected int commitLogSize;

    protected final PutMessageLock putMessageLock;
    protected final TopicQueueLock topicQueueLock;

    private volatile Set<String> fullStorePaths = Collections.emptySet();
    private final FlushDiskWatcher flushDiskWatcher;

    public CommitLog(final DefaultMessageStore messageStore) {
        initMappedFileQueue(messageStore);

        this.defaultMessageStore = messageStore;
        this.flushManager = new DefaultFlushManager(messageStore, this);
        this.coldDataCheckService = new ColdDataCheckService(messageStore);
        this.commitLogRecoverService = new CommitLogRecoverService(messageStore, this);
        this.appendMessageCallback = new DefaultAppendMessageCallback(messageStore, this);

        initPutMessageThreadLocal();

        this.putMessageLock = messageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();
        this.flushDiskWatcher = new FlushDiskWatcher();
        this.topicQueueLock = new TopicQueueLock(messageStore.getMessageStoreConfig().getTopicQueueLockNum());
        this.commitLogSize = messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
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
        if (this.coldDataCheckService != null) {
            this.coldDataCheckService.start();
        }
    }

    public void shutdown() {
        this.flushManager.shutdown();
        log.info("shutdown commitLog successfully. storeRoot: {}", this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        flushDiskWatcher.shutdown(true);
        if (this.coldDataCheckService != null) {
            this.coldDataCheckService.shutdown();
        }
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    public long getFlushedWhere() {
        return this.mappedFileQueue.getFlushedWhere();
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
            int readableSize = mappedFile.getReadPosition() - pos;
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

        int size = (int) (mappedFile.getReadPosition() - offset % mappedFileSize);
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
     */
    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) {
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
        if (bodyLen > 0) {
            if (readBody) {
                byteBuffer.get(bytesContent, 0, bodyLen);

                if (checkCRC) {
                    int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                    if (crc != bodyCRC) {
                        log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                        return new DispatchRequest(-1, false/* success */);
                    }
                }
            } else {
                byteBuffer.position(byteBuffer.position() + bodyLen);
            }
        }
        return null;
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
        if (tags != null && tags.length() > 0) {
            tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
        }

        // Timing message processing
        String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        if (!TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(topic) || t == null) {
            return tagsCode;
        }

        int delayLevel = Integer.parseInt(t);
        if (delayLevel > this.defaultMessageStore.getDelayLevelService().getMaxDelayLevel()) {
            delayLevel = this.defaultMessageStore.getDelayLevelService().getMaxDelayLevel();
        }

        if (delayLevel > 0) {
            tagsCode = this.defaultMessageStore.getDelayLevelService().computeDeliverTimestamp(delayLevel,
                storeTimestamp);
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
            return getConfirmOffsetInControllerMode();
        } else if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return this.confirmOffset;
        } else {
            return getMaxOffset();
        }
    }

    public long getConfirmOffsetInControllerMode() {
        if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE && !this.defaultMessageStore.getRunningFlags().isFenced()) {
            if (((AutoSwitchHAService) this.defaultMessageStore.getHaService()).getLocalSyncStateSet().size() == 1) {
                return this.defaultMessageStore.getMaxPhyOffset();
            }
            // First time it will compute the confirmOffset.
            if (this.confirmOffset <= 0) {
                setConfirmOffset(((AutoSwitchHAService) this.defaultMessageStore.getHaService()).computeConfirmOffset());
                log.info("Init the confirmOffset to {}.", this.confirmOffset);
            }
        }
        return this.confirmOffset;
    }

    // Fetch the original confirmOffset's value.
    // Without checking and re-computing.
    public long getConfirmOffsetDirectly() {
        if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE && !this.defaultMessageStore.getRunningFlags().isFenced()) {
                if (((AutoSwitchHAService) this.defaultMessageStore.getHaService()).getLocalSyncStateSet().size() == 1) {
                    return this.defaultMessageStore.getMaxPhyOffset();
                }
            }
            return this.confirmOffset;
        } else if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return this.confirmOffset;
        } else {
            return getMaxOffset();
        }
    }

    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
        this.defaultMessageStore.getStoreCheckpoint().setConfirmPhyOffset(confirmOffset);
    }

    public long getLastFileFromOffset() {
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile();
        if (lastMappedFile != null) {
            if (lastMappedFile.isAvailable()) {
                return lastMappedFile.getFileFromOffset();
            }
        }

        return -1;
    }

    @Deprecated
    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) {
        this.commitLogRecoverService.recoverAbnormally(maxPhyOffsetOfConsumeQueue);
    }

    public void truncateDirtyFiles(long phyOffset) {
        if (phyOffset <= this.getFlushedWhere()) {
            this.mappedFileQueue.setFlushedWhere(phyOffset);
        }

        if (phyOffset <= this.mappedFileQueue.getCommittedWhere()) {
            this.mappedFileQueue.setCommittedWhere(phyOffset);
        }

        this.mappedFileQueue.truncateDirtyFiles(phyOffset);
        if (this.confirmOffset > phyOffset) {
            this.setConfirmOffset(phyOffset);
        }
    }

    public boolean isMappedFileMatchedRecover(final MappedFile mappedFile) {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSITION);
        if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE && magicCode != MessageDecoder.MESSAGE_MAGIC_CODE_V2) {
            return false;
        }

        int sysFlag = byteBuffer.getInt(MessageDecoder.SYSFLAG_POSITION);
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
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
        this.mappedFileQueue.setFlushedWhere(phyOffset);
        this.mappedFileQueue.setCommittedWhere(phyOffset);
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
        if (storePath.contains(MixAll.MULTI_PATH_SPLITTER)) {
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
                return new PutMessageThreadLocal(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
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
            return mappedFile.getFileFromOffset();
        }

        return this.rollNextFile(mappedFile.getFileFromOffset());
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

        selectMappedBufferResult.setInCache(coldDataCheckService.isDataInPageCache(offset));
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
            diff = this.defaultMessageStore.now() - begin;
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

        if (MessageSysFlag.check(msgInner.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG) || CQType.BatchCQ.equals(cqType)) {
            if (msgInner.getProperty(MessageConst.PROPERTY_INNER_NUM) != null) {
                messageNum = Short.parseShort(msgInner.getProperty(MessageConst.PROPERTY_INNER_NUM));
                messageNum = messageNum >= 1 ? messageNum : 1;
            }
        }

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
        if (MixAll.isWindows()) {
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
        final long address = ((DirectBuffer)mappedFile.getMappedByteBuffer()).address();
        int madvise = LibC.INSTANCE.madvise(new Pointer(address), new NativeLong(mappedFile.getFileSize()), mode);
        if (madvise != 0) {
            log.error("setFileReadMode error fileName: {}, madvise: {}, mode:{}", mappedFile.getFileName(), madvise, mode);
        }
        return madvise;
    }

    public ColdDataCheckService getColdDataCheckService() {
        return coldDataCheckService;
    }
}
