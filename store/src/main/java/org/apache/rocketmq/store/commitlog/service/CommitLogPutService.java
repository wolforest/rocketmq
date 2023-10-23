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
package org.apache.rocketmq.store.commitlog.service;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageVersion;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.BinaryUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtEncoder;
import org.apache.rocketmq.store.PutMessageContext;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.commitlog.CommitLog;
import org.apache.rocketmq.store.commitlog.GroupCommitRequest;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.util.LibC;
import org.rocksdb.RocksDBException;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

public class CommitLogPutService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final CommitLog commitLog;
    protected final DefaultMessageStore defaultMessageStore;

    public CommitLogPutService(CommitLog commitLog) {
        this.commitLog = commitLog;
        this.defaultMessageStore = (DefaultMessageStore) commitLog.getMessageStore();
    }

    public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        initPutMessage(msg);
        CommitLogPutContext context = initContext(msg);
        updateMaxMessageSize(context.getPutMessageThreadLocal());

        CompletableFuture<PutMessageResult> haResult = haCheck(context);
        if (haResult != null) {
            return haResult;
        }

        CompletableFuture<PutMessageResult> errorResult = asyncPutMessage(context, msg);
        if (errorResult != null) {
            return errorResult;
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, context.getResult());

        // Statistics
        this.defaultMessageStore.getStoreStatsService().getSinglePutMessageTopicTimesTotal(msg.getTopic()).add(context.getResult().getMsgNum());
        this.defaultMessageStore.getStoreStatsService().getSinglePutMessageTopicSizeTotal(msg.getTopic()).add(context.getResult().getWroteBytes());

        return handleDiskFlushAndHA(putMessageResult, msg, context.getNeedAckNums(), context.isNeedHandleHA());
    }

    public CompletableFuture<PutMessageResult> asyncPutMessages(final MessageExtBatch messageExtBatch) {
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result = null;

        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) messageExtBatch.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) messageExtBatch.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setStoreHostAddressV6Flag();
        }

        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = commitLog.getMappedFileQueue().getLastMappedFile();

        long currOffset;
        if (mappedFile == null) {
            currOffset = 0;
        } else {
            currOffset = mappedFile.getOffsetInFileName() + mappedFile.getWrotePosition();
        }

        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        boolean needHandleHA = needHandleHA(messageExtBatch);

        if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset) < this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas()) {
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
            if (this.defaultMessageStore.getMessageStoreConfig().isAllAckInSyncStateSet()) {
                // -1 means all ack in SyncStateSet
                needAckNums = MixAll.ALL_ACK_IN_SYNC_STATE_SET;
            }
        } else if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableSlaveActingMaster()) {
            int inSyncReplicas = Math.min(this.defaultMessageStore.getAliveReplicaNumInGroup(),
                this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset));
            needAckNums = calcNeedAckNums(inSyncReplicas);
            if (needAckNums > inSyncReplicas) {
                // Tell the producer, don't have enough slaves to handle the send request
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
        }

        messageExtBatch.setVersion(MessageVersion.MESSAGE_VERSION_V1);
        boolean autoMessageVersionOnTopicLen =
            this.defaultMessageStore.getMessageStoreConfig().isAutoMessageVersionOnTopicLen();
        if (autoMessageVersionOnTopicLen && messageExtBatch.getTopic().length() > Byte.MAX_VALUE) {
            messageExtBatch.setVersion(MessageVersion.MESSAGE_VERSION_V2);
        }

        //fine-grained lock instead of the coarse-grained
        MessageExtEncoder.PutMessageThreadLocal pmThreadLocal = commitLog.getPutMessageThreadLocal().get();
        updateMaxMessageSize(pmThreadLocal);
        MessageExtEncoder batchEncoder = pmThreadLocal.getEncoder();

        String topicQueueKey = generateKey(pmThreadLocal.getKeyBuilder(), messageExtBatch);

        PutMessageContext putMessageContext = new PutMessageContext(topicQueueKey);
        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch, putMessageContext));

        commitLog.getTopicQueueLock().lock(topicQueueKey);
        try {
            defaultMessageStore.assignOffset(messageExtBatch);

            commitLog.getPutMessageLock().lock();
            try {
                long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
                commitLog.setBeginTimeInLock(beginLockTimestamp);

                // Here settings are stored timestamp, in order to ensure an orderly
                // global
                messageExtBatch.setStoreTimestamp(beginLockTimestamp);

                if (null == mappedFile || mappedFile.isFull()) {
                    mappedFile = commitLog.getMappedFileQueue().getLastMappedFile(0); // Mark: NewFile may be cause noise
                    if (isCloseReadAhead()) {
                        commitLog.setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                    }
                }
                if (null == mappedFile) {
                    log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                    commitLog.setBeginTimeInLock(0);
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null));
                }

                result = mappedFile.appendMessages(messageExtBatch, commitLog.getAppendMessageCallback(), putMessageContext);
                switch (result.getStatus()) {
                    case PUT_OK:
                        break;
                    case END_OF_FILE:
                        unlockMappedFile = mappedFile;
                        // Create a new file, re-write the message
                        mappedFile = commitLog.getMappedFileQueue().getLastMappedFile(0);
                        if (null == mappedFile) {
                            // XXX: warn and notify me
                            log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                            commitLog.setBeginTimeInLock(0);
                            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, result));
                        }
                        if (isCloseReadAhead()) {
                            commitLog.setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                        }
                        result = mappedFile.appendMessages(messageExtBatch, commitLog.getAppendMessageCallback(), putMessageContext);
                        break;
                    case MESSAGE_SIZE_EXCEEDED:
                    case PROPERTIES_SIZE_EXCEEDED:
                        commitLog.setBeginTimeInLock(0);
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                    case UNKNOWN_ERROR:
                    default:
                        commitLog.setBeginTimeInLock(0);
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                }

                elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
                commitLog.setBeginTimeInLock(0);
            } finally {
                commitLog.getPutMessageLock().unlock();
            }

            // Increase queue offset when messages are successfully written
            if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
                this.defaultMessageStore.increaseOffset(messageExtBatch, (short) putMessageContext.getBatchSize());
            }
        } catch (RocksDBException e) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
        } finally {
            commitLog.getTopicQueueLock().unlock(topicQueueKey);
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, messageExtBatch.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        this.defaultMessageStore.getStoreStatsService().getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).add(result.getMsgNum());
        this.defaultMessageStore.getStoreStatsService().getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).add(result.getWroteBytes());

        return handleDiskFlushAndHA(putMessageResult, messageExtBatch, needAckNums, needHandleHA);
    }

    private CommitLogPutContext initContext(final MessageExtBrokerInner msg) {
        CommitLogPutContext context = new CommitLogPutContext();
        context.setMsg(msg);
        context.setPutMessageThreadLocal(commitLog.getPutMessageThreadLocal().get());
        context.setTopicQueueKey(generateKey(context.getPutMessageThreadLocal().getKeyBuilder(), msg));
        context.setMappedFile(commitLog.getMappedFileQueue().getLastMappedFile());
        context.setCurrOffset(getCurrOffset(context.getMappedFile()));
        context.setNeedAckNums(this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas());
        context.setNeedHandleHA(needHandleHA(msg));

        return context;
    }

    private CompletableFuture<PutMessageResult> haCheck(CommitLogPutContext context) {
        if (context.isNeedHandleHA() && this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getHaService().inSyncReplicasNums(context.getCurrOffset()) < this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas()) {
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
            if (this.defaultMessageStore.getMessageStoreConfig().isAllAckInSyncStateSet()) {
                // -1 means all ack in SyncStateSet
                context.setNeedAckNums(MixAll.ALL_ACK_IN_SYNC_STATE_SET);
            }
        } else if (context.isNeedHandleHA() && this.defaultMessageStore.getBrokerConfig().isEnableSlaveActingMaster()) {
            int inSyncReplicas = Math.min(this.defaultMessageStore.getAliveReplicaNumInGroup(),
                this.defaultMessageStore.getHaService().inSyncReplicasNums(context.getCurrOffset()));
            context.setNeedAckNums(calcNeedAckNums(inSyncReplicas));
            if (context.getNeedAckNums() > inSyncReplicas) {
                // Tell the producer, don't have enough slaves to handle the send request
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
        }

        return null;
    }

    private boolean isNeedAssignOffset() {
        boolean needAssignOffset = true;
        if (defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()
            && defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE) {
            needAssignOffset = false;
        }

        return needAssignOffset;
    }

    private CompletableFuture<PutMessageResult> parseResultStatus(CommitLogPutContext context, final MessageExtBrokerInner msg, PutMessageContext putMessageContext) {
        switch (context.getResult().getStatus()) {
            case PUT_OK:
                onCommitLogAppend(msg, context.getResult(), context.getMappedFile());
                break;
            case END_OF_FILE:
                onCommitLogAppend(msg, context.getResult(), context.getMappedFile());
                context.setUnlockMappedFile(context.getMappedFile());
                // Create a new file, re-write the message
                context.setMappedFile(commitLog.getMappedFileQueue().getLastMappedFile(0));
                if (null == context.getMappedFile()) {
                    // XXX: warn and notify me
                    log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                    commitLog.setBeginTimeInLock(0);
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, context.getResult()));
                }
                if (isCloseReadAhead()) {
                    commitLog.setFileReadMode(context.getMappedFile(), LibC.MADV_RANDOM);
                }
                context.setResult(context.getMappedFile().appendMessage(msg, commitLog.getAppendMessageCallback(), putMessageContext));
                if (AppendMessageStatus.PUT_OK.equals(context.getResult().getStatus())) {
                    onCommitLogAppend(msg, context.getResult(), context.getMappedFile());
                }
                break;
            case MESSAGE_SIZE_EXCEEDED:
            case PROPERTIES_SIZE_EXCEEDED:
                commitLog.setBeginTimeInLock(0);
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, context.getResult()));
            case UNKNOWN_ERROR:
            default:
                commitLog.setBeginTimeInLock(0);
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, context.getResult()));
        }

        return null;
    }

    private void resetMappedFile(CommitLogPutContext context) {
        if (null != context.getMappedFile() && !context.getMappedFile().isFull()) {
            return;
        }

        // Mark: NewFile may cause noise
        context.setMappedFile(commitLog.getMappedFileQueue().getLastMappedFile(0));
        if (isCloseReadAhead()) {
            commitLog.setFileReadMode(context.getMappedFile(), LibC.MADV_RANDOM);
        }
    }

    private CompletableFuture<PutMessageResult> appendMessage(CommitLogPutContext context, final MessageExtBrokerInner msg) {
        PutMessageContext putMessageContext = new PutMessageContext(context.getTopicQueueKey());
        commitLog.getPutMessageLock().lock(); //spin or ReentrantLock ,depending on store config
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            commitLog.setBeginTimeInLock(beginLockTimestamp);

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            if (!defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                msg.setStoreTimestamp(beginLockTimestamp);
            }

            resetMappedFile(context);

            if (null == context.getMappedFile()) {
                log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                commitLog.setBeginTimeInLock(0);
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null));
            }

            AppendMessageResult result = context.getMappedFile().appendMessage(msg, commitLog.getAppendMessageCallback(), putMessageContext);
            context.setResult(result);

            CompletableFuture<PutMessageResult> statusResult = parseResultStatus(context, msg, putMessageContext);
            if (statusResult != null) {
                return statusResult;
            }

            context.setElapsedTimeInLock(this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp);
            commitLog.setBeginTimeInLock(0);
        } finally {
            commitLog.getPutMessageLock().unlock();
        }

        return null;
    }

    private CompletableFuture<PutMessageResult> asyncPutMessage(CommitLogPutContext context, final MessageExtBrokerInner msg) {
        commitLog.getTopicQueueLock().lock(context.getTopicQueueKey());
        try {
            if (isNeedAssignOffset()) {
                defaultMessageStore.assignOffset(msg);
            }

            PutMessageResult encodeResult = context.getPutMessageThreadLocal().getEncoder().encode(msg);
            if (encodeResult != null) {
                return CompletableFuture.completedFuture(encodeResult);
            }
            msg.setEncodedBuff(context.getPutMessageThreadLocal().getEncoder().getEncoderBuffer());

            CompletableFuture<PutMessageResult> appendResult = appendMessage(context, msg);
            if (appendResult != null) {
                return appendResult;
            }

            // Increase queue offset when messages are successfully written
            if (AppendMessageStatus.PUT_OK.equals(context.getResult().getStatus())) {
                this.defaultMessageStore.increaseOffset(msg, commitLog.getMessageNum(msg));
            }
        } catch (RocksDBException e) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null));
        } finally {
            commitLog.getTopicQueueLock().unlock(context.getTopicQueueKey());
        }

        if (context.getElapsedTimeInLock() > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", context.getElapsedTimeInLock(), msg.getBody().length, context.getResult());
        }

        if (null != context.getUnlockMappedFile() && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(context.getUnlockMappedFile());
        }

        return null;
    }

    private void initPutMessage(final MessageExtBrokerInner msg) {
        // Set the storage time
        if (!defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            msg.setStoreTimestamp(System.currentTimeMillis());
        }

        // Set the message body CRC (consider the most appropriate setting on the client)
        msg.setBodyCRC(BinaryUtil.crc32(msg.getBody()));

        msg.setVersion(MessageVersion.MESSAGE_VERSION_V1);
        boolean autoMessageVersionOnTopicLen = this.defaultMessageStore.getMessageStoreConfig().isAutoMessageVersionOnTopicLen();
        if (autoMessageVersionOnTopicLen && msg.getTopic().length() > Byte.MAX_VALUE) {
            msg.setVersion(MessageVersion.MESSAGE_VERSION_V2);
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setStoreHostAddressV6Flag();
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setBornHostV6Flag();
        }
    }

    private long getCurrOffset(MappedFile mappedFile) {
        long currOffset;
        if (mappedFile == null) {
            currOffset = 0;
        } else {
            currOffset = mappedFile.getOffsetInFileName() + mappedFile.getWrotePosition();
        }

        return currOffset;
    }

    private boolean isCloseReadAhead() {
        return !MixAll.isWindows() && !defaultMessageStore.getMessageStoreConfig().isDataReadAheadEnable();
    }

    private int calcNeedAckNums(int inSyncReplicas) {
        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        if (this.defaultMessageStore.getMessageStoreConfig().isEnableAutoInSyncReplicas()) {
            needAckNums = Math.min(needAckNums, inSyncReplicas);
            needAckNums = Math.max(needAckNums, this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas());
        }
        return needAckNums;
    }

    private boolean needHandleHA(MessageExt messageExt) {

        if (!messageExt.isWaitStoreMsgOK()) {
            /*
              No need to sync messages that special config to extra broker slaves.
              @see MessageConst.PROPERTY_WAIT_STORE_MSG_OK
             */
            return false;
        }

        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return false;
        }

        if (BrokerRole.SYNC_MASTER != this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            // No need to check ha in async or slave broker
            return false;
        }

        return true;
    }

    private CompletableFuture<PutMessageResult> handleDiskFlushAndHA(PutMessageResult putMessageResult,
        MessageExt messageExt, int needAckNums, boolean needHandleHA) {
        CompletableFuture<PutMessageStatus> flushResultFuture = handleDiskFlush(putMessageResult.getAppendMessageResult(), messageExt);
        CompletableFuture<PutMessageStatus> replicaResultFuture;
        if (!needHandleHA) {
            replicaResultFuture = CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        } else {
            replicaResultFuture = handleHA(putMessageResult.getAppendMessageResult(), putMessageResult, needAckNums);
        }

        return handleDiskFlushAndHACallback(flushResultFuture, replicaResultFuture, putMessageResult);
    }

    private CompletableFuture<PutMessageResult> handleDiskFlushAndHACallback(CompletableFuture<PutMessageStatus> flushResultFuture, CompletableFuture<PutMessageStatus> replicaResultFuture, PutMessageResult putMessageResult) {
        return flushResultFuture.thenCombine(replicaResultFuture, (flushStatus, replicaStatus) -> {
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(flushStatus);
            }
            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
            }
            return putMessageResult;
        });
    }


    private CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt) {
        return commitLog.getFlushManager().handleDiskFlush(result, messageExt);
    }

    private CompletableFuture<PutMessageStatus> handleHA(AppendMessageResult result, PutMessageResult putMessageResult,
        int needAckNums) {
        if (needAckNums >= 0 && needAckNums <= 1) {
            return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        }

        HAService haService = this.defaultMessageStore.getHaService();

        long nextOffset = result.getWroteOffset() + result.getWroteBytes();

        // Wait enough acks from different slaves
        GroupCommitRequest request = new GroupCommitRequest(nextOffset, this.defaultMessageStore.getMessageStoreConfig().getSlaveTimeout(), needAckNums);
        haService.putRequest(request);
        haService.getWaitNotifyObject().wakeupAll();
        return request.future();
    }


    private void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile) {
        this.defaultMessageStore.onCommitLogAppend(msg, result, commitLogFile);
    }

    private String generateKey(StringBuilder keyBuilder, MessageExt messageExt) {
        keyBuilder.setLength(0);
        keyBuilder.append(messageExt.getTopic());
        keyBuilder.append('-');
        keyBuilder.append(messageExt.getQueueId());
        return keyBuilder.toString();
    }

    private void updateMaxMessageSize(MessageExtEncoder.PutMessageThreadLocal putMessageThreadLocal) {
        // dynamically adjust maxMessageSize, but not support DLedger mode temporarily
        int newMaxMessageSize = this.defaultMessageStore.getMessageStoreConfig().getMaxMessageSize();
        if (newMaxMessageSize >= 10 &&
            putMessageThreadLocal.getEncoder().getMaxMessageBodySize() != newMaxMessageSize) {
            putMessageThreadLocal.getEncoder().updateEncoderBufferCapacity(newMaxMessageSize);
        }
    }

}
