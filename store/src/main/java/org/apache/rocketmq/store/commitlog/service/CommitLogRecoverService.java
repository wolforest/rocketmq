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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.commitlog.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.logfile.MappedFileQueue;
import org.apache.rocketmq.store.logfile.MappedFile;

import java.nio.ByteBuffer;
import java.util.List;


public class CommitLogRecoverService {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final DefaultMessageStore defaultMessageStore;
    private final CommitLog commitLog;
    private final MappedFileQueue mappedFileQueue;

    public CommitLogRecoverService(final DefaultMessageStore messageStore, CommitLog commitLog) {
        this.defaultMessageStore = messageStore;
        this.commitLog = commitLog;
        this.mappedFileQueue = commitLog.getMappedFileQueue();
    }

    private void recoverWithoutMappedFile() {
        // CommitLog case files are deleted
        log.warn("The CommitLog files are deleted, and delete the consume queue files");
        this.mappedFileQueue.setFlushedWhere(0);
        this.mappedFileQueue.setCommittedWhere(0);
        this.defaultMessageStore.destroyLogics();
    }

    private int getLastThirdIndex(List<MappedFile> mappedFiles) {
        int index = mappedFiles.size() - 3;
        if (index < 0) {
            index = 0;
        }

        return index;
    }

    /**
     * When the normal exit, data recovery, all memory data have been flush
     */
    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) {
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        boolean checkDupInfo = this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (mappedFiles.isEmpty()) {
            recoverWithoutMappedFile();
            return;
        }

        // Began to recover from the last third file
        int index = getLastThirdIndex(mappedFiles);

        MappedFile mappedFile = mappedFiles.get(index);
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        long processOffset = mappedFile.getFileFromOffset();
        long mappedFileOffset = 0;
        long lastValidMsgPhyOffset = this.commitLog.getConfirmOffset();
        // normal recover doesn't require dispatching
        boolean doDispatch = false;
        while (true) {
            DispatchRequest dispatchRequest = this.commitLog.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover, checkDupInfo);
            int size = dispatchRequest.getMsgSize();
            // Normal data
            if (dispatchRequest.isSuccess() && size > 0) {
                lastValidMsgPhyOffset = processOffset + mappedFileOffset;
                mappedFileOffset += size;
                // below dispatching action is useless, it's better to be deleted
                this.commitLog.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
            }
            // Come the end of the file, switch to the next file Since the
            // return 0 representatives met last hole,
            // this can not be included in truncate offset
            else if (dispatchRequest.isSuccess() && size == 0) {
                // below dispatching action is useless, it's better to be deleted
                this.commitLog.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, true);
                index++;
                if (index >= mappedFiles.size()) {
                    // Current branch can not happen
                    log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                    break;
                } else {
                    mappedFile = mappedFiles.get(index);
                    byteBuffer = mappedFile.sliceByteBuffer();
                    processOffset = mappedFile.getFileFromOffset();
                    mappedFileOffset = 0;
                    log.info("recover next physics file, " + mappedFile.getFileName());
                }
            }
            // Intermediate file read error
            else if (!dispatchRequest.isSuccess()) {
                if (size > 0) {
                    log.warn("found a half message at {}, it will be truncated.", processOffset + mappedFileOffset);
                }
                log.info("recover physics file end, " + mappedFile.getFileName());
                break;
            }
        }

        processOffset += mappedFileOffset;
        storeRecoverOffset(processOffset, lastValidMsgPhyOffset);
        truncateDirtyLogicFiles(processOffset, maxPhyOffsetOfConsumeQueue);
    }

    private void storeRecoverOffset(long processOffset, long lastValidMsgPhyOffset) {
        if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getConfirmOffset() < this.defaultMessageStore.getMinPhyOffset()) {
                log.error("confirmOffset {} is less than minPhyOffset {}, correct confirmOffset to minPhyOffset", this.defaultMessageStore.getConfirmOffset(), this.defaultMessageStore.getMinPhyOffset());
                this.defaultMessageStore.setConfirmOffset(this.defaultMessageStore.getMinPhyOffset());
            } else if (this.defaultMessageStore.getConfirmOffset() > processOffset) {
                log.error("confirmOffset {} is larger than processOffset {}, correct confirmOffset to processOffset", this.defaultMessageStore.getConfirmOffset(), processOffset);
                this.defaultMessageStore.setConfirmOffset(processOffset);
            }
        } else {
            this.commitLog.setConfirmOffset(lastValidMsgPhyOffset);
        }

        this.mappedFileQueue.setFlushedWhere(processOffset);
        this.mappedFileQueue.setCommittedWhere(processOffset);
        this.mappedFileQueue.truncateDirtyFiles(processOffset);
    }

    private void truncateDirtyLogicFiles(long processOffset, long maxPhyOffsetOfConsumeQueue) {
        // Clear ConsumeQueue redundant data
        if (maxPhyOffsetOfConsumeQueue < processOffset) {
            return;
        }

        log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
        this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
    }

    @Deprecated
    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) {
        // recover by the minimum time stamp
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        boolean checkDupInfo = this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        // Commitlog case files are deleted
        if (mappedFiles.isEmpty()) {
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }

        // Looking beginning to recover from which file
        int index = mappedFiles.size() - 1;
        MappedFile mappedFile = null;
        for (; index >= 0; index--) {
            mappedFile = mappedFiles.get(index);
            if (this.commitLog.isMappedFileMatchedRecover(mappedFile)) {
                log.info("recover from this mapped file " + mappedFile.getFileName());
                break;
            }
        }

        if (index < 0) {
            index = 0;
            mappedFile = mappedFiles.get(index);
        }

        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        long processOffset = mappedFile.getFileFromOffset();
        long mappedFileOffset = 0;
        long lastValidMsgPhyOffset = processOffset;
        long lastConfirmValidMsgPhyOffset = processOffset;
        // abnormal recover require dispatching
        boolean doDispatch = true;
        while (true) {
            DispatchRequest dispatchRequest = this.commitLog.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover, checkDupInfo);
            int size = dispatchRequest.getMsgSize();

            if (dispatchRequest.isSuccess()) {
                // Normal data
                if (size > 0) {
                    lastValidMsgPhyOffset = processOffset + mappedFileOffset;
                    mappedFileOffset += size;

                    if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable() || this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
                        if (dispatchRequest.getCommitLogOffset() + size <= this.defaultMessageStore.getCommitLog().getConfirmOffset()) {
                            this.commitLog.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
                            lastConfirmValidMsgPhyOffset = dispatchRequest.getCommitLogOffset() + size;
                        }
                    } else {
                        this.commitLog.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
                    }
                }
                // Come the end of the file, switch to the next file
                // Since the return 0 representatives met last hole, this can
                // not be included in truncate offset
                else if (size == 0) {
                    this.commitLog.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, true);
                    index++;
                    if (index >= mappedFiles.size()) {
                        // The current branch under normal circumstances should
                        // not happen
                        log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
            } else {

                if (size > 0) {
                    log.warn("found a half message at {}, it will be truncated.", processOffset + mappedFileOffset);
                }

                log.info("recover physics file end, " + mappedFile.getFileName() + " pos=" + byteBuffer.position());
                break;
            }
        }

        processOffset += mappedFileOffset;
        if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getConfirmOffset() < this.defaultMessageStore.getMinPhyOffset()) {
                log.error("confirmOffset {} is less than minPhyOffset {}, correct confirmOffset to minPhyOffset", this.defaultMessageStore.getConfirmOffset(), this.defaultMessageStore.getMinPhyOffset());
                this.defaultMessageStore.setConfirmOffset(this.defaultMessageStore.getMinPhyOffset());
            } else if (this.defaultMessageStore.getConfirmOffset() > lastConfirmValidMsgPhyOffset) {
                log.error("confirmOffset {} is larger than lastConfirmValidMsgPhyOffset {}, correct confirmOffset to lastConfirmValidMsgPhyOffset", this.defaultMessageStore.getConfirmOffset(), lastConfirmValidMsgPhyOffset);
                this.defaultMessageStore.setConfirmOffset(lastConfirmValidMsgPhyOffset);
            }
        } else {
            this.commitLog.setConfirmOffset(lastValidMsgPhyOffset);
        }
        this.mappedFileQueue.setFlushedWhere(processOffset);
        this.mappedFileQueue.setCommittedWhere(processOffset);
        this.mappedFileQueue.truncateDirtyFiles(processOffset);

        // Clear ConsumeQueue redundant data
        if (maxPhyOffsetOfConsumeQueue >= processOffset) {
            log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
            this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
        }
    }
}
