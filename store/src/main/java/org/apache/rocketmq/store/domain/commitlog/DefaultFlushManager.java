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

import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.api.dto.AppendMessageResult;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;
import org.apache.rocketmq.store.domain.commitlog.service.CommitRealTimeService;
import org.apache.rocketmq.store.domain.commitlog.service.FlushCommitLogService;
import org.apache.rocketmq.store.domain.commitlog.service.FlushRealTimeService;
import org.apache.rocketmq.store.domain.commitlog.service.GroupCommitService;
import org.apache.rocketmq.store.server.config.FlushDiskType;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DefaultFlushManager implements FlushManager {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final DefaultMessageStore defaultMessageStore;
    private final CommitLog commitLog;

    private final FlushCommitLogService flushCommitLogService;

    //If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
    private final FlushCommitLogService commitRealTimeService;

    public DefaultFlushManager(final DefaultMessageStore messageStore, final CommitLog commitLog) {
        this.commitLog = commitLog;
        this.defaultMessageStore = messageStore;

        if (FlushDiskType.SYNC_FLUSH == defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            this.flushCommitLogService = new GroupCommitService(messageStore, commitLog);
        } else {
            this.flushCommitLogService = new FlushRealTimeService(messageStore, commitLog);
        }

        this.commitRealTimeService = new CommitRealTimeService(messageStore, commitLog);
    }

    @Override public void start() {
        this.flushCommitLogService.start();

        if (defaultMessageStore.isTransientStorePoolEnable()) {
            this.commitRealTimeService.start();
        }
    }

    public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        if (FlushDiskType.SYNC_FLUSH == defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            handleSyncFlush(result, putMessageResult, messageExt);
        } else {
            handleAsyncFlush();
        }
    }

    private void handleSyncFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
        if (!messageExt.isWaitStoreMsgOK()) {
            service.wakeup();
            return;
        }

        GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(), defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
        service.putRequest(request);
        CompletableFuture<PutMessageStatus> flushOkFuture = request.future();
        PutMessageStatus flushStatus = null;

        try {
            flushStatus = flushOkFuture.get(defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            //flushOK=false;
        }

        if (flushStatus != PutMessageStatus.PUT_OK) {
            log.error("do group commit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags() + " client address: " + messageExt.getBornHostString());
            putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
        }
    }

    private void handleAsyncFlush() {
        if (!defaultMessageStore.isTransientStorePoolEnable()) {
            flushCommitLogService.wakeup();
        } else {
            commitRealTimeService.wakeup();
        }
    }

    @Override
    public CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt) {
        if (FlushDiskType.SYNC_FLUSH == defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            return handleSyncAppendFlush(result, messageExt);
        } else {
            return handleAsyncAppendFlush();
        }
    }

    private CompletableFuture<PutMessageStatus> handleSyncAppendFlush(AppendMessageResult result, MessageExt messageExt) {
        final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
        if (messageExt.isWaitStoreMsgOK()) {
            GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(), defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
            commitLog.getFlushDiskWatcher().add(request);
            service.putRequest(request);
            return request.future();
        } else {
            service.wakeup();
            return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        }
    }

    private CompletableFuture<PutMessageStatus> handleAsyncAppendFlush() {
        if (!defaultMessageStore.isTransientStorePoolEnable()) {
            flushCommitLogService.wakeup();
        } else {
            commitRealTimeService.wakeup();
        }
        return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
    }

    @Override
    public void wakeUpFlush() {
        // now wake up flush thread.
        flushCommitLogService.wakeup();
    }

    @Override
    public void wakeUpCommit() {
        // now wake up commit log thread.
        commitRealTimeService.wakeup();
    }

    @Override
    public void shutdown() {
        if (defaultMessageStore.isTransientStorePoolEnable()) {
            this.commitRealTimeService.shutdown();
        }

        this.flushCommitLogService.shutdown();
    }

}