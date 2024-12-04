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
package org.apache.rocketmq.store.domain.commitlog.service;

import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.domain.commitlog.CommitLog;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.domain.commitlog.GroupCommitRequest;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;

import java.util.ArrayList;
import java.util.List;


public class GroupCheckService extends FlushCommitLogService {

    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final DefaultMessageStore defaultMessageStore;
    private final CommitLog commitLog;


    private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<>();
    private volatile List<GroupCommitRequest> requestsRead = new ArrayList<>();

    public GroupCheckService(final DefaultMessageStore messageStore, final CommitLog commitLog) {
        this.defaultMessageStore = messageStore;
        this.commitLog = commitLog;
    }

    public boolean isAsyncRequestsFull() {
        return requestsWrite.size() > defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests() * 2;
    }

    public synchronized boolean putRequest(final GroupCommitRequest request) {
        synchronized (this.requestsWrite) {
            this.requestsWrite.add(request);
        }
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
        boolean flag = this.requestsWrite.size() >
            defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests();
        if (flag) {
            log.info("Async requests {} exceeded the threshold {}", requestsWrite.size(),
                defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests());
        }

        return flag;
    }

    private void swapRequests() {
        List<GroupCommitRequest> tmp = this.requestsWrite;
        this.requestsWrite = this.requestsRead;
        this.requestsRead = tmp;
    }

    private void doCommit() {
        synchronized (this.requestsRead) {
            if (this.requestsRead.isEmpty()) {
                return;
            }

            for (GroupCommitRequest req : this.requestsRead) {
                // There may be a message in the next file, so a maximum of
                // two times the flush
                boolean flushOK = false;
                for (int i = 0; i < 1000; i++) {
                    flushOK = commitLog.getMappedFileQueue().getFlushedPosition() >= req.getNextOffset();
                    if (flushOK) {
                        break;
                    } else {
                        try {
                            Thread.sleep(1);
                        } catch (Throwable ignored) {

                        }
                    }
                }
                req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
            }

            long storeTimestamp = commitLog.getMappedFileQueue().getStoreTimestamp();
            if (storeTimestamp > 0) {
                defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
            }

            this.requestsRead.clear();
        }
    }

    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(1);
                this.doCommit();
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        // Under normal circumstances shutdown, wait for the arrival of the
        // request, and then flush
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            log.warn("GroupCommitService Exception, ", e);
        }

        synchronized (this) {
            this.swapRequests();
        }

        this.doCommit();

        log.info(this.getServiceName() + " service end");
    }

    @Override
    protected void onWaitEnd() {
        this.swapRequests();
    }

    @Override
    public String getServiceName() {
        if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return defaultMessageStore.getBrokerConfig().getIdentifier() + GroupCheckService.class.getSimpleName();
        }
        return GroupCheckService.class.getSimpleName();
    }

    @Override
    public long getJoinTime() {
        return 1000 * 60 * 5;
    }
}
