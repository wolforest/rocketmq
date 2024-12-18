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
package org.apache.rocketmq.store.domain.commitlog.thread;

import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.domain.commitlog.CommitLog;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.domain.commitlog.dto.GroupCommitRequest;
import org.apache.rocketmq.store.domain.message.PutMessageSpinLock;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;

import java.util.LinkedList;

/**
 * GroupCommit Service
 */
public class GroupCommitService extends FlushCommitLogService {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final DefaultMessageStore defaultMessageStore;
    private final CommitLog commitLog;

    private volatile LinkedList<GroupCommitRequest> requestsWrite = new LinkedList<>();
    private volatile LinkedList<GroupCommitRequest> requestsRead = new LinkedList<>();
    private final PutMessageSpinLock lock = new PutMessageSpinLock();

    public GroupCommitService(final DefaultMessageStore messageStore, final CommitLog commitLog) {
        this.defaultMessageStore = messageStore;
        this.commitLog = commitLog;
    }

    public void putRequest(final GroupCommitRequest request) {
        lock.lock();
        try {
            this.requestsWrite.add(request);
        } finally {
            lock.unlock();
        }
        this.wakeup();
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(10);

                this.swapRequests();
                this.doCommit();

            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        // Under normal circumstances shutdown, wait for the arrival of the
        // request, and then flush
        ThreadUtils.sleep(10, "GroupCommitService Exception, ");

        this.swapRequests();
        this.doCommit();

        log.info(this.getServiceName() + " service end");
    }

    /**
     *  moved swapRequest to run()
     *  this will be helpful for understanding method run()
     */
    @Override
    protected void onWaitEnd() {
        // this.swapRequests();
    }

    @Override
    public String getServiceName() {
        if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return defaultMessageStore.getBrokerConfig().getIdentifier() + GroupCommitService.class.getSimpleName();
        }
        return GroupCommitService.class.getSimpleName();
    }

    @Override
    public long getJoinTime() {
        return 1000 * 60 * 5;
    }

    private void swapRequests() {
        lock.lock();
        try {
            LinkedList<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        } finally {
            lock.unlock();
        }
    }

    private void doCommit() {
        if (this.requestsRead.isEmpty()) {
            // Because of individual messages is set to not sync flush, it
            // will come to this process
            commitLog.getMappedFileQueue().flush(0);
            return;
        }

        for (GroupCommitRequest req : this.requestsRead) {
            // There may be a message in the next file, so a maximum of
            // two times the flush
            boolean flushOK = commitLog.getMappedFileQueue().getFlushedPosition() >= req.getNextOffset();
            for (int i = 0; i < 2 && !flushOK; i++) {
                commitLog.getMappedFileQueue().flush(0);
                flushOK = commitLog.getMappedFileQueue().getFlushedPosition() >= req.getNextOffset();
            }

            req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
        }

        long storeTimestamp = commitLog.getMappedFileQueue().getStoreTimestamp();
        if (storeTimestamp > 0) {
            defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
        }

        this.requestsRead = new LinkedList<>();

    }

}
