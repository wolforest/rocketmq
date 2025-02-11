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

package org.apache.rocketmq.store.server.ha.core;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.common.lang.thread.ServiceThread;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.server.ha.HAConnection;
import org.apache.rocketmq.store.server.ha.HAService;
import org.apache.rocketmq.store.server.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.domain.commitlog.dto.GroupCommitRequest;
import org.apache.rocketmq.store.domain.message.PutMessageSpinLock;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;
import org.apache.rocketmq.store.server.ha.autoswitch.AutoSwitchHAConnection;

/**
 * GroupTransferService Service
 * @renamed from GroupTransferService to GroupTransferThread
 */
public class GroupTransferThread extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
    private final PutMessageSpinLock lock = new PutMessageSpinLock();
    private final DefaultMessageStore defaultMessageStore;
    private final HAService haService;
    private volatile List<GroupCommitRequest> requestsWrite = new LinkedList<>();
    private volatile List<GroupCommitRequest> requestsRead = new LinkedList<>();

    public GroupTransferThread(final HAService haService, final DefaultMessageStore defaultMessageStore) {
        this.haService = haService;
        this.defaultMessageStore = defaultMessageStore;
    }

    public void putRequest(final GroupCommitRequest request) {
        lock.lock();
        try {
            this.requestsWrite.add(request);
        } finally {
            lock.unlock();
        }
        wakeup();
    }

    public void notifyTransferSome() {
        this.notifyTransferObject.wakeup();
    }

    private void swapRequests() {
        lock.lock();
        try {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        } finally {
            lock.unlock();
        }
    }

    private void doWaitTransfer() {
        if (this.requestsRead.isEmpty()) {
            return;
        }

        for (GroupCommitRequest req : this.requestsRead) {
            final boolean allAckInSyncStateSet = req.getAckNums() == MQConstants.ALL_ACK_IN_SYNC_STATE_SET;
            boolean transferOK = doWaitTransfer(req, allAckInSyncStateSet);

            if (!transferOK) {
                log.warn("transfer message to slave timeout, offset : {}, request ack: {}", req.getNextOffset(), req.getAckNums());
            }

            PutMessageStatus status = transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT;
            req.wakeupCustomer(status);
        }

        this.requestsRead = new LinkedList<>();
    }

    private boolean doWaitTransfer(GroupCommitRequest req, boolean allAckInSyncStateSet) {
        boolean transferOK = false;

        for (int i = 0; !transferOK && req.getDeadLine() - System.nanoTime() > 0; i++) {
            if (i > 0) {
                this.notifyTransferObject.waitForRunning(1);
            }

            if (!allAckInSyncStateSet && req.getAckNums() <= 1) {
                transferOK = haService.getPush2SlaveMaxOffset().get() >= req.getNextOffset();
                continue;
            }

            if (allAckInSyncStateSet && this.haService instanceof AutoSwitchHAService) {
                // In this mode, we must wait for all replicas that in SyncStateSet.
                final AutoSwitchHAService autoSwitchHAService = (AutoSwitchHAService) this.haService;
                final Set<Long> syncStateSet = autoSwitchHAService.getSyncStateSet();
                if (syncStateSet.size() <= 1) {
                    // Only master
                    transferOK = true;
                    break;
                }

                transferOK = waitInAutoSwitchHAService(req, syncStateSet);
            } else {
                transferOK = waitInHAService(req);
            }
        }

        return transferOK;
    }

    private boolean waitInAutoSwitchHAService(GroupCommitRequest req, Set<Long> syncStateSet) {
        boolean transferOK = false;

        // Include master
        int ackNums = 1;
        for (HAConnection conn : haService.getConnectionList()) {
            final AutoSwitchHAConnection autoSwitchHAConnection = (AutoSwitchHAConnection) conn;
            if (syncStateSet.contains(autoSwitchHAConnection.getSlaveId()) && autoSwitchHAConnection.getSlaveAckOffset() >= req.getNextOffset()) {
                ackNums++;
            }

            if (ackNums >= syncStateSet.size()) {
                transferOK = true;
                break;
            }
        }

        return transferOK;
    }

    private boolean waitInHAService(GroupCommitRequest req) {
        boolean transferOK = false;

        // Include master
        int ackNums = 1;
        for (HAConnection conn : haService.getConnectionList()) {
            // TODO: We must ensure every HAConnection represents a different slave
            // Solution: Consider assign a unique and fixed IP:ADDR for each different slave
            if (conn.getSlaveAckOffset() >= req.getNextOffset()) {
                ackNums++;
            }
            if (ackNums >= req.getAckNums()) {
                transferOK = true;
                break;
            }
        }

        return transferOK;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(10);
                this.doWaitTransfer();
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    protected void onWaitEnd() {
        this.swapRequests();
    }

    @Override
    public String getServiceName() {
        if (defaultMessageStore != null && defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return defaultMessageStore.getBrokerIdentity().getIdentifier() + GroupTransferThread.class.getSimpleName();
        }
        return GroupTransferThread.class.getSimpleName();
    }
}
