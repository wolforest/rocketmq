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
package org.apache.rocketmq.broker.server.client.rebalance;

import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RebalanceLockManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.REBALANCE_LOCK_LOGGER_NAME);
    private final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty(
        "rocketmq.broker.rebalance.lockMaxLiveTime", "60000"));
    private final Lock lock = new ReentrantLock();
    private final ConcurrentMap<String/* group */, ConcurrentHashMap<MessageQueue, LockEntry>> mqLockTable =
        new ConcurrentHashMap<>(1024);

    public boolean isLockAllExpired(final String group) {
        final ConcurrentHashMap<MessageQueue, LockEntry> lockEntryMap = mqLockTable.get(group);
        if (null == lockEntryMap) {
            return true;
        }
        for (LockEntry entry : lockEntryMap.values()) {
            if (!entry.isExpired()) {
                return false;
            }
        }
        return true;
    }

    public boolean tryLock(final String group, final MessageQueue mq, final String clientId) {
        if (this.isLocked(group, mq, clientId)) {
            return true;
        }

        try {
            this.lock.lockInterruptibly();
            try {
                ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                if (null == groupValue) {
                    groupValue = new ConcurrentHashMap<>(32);
                    this.mqLockTable.put(group, groupValue);
                }

                LockEntry lockEntry = getLockEntry(group, mq, clientId, groupValue);

                if (lockEntry.isLocked(clientId)) {
                    lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                    return true;
                }

                return handleLockExpired(lockEntry, group, clientId, mq, null);
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("RebalanceLockManager#tryLock: unexpected error, group={}, mq={}, clientId={}", group, mq,
                clientId, e);
        }

        return true;
    }

    public Set<MessageQueue> tryLockBatch(final String group, final Set<MessageQueue> mqs, final String clientId) {
        Set<MessageQueue> lockedMqs = new HashSet<>(mqs.size());
        Set<MessageQueue> notLockedMqs = new HashSet<>(mqs.size());
        initLockMQSet(group, mqs, clientId, lockedMqs, notLockedMqs);

        if (notLockedMqs.isEmpty()) {
            return lockedMqs;
        }

        try {
            this.lock.lockInterruptibly();
            try {
                ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                if (null == groupValue) {
                    groupValue = new ConcurrentHashMap<>(32);
                    this.mqLockTable.put(group, groupValue);
                }

                tryLockMQ(group, clientId, notLockedMqs, lockedMqs, groupValue);

            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("RebalanceLockManager#tryBatch: unexpected error, group={}, mqs={}, clientId={}",
                group, mqs, clientId, e);
        }

        return lockedMqs;
    }

    public void unlockBatch(final String group, final Set<MessageQueue> mqs, final String clientId) {
        try {
            this.lock.lockInterruptibly();
            try {
                ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                if (null == groupValue) {
                    log.warn("RebalanceLockManager#unlockBatch: group not exist, group={}, clientId={}, mqs={}", group, clientId, mqs);
                    return;
                }

                unlockBatchMQ(group, mqs, clientId, groupValue);
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("RebalanceLockManager#unlockBatch: unexpected error, group={}, mqs={}, clientId={}", group, mqs,
                clientId);
        }
    }

    private boolean handleLockExpired(LockEntry lockEntry, final String group, final String clientId, MessageQueue mq, Set<MessageQueue> lockedMqs) {
        String oldClientId = lockEntry.getClientId();
        if (!lockEntry.isExpired()) {
            log.warn("RebalanceLockManager: message queue has been locked by other client, "
                + "group={}, mq={}, locked client id={}, current client id={}", group, mq, oldClientId, clientId);
            return false;
        }

        lockEntry.setClientId(clientId);
        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
        log.warn("RebalanceLockManager: try to lock a expired message queue, group={}, "
                + "mq={}, old client id={}, new client id={}", group, mq, oldClientId, clientId);

        if (null != lockedMqs) {
            lockedMqs.add(mq);
        }

        return true;
    }

    private boolean isLocked(final String group, final MessageQueue mq, final String clientId) {
        ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
        if (groupValue == null) {
            return false;
        }

        LockEntry lockEntry = groupValue.get(mq);
        if (lockEntry == null) {
            return false;
        }

        boolean locked = lockEntry.isLocked(clientId);
        if (locked) {
            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
        }

        return locked;
    }

    private void unlockBatchMQ(final String group, final Set<MessageQueue> mqs, final String clientId, ConcurrentHashMap<MessageQueue, LockEntry> groupValue) {
        for (MessageQueue mq : mqs) {
            LockEntry lockEntry = groupValue.get(mq);
            if (null == lockEntry) {
                log.warn("RebalanceLockManager#unlockBatch: mq not locked, group={}, clientId={}, mq={}", group, clientId, mq);
                continue;
            }

            if (lockEntry.getClientId().equals(clientId)) {
                groupValue.remove(mq);
                log.info("RebalanceLockManager#unlockBatch: unlock mq, group={}, clientId={}, mqs={}", group, clientId, mq);
                continue;
            }

            log.warn("RebalanceLockManager#unlockBatch: mq locked by other client, group={}, locked "
                + "clientId={}, current clientId={}, mqs={}", group, lockEntry.getClientId(), clientId, mq);
        }
    }

    private void initLockMQSet(final String group, final Set<MessageQueue> mqs, final String clientId,
        Set<MessageQueue> lockedMqs, Set<MessageQueue> notLockedMqs) {
        for (MessageQueue mq : mqs) {
            if (this.isLocked(group, mq, clientId)) {
                lockedMqs.add(mq);
                continue;
            }

            notLockedMqs.add(mq);
        }
    }

    private LockEntry getLockEntry(final String group, final MessageQueue mq, final String clientId, ConcurrentHashMap<MessageQueue, LockEntry> groupValue) {
        LockEntry lockEntry = groupValue.get(mq);
        if (null != lockEntry) {
            return lockEntry;
        }

        lockEntry = new LockEntry();
        lockEntry.setClientId(clientId);
        groupValue.put(mq, lockEntry);
        log.info("RebalanceLockManager: lock a message queue which has not been locked yet, "
            + "group={}, clientId={}, mq={}", group, clientId, mq);

        return lockEntry;
    }

    private void tryLockMQ(final String group, final String clientId, Set<MessageQueue> notLockedMqs,
        Set<MessageQueue> lockedMqs, ConcurrentHashMap<MessageQueue, LockEntry> groupValue) {

        for (MessageQueue mq : notLockedMqs) {
            LockEntry lockEntry = getLockEntry(group, mq, clientId, groupValue);

            if (lockEntry.isLocked(clientId)) {
                lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                lockedMqs.add(mq);
                continue;
            }

            handleLockExpired(lockEntry, group, clientId, mq, lockedMqs);
        }
    }

    static class LockEntry {
        private String clientId;
        private volatile long lastUpdateTimestamp = System.currentTimeMillis();

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }

        public boolean isLocked(final String clientId) {
            boolean eq = this.clientId.equals(clientId);
            return eq && !this.isExpired();
        }

        public boolean isExpired() {
            return (System.currentTimeMillis() - this.lastUpdateTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
        }
    }
}
