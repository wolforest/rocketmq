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
package org.apache.rocketmq.broker.server.daemon.longpolling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.common.lang.thread.ServiceThread;
import org.apache.rocketmq.common.utils.SystemClock;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.utils.TimeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.domain.queue.CqExtUnit;

public class PullRequestHoldThread extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    protected static final String TOPIC_QUEUE_ID_SEPARATOR = "@";
    protected final Broker broker;
    protected ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable = new ConcurrentHashMap<>(1024);

    public PullRequestHoldThread(final Broker broker) {
        this.broker = broker;
    }

    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        ManyPullRequest mpr = getManyPullRequest(topic, queueId);

        pullRequest.getRequestCommand().setSuspended(true);
        mpr.addPullRequest(pullRequest);
    }

    private ManyPullRequest getManyPullRequest(final String topic, final int queueId) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null != mpr) {
            return mpr;
        }

        mpr = new ManyPullRequest();
        ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
        if (prev != null) {
            mpr = prev;
        }

        return mpr;
    }

    private String buildKey(final String topic, final int queueId) {
        return topic + TOPIC_QUEUE_ID_SEPARATOR + queueId;
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                if (this.broker.getBrokerConfig().isLongPollingEnable()) {
                    this.waitForRunning(5 * 1000);
                } else {
                    this.waitForRunning(this.broker.getBrokerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = TimeUtils.now();
                this.checkHoldRequest();
                long costTime = TimeUtils.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.warn("PullRequestHoldService: check hold pull request cost {}ms", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        if (broker != null && broker.getBrokerConfig().isInBrokerContainer()) {
            return this.broker.getBrokerIdentity().getIdentifier() + PullRequestHoldThread.class.getSimpleName();
        }
        return PullRequestHoldThread.class.getSimpleName();
    }

    protected void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUE_ID_SEPARATOR);
            if (2 != kArray.length) {
                continue;
            }

            String topic = kArray[0];
            int queueId = Integer.parseInt(kArray[1]);
            final long offset = this.broker.getMessageStore().getMaxOffsetInQueue(topic, queueId);

            try {
                this.notifyMessageArriving(topic, queueId, offset);
            } catch (Throwable e) {
                log.error("PullRequestHoldService: failed to check hold request failed, topic={}, queueId={}", topic, queueId, e);
            }
        }
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null, 0, null, null);
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode,
        long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            return;
        }

        List<PullRequest> requestList = mpr.cloneListAndClear();
        if (null == requestList) {
            return;
        }

        List<PullRequest> replayList = new ArrayList<>();
        for (PullRequest request : requestList) {
            if (!notifyByOffset(request, topic, queueId, maxOffset, tagsCode, msgStoreTime, filterBitMap, properties)) {
                continue;
            }

            if (!notifyBySuspendTime(request)) {
                continue;
            }

            replayList.add(request);
        }

        if (!replayList.isEmpty()) {
            mpr.addPullRequest(replayList);
        }

    }

    private boolean notifyByOffset(PullRequest request, String topic, int queueId, long maxOffset, Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        long newestOffset = maxOffset;
        if (newestOffset <= request.getPullFromThisOffset()) {
            newestOffset = this.broker.getMessageStore().getMaxOffsetInQueue(topic, queueId);
        }

        if (newestOffset <= request.getPullFromThisOffset()) {
            return true;
        }

        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode, new CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
        // match by bit map, need eval again when properties are not null.
        if (match && properties != null) {
            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
        }

        if (!match) {
            return true;
        }

        try {
            this.broker.getBrokerNettyServer().executePullRequest(request.getClientChannel(), request.getRequestCommand());
        } catch (Throwable e) {
            log.error("PullRequestHoldService#notifyMessageArriving: failed to execute request when message matched, topic={}, queueId={}", topic, queueId, e);
        }

        return false;
    }

    private boolean notifyBySuspendTime(PullRequest request) {
        if (System.currentTimeMillis() < (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
            return true;
        }

        try {
            this.broker.getBrokerNettyServer().executePullRequest(request.getClientChannel(), request.getRequestCommand());
        } catch (Throwable e) {
            log.error("PullRequestHoldService#notifyMessageArriving: failed to execute request when time's up, topic={}, queueId={}", topic, queueId, e);
        }
        return false;
    }

    public void notifyMasterOnline() {
        for (ManyPullRequest mpr : this.pullRequestTable.values()) {
            if (mpr == null || mpr.isEmpty()) {
                continue;
            }

            notifyMasterOnline(mpr);
        }
    }

    private void notifyMasterOnline(ManyPullRequest mpr) {
        for (PullRequest request : mpr.cloneListAndClear()) {
            try {
                log.info("notify master online, wakeup {} {}", request.getClientChannel(), request.getRequestCommand());
                this.broker.getBrokerNettyServer().executePullRequest(request.getClientChannel(), request.getRequestCommand());
            } catch (Throwable e) {
                log.error("execute request when master online failed.", e);
            }
        }
    }

}
