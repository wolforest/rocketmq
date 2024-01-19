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

public class CommitRealTimeService extends FlushCommitLogService {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final DefaultMessageStore defaultMessageStore;
    private final CommitLog commitLog;
    
    private long lastCommitTimestamp = 0;
    
    public CommitRealTimeService(final DefaultMessageStore messageStore, final CommitLog commitLog) {
        this.defaultMessageStore = messageStore;
        this.commitLog = commitLog;
    }

    @Override
    public String getServiceName() {
        if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return defaultMessageStore.getBrokerIdentity().getIdentifier() + CommitRealTimeService.class.getSimpleName();
        }
        return CommitRealTimeService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");
        while (!this.isStopped()) {
            int interval = defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();

            int commitDataLeastPages = defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();

            int commitDataThoroughInterval =
                defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

            long begin = System.currentTimeMillis();
            if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                this.lastCommitTimestamp = begin;
                commitDataLeastPages = 0;
            }

            try {
                boolean result = commitLog.getMappedFileQueue().commit(commitDataLeastPages);
                long end = System.currentTimeMillis();
                if (!result) {
                    this.lastCommitTimestamp = end; // result = false means some data committed.
                    commitLog.getFlushManager().wakeUpFlush();
                }
                commitLog.getMessageStore().getPerfCounter().flowOnce("COMMIT_DATA_TIME_MS", (int) (end - begin));
                if (end - begin > 500) {
                    log.info("Commit data to file costs {} ms", end - begin);
                }
                this.waitForRunning(interval);
            } catch (Throwable e) {
                log.error(this.getServiceName() + " service has exception. ", e);
            }
        }

        boolean result = false;
        for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
            result = commitLog.getMappedFileQueue().commit(0);
            log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
        }
        log.info(this.getServiceName() + " service end");
    }
}
