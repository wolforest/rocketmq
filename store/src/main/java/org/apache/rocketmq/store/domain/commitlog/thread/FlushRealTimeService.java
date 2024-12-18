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
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.domain.commitlog.CommitLog;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;

public class FlushRealTimeService extends FlushCommitLogService {

    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final DefaultMessageStore defaultMessageStore;
    private final CommitLog commitLog;

    private long lastFlushTimestamp = 0;
    private long printTimes = 0;

    public FlushRealTimeService(final DefaultMessageStore messageStore, final CommitLog commitLog) {
        this.defaultMessageStore = messageStore;
        this.commitLog = commitLog;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            boolean flushCommitLogTimed = defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();

            int interval = defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
            int flushPhysicQueueLeastPages = defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();

            int flushPhysicQueueThoroughInterval =
                defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

            boolean printFlushProgress = false;

            // Print flush progress
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushPhysicQueueLeastPages = 0;
                printFlushProgress = (printTimes++ % 10) == 0;
            }

            try {
                if (flushCommitLogTimed) {
                    Thread.sleep(interval);
                } else {
                    this.waitForRunning(interval);
                }

                if (printFlushProgress) {
                    this.printFlushProgress();
                }

                long begin = System.currentTimeMillis();
                commitLog.getMappedFileQueue().flush(flushPhysicQueueLeastPages);
                long storeTimestamp = commitLog.getMappedFileQueue().getStoreTimestamp();
                if (storeTimestamp > 0) {
                    defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                }
                long past = System.currentTimeMillis() - begin;
                defaultMessageStore.getPerfCounter().flowOnce("FLUSH_DATA_TIME_MS", (int) past);
                if (past > 500) {
                    log.info("Flush data to disk costs {} ms", past);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
                this.printFlushProgress();
            }
        }

        // Normal shutdown, to ensure that all the flush before exit
        boolean result = false;
        for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
            result = commitLog.getMappedFileQueue().flush(0);
            log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
        }

        this.printFlushProgress();

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return defaultMessageStore.getBrokerConfig().getIdentifier() + FlushRealTimeService.class.getSimpleName();
        }
        return FlushRealTimeService.class.getSimpleName();
    }

    private void printFlushProgress() {
        // log.info("how much disk fall behind memory, "
        // + commitLog.getMappedFileQueue().howMuchFallBehind());
    }

    @Override
    public long getJoinTime() {
        return 1000 * 60 * 5;
    }
}
