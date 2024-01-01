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
package org.apache.rocketmq.store.rocksdb;

import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.service.CleanConsumeQueueService;

public class RocksDBCleanConsumeQueueService extends CleanConsumeQueueService {
    protected static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final double diskSpaceWarningLevelRatio =
        Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "0.90"));

    private final double diskSpaceCleanForciblyRatio =
        Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "0.85"));

    public RocksDBCleanConsumeQueueService(DefaultMessageStore messageStore) {
        super(messageStore);
    }

    @Override
    protected void deleteExpiredFiles() {

        long minOffset = this.messageStore.getCommitLog().getMinOffset();
        if (minOffset > this.lastPhysicalMinOffset) {
            this.lastPhysicalMinOffset = minOffset;

            boolean spaceFull = isSpaceToDelete();
            boolean timeUp = this.messageStore.getCleanCommitLogService().isTimeToDelete();
            if (spaceFull || timeUp) {
                this.messageStore.getConsumeQueueStore().cleanExpired(minOffset);
            }

            this.messageStore.getIndexService().deleteExpiredFile(minOffset);
        }
    }

    private boolean isSpaceToDelete() {
        double ratio = this.messageStore.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;

        String storePathLogics = StorePathConfigHelper
            .getStorePathConsumeQueue(this.messageStore.getMessageStoreConfig().getStorePathRootDir());
        double logicsRatio = IOUtils.getDiskPartitionSpaceUsedPercent(storePathLogics);
        if (logicsRatio > diskSpaceWarningLevelRatio) {
            boolean diskOk = this.messageStore.getRunningFlags().getAndMakeLogicDiskFull();
            if (diskOk) {
                LOGGER.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
            }
        } else if (logicsRatio > diskSpaceCleanForciblyRatio) {
        } else {
            boolean diskOk = this.messageStore.getRunningFlags().getAndMakeLogicDiskOK();
            if (!diskOk) {
                LOGGER.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
            }
        }

        if (logicsRatio < 0 || logicsRatio > ratio) {
            LOGGER.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
            return true;
        }

        return false;
    }
}

