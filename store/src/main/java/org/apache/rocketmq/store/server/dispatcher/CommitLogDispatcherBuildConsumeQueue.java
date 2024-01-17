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
package org.apache.rocketmq.store.server.dispatcher;

import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.domain.queue.ConsumeQueueStoreInterface;
import org.apache.rocketmq.store.server.DefaultMessageStore;
import org.rocksdb.RocksDBException;

/**
 * Dispatch commitLog with Flag:
 *  1. TRANSACTION_NOT_TYPE
 *      1.1 normal message
 *      1.2 message with DelayLevel
 *      1.3 message with Timer
 *      1.4 transaction prepare message
 *  2. TRANSACTION_COMMIT_TYPE
 */
public class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final ConsumeQueueStoreInterface consumeQueueStore;

    public CommitLogDispatcherBuildConsumeQueue(DefaultMessageStore messageStore) {
        this.consumeQueueStore = messageStore.getConsumeQueueStore();
    }

    @Override
    public void dispatch(DispatchRequest request) throws RocksDBException {
        final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
        switch (tranType) {
            case MessageSysFlag.TRANSACTION_NOT_TYPE:
            case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                consumeQueueStore.putMessagePositionInfoWrapper(request);
                break;
            case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
            case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                break;
        }
    }
}
