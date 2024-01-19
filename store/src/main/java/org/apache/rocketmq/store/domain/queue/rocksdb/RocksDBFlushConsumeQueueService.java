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
package org.apache.rocketmq.store.domain.queue.rocksdb;

import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.server.daemon.FlushConsumeQueueService;

public class RocksDBFlushConsumeQueueService extends FlushConsumeQueueService {

    public RocksDBFlushConsumeQueueService(DefaultMessageStore messageStore) {
        super(messageStore);
    }
    /**
     * There is no need to flush consume queue,
     * we put all consume queues in RocksDBConsumeQueueStore,
     * it depends on rocksdb to flush consume queue to disk(sorted string table),
     * we even don't flush WAL of consume store, since we think it can recover consume queue from commitlog.
     */
    @Override
    public void run() {

    }
}
