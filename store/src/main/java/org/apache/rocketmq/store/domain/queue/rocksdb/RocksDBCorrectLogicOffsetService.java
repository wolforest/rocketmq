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
import org.apache.rocketmq.store.server.daemon.CorrectLogicOffsetService;

public class RocksDBCorrectLogicOffsetService extends CorrectLogicOffsetService {

    public RocksDBCorrectLogicOffsetService(DefaultMessageStore messageStore) {
        super(messageStore);
    }

    /**
     * There is no need to correct min offset of consume queue, we already fix this problem.
     *  @see RocksDBConsumeQueueOffsetTable#getMinCqOffset
     */
    public void run() {

    }
}

