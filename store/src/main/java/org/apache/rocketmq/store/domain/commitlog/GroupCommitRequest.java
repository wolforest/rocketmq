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
package org.apache.rocketmq.store.domain.commitlog;

import org.apache.rocketmq.store.api.dto.PutMessageStatus;

import java.util.concurrent.CompletableFuture;

public class GroupCommitRequest {
    private final long nextOffset;
    /**
     * Indicate the GroupCommitRequest result: true or false
     */
    private final CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();
    /**
     * slave nums, in controller mode: -1
     */
    private volatile int ackNums = 1;

    private final long deadLine;

    public GroupCommitRequest(long nextOffset, long timeoutMillis) {
        this.nextOffset = nextOffset;
        this.deadLine = System.nanoTime() + (timeoutMillis * 1_000_000);
    }

    public GroupCommitRequest(long nextOffset, long timeoutMillis, int ackNums) {
        this(nextOffset, timeoutMillis);
        this.ackNums = ackNums;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public int getAckNums() {
        return ackNums;
    }

    public long getDeadLine() {
        return deadLine;
    }

    public void wakeupCustomer(final PutMessageStatus status) {
        this.flushOKFuture.complete(status);
    }

    public CompletableFuture<PutMessageStatus> future() {
        return flushOKFuture;
    }
}
