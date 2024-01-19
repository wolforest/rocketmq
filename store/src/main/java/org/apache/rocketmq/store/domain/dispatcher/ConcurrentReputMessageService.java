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
package org.apache.rocketmq.store.domain.dispatcher;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.infra.mappedfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.server.daemon.BatchDispatchRequest;
import org.apache.rocketmq.store.server.daemon.MainBatchDispatchRequestService;
import org.apache.rocketmq.store.server.daemon.ReputMessageService;

public class ConcurrentReputMessageService extends ReputMessageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final int BATCH_SIZE = 1024 * 1024 * 4;

    private long batchId = 0;
    private final MainBatchDispatchRequestService mainBatchDispatchRequestService;
    private final DispatchService dispatchService;

    public ConcurrentReputMessageService(DefaultMessageStore messageStore) {
        super(messageStore);
        this.mainBatchDispatchRequestService = new MainBatchDispatchRequestService(messageStore);
        this.dispatchService = new DispatchService(messageStore);
    }

    public void createBatchDispatchRequest(ByteBuffer byteBuffer, int position, int size) {
        if (position < 0) {
            return;
        }
        messageStore.getMappedPageHoldCount().getAndIncrement();
        BatchDispatchRequest task = new BatchDispatchRequest(byteBuffer.duplicate(), position, size, batchId++);
        messageStore.getBatchDispatchRequestQueue().offer(task);
    }

    @Override
    public void start() {
        super.start();
        this.mainBatchDispatchRequestService.start();
        this.dispatchService.start();
    }

    @Override
    public void doReput() {
        loadReputOffset();

        for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {
            SelectMappedBufferResult result = messageStore.getCommitLog().getData(reputFromOffset);
            if (result == null) {
                break;
            }

            doNext = doReput(result, doNext);
        }

        // only for rocksdb mode
        messageStore.finishCommitLogDispatch();
    }

    private boolean doReput(SelectMappedBufferResult result, boolean doNext) {
        int start = -1;
        int size = -1;
        try {
            this.reputFromOffset = result.getStartOffset();
            ByteBuffer byteBuffer = result.getByteBuffer();

            for (int readSize = 0; readSize < result.getSize() && reputFromOffset < messageStore.getConfirmOffset() && doNext; ) {
                int totalSize = preCheckMessageAndReturnSize(byteBuffer);
                if (totalSize <= 0) {
                    doNext = rollNextFile(byteBuffer, totalSize, start, size);
                    start = -1;
                    size = -1;
                    continue;
                }

                if (start == -1) {
                    start = byteBuffer.position();
                    size = 0;
                }
                size += totalSize;
                if (size > BATCH_SIZE) {
                    this.createBatchDispatchRequest(byteBuffer, start, size);
                    start = -1;
                    size = -1;
                }

                byteBuffer.position(byteBuffer.position() + totalSize);
                this.reputFromOffset += totalSize;
                readSize += totalSize;
            }
        } finally {
            releaseReputResult(result, start, size);
        }

        return doNext;
    }

    private boolean rollNextFile(ByteBuffer byteBuffer, int totalSize, int start, int size) {
        if (totalSize == 0) {
            this.reputFromOffset = messageStore.getCommitLog().rollNextFile(this.reputFromOffset);
        }
        this.createBatchDispatchRequest(byteBuffer, start, size);
        return false;
    }

    private void releaseReputResult(SelectMappedBufferResult result, int batchDispatchRequestStart, int batchDispatchRequestSize) {
        this.createBatchDispatchRequest(result.getByteBuffer(), batchDispatchRequestStart, batchDispatchRequestSize);
        boolean over = messageStore.getMappedPageHoldCount().get() == 0;
        while (!over) {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
            } catch (Exception e) {
                e.printStackTrace();
            }
            over = messageStore.getMappedPageHoldCount().get() == 0;
        }
        result.release();
    }

    /**
     * pre-check the message and returns the message size
     *
     * @return 0 Come to the end of file // >0 Normal messages // -1 Message checksum failure
     */
    public int preCheckMessageAndReturnSize(ByteBuffer byteBuffer) {
        byteBuffer.mark();

        int totalSize = byteBuffer.getInt();
        if (reputFromOffset + totalSize > messageStore.getConfirmOffset()) {
            return -1;
        }

        int magicCode = byteBuffer.getInt();
        switch (magicCode) {
            case MessageDecoder.MESSAGE_MAGIC_CODE:
            case MessageDecoder.MESSAGE_MAGIC_CODE_V2:
                break;
            case MessageDecoder.BLANK_MAGIC_CODE:
                return 0;
            default:
                return -1;
        }

        byteBuffer.reset();

        return totalSize;
    }

    @Override
    public void shutdown() {
        for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException ignored) {
            }
        }

        if (this.isCommitLogAvailable()) {
            LOGGER.warn("shutdown concurrentReputMessageService, but CommitLog have not finish to be dispatched, CommitLog max" +
                    " offset={}, reputFromOffset={}", messageStore.getCommitLog().getMaxOffset(), this.reputFromOffset);
        }

        this.mainBatchDispatchRequestService.shutdown();
        this.dispatchService.shutdown();
        super.shutdown();
    }

    @Override
    public String getServiceName() {
        if (messageStore.getBrokerConfig().isInBrokerContainer()) {
            return messageStore.getBrokerIdentity().getIdentifier() + ConcurrentReputMessageService.class.getSimpleName();
        }
        return ConcurrentReputMessageService.class.getSimpleName();
    }
}

