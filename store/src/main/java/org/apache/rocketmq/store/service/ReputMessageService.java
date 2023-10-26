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
package org.apache.rocketmq.store.service;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.utils.MQUtils;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.logfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.rocksdb.RocksDBException;

/**
 * synchronize commitLog messages to consume queue, index service, ...
 * CommitLogSynchronizer may be a better name
 *
 * daemon thread, start by DefaultMessageStore.start()
 */
public class ReputMessageService extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    protected volatile long reputFromOffset = 0;

    protected DefaultMessageStore messageStore;

    public ReputMessageService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
                this.doReput();
            } catch (Exception e) {
                LOGGER.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        LOGGER.info(this.getServiceName() + " service end");
    }

    /**
     *
     * invoked by self.run() or DispatchService.run()
     * if enableBuildConsumeQueueConcurrently is false, this method will be useless
     *
     * @param dispatchRequest dispatchRequest
     */
    public void notifyMessageArrive4MultiQueue(DispatchRequest dispatchRequest) {
        Map<String, String> prop = dispatchRequest.getPropertiesMap();
        if (prop == null || dispatchRequest.getTopic().startsWith(MQUtils.RETRY_GROUP_TOPIC_PREFIX)) {
            return;
        }
        String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        if (StringUtils.isBlank(multiDispatchQueue) || StringUtils.isBlank(multiQueueOffset)) {
            return;
        }
        String[] queues = multiDispatchQueue.split(MQUtils.MULTI_DISPATCH_QUEUE_SPLITTER);
        String[] queueOffsets = multiQueueOffset.split(MQUtils.MULTI_DISPATCH_QUEUE_SPLITTER);
        if (queues.length != queueOffsets.length) {
            return;
        }

        notifyMessageArrivingListener(queues, queueOffsets, dispatchRequest);
    }

    @Override
    public void shutdown() {
        for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
            ThreadUtils.sleep(100);
        }

        if (this.isCommitLogAvailable()) {
            LOGGER.warn("shutdown ReputMessageService, but CommitLog have not finish to be dispatched, CommitLog max" +
                    " offset={}, reputFromOffset={}", messageStore.getCommitLog().getMaxOffset(),
                this.reputFromOffset);
        }

        super.shutdown();
    }

    public long behind() {
        return messageStore.getConfirmOffset() - this.reputFromOffset;
    }

    public boolean isCommitLogAvailable() {
        return this.reputFromOffset < messageStore.getConfirmOffset();
    }

    protected void doReput() {
        loadReputOffset();
        for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {
            SelectMappedBufferResult result = messageStore.getCommitLog().getData(reputFromOffset);
            if (result == null) {
                break;
            }

            doNext = doReput(doNext, result);
        }
    }

    /**
     *
     * @param doNext boolean
     * @param result not null
     * @return boolean
     */
    private boolean doReput(boolean doNext, SelectMappedBufferResult result) {
        this.reputFromOffset = result.getStartOffset();

        try {
            for (int readSize = 0; readSize < result.getSize() && reputFromOffset < messageStore.getConfirmOffset() && doNext; ) {
                DispatchRequest dispatchRequest = messageStore.getCommitLog().checkMessageAndReturnSize(result.getByteBuffer(), false, false, false);
                int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();

                if (reputFromOffset + size > messageStore.getConfirmOffset()) {
                    doNext = false;
                    break;
                }

                if (dispatchRequest.isSuccess()) {
                    readSize = handleSuccessDispatchRequest(readSize, size, result, dispatchRequest);
                } else if (size > 0) {
                    LOGGER.error("[BUG]read total count not equals msg total size. reputFromOffset={}", reputFromOffset);
                    this.reputFromOffset += size;
                } else {
                    doNext = false;
                    fixReputOffset(result, readSize);
                }
            }
        } catch (RocksDBException e) {
            LOGGER.info("dispatch message to cq exception. reputFromOffset: {}", this.reputFromOffset, e);
            return doNext;
        } finally {
            result.release();
        }

        messageStore.finishCommitLogDispatch();

        return doNext;
    }

    protected void loadReputOffset() {
        if (this.reputFromOffset >= messageStore.getCommitLog().getMinOffset()) {
            return;
        }

        LOGGER.warn("The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch behind too much and the commitlog has expired.",
            this.reputFromOffset, messageStore.getCommitLog().getMinOffset());
        this.reputFromOffset = messageStore.getCommitLog().getMinOffset();
    }

    private void addDispatchCount(DispatchRequest dispatchRequest) {
        if (messageStore.getMessageStoreConfig().isDuplicationEnable() || messageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE) {
            return;
        }

        messageStore.getStoreStatsService().getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).add(dispatchRequest.getBatchSize());
        messageStore.getStoreStatsService().getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic()).add(dispatchRequest.getMsgSize());
    }

    private void invokeArrivingListener(DispatchRequest dispatchRequest) {
        if (!messageStore.isNotifyMessageArriveInBatch()) {
            messageStore.notifyMessageArriveIfNecessary(dispatchRequest);
        }
    }

    private int handleSuccessDispatchRequest(int readSize, int size, SelectMappedBufferResult result, DispatchRequest dispatchRequest) throws RocksDBException {
        if (size < 0) {
            return readSize;
        }

        if (size == 0) {
            this.reputFromOffset = messageStore.getCommitLog().rollNextFile(this.reputFromOffset);
            readSize = result.getSize();
            return readSize;
        }

        messageStore.doDispatch(dispatchRequest);
        invokeArrivingListener(dispatchRequest);

        this.reputFromOffset += size;
        readSize += size;
        addDispatchCount(dispatchRequest);

        return readSize;
    }

    private void notifyMessageArrivingListener(String[] queues, String[] queueOffsets, DispatchRequest dispatchRequest) {
        for (int i = 0; i < queues.length; i++) {
            String queueName = queues[i];
            long queueOffset = Long.parseLong(queueOffsets[i]);
            int queueId = dispatchRequest.getQueueId();
            if (messageStore.getMessageStoreConfig().isEnableLmq() && MQUtils.isLmq(queueName)) {
                queueId = 0;
            }
            messageStore.getMessageArrivingListener().arriving(
                queueName, queueId, queueOffset + 1, dispatchRequest.getTagsCode(),
                dispatchRequest.getStoreTimestamp(), dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
        }
    }

    private void fixReputOffset(SelectMappedBufferResult result, int readSize) {
        // If user open the dledger pattern or the broker is master node,
        // it will not ignore the exception and fix the reputFromOffset variable
        if (messageStore.getMessageStoreConfig().isEnableDLegerCommitLog() ||
            messageStore.getBrokerConfig().getBrokerId() == MQUtils.MASTER_ID) {
            LOGGER.error("[BUG]dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                this.reputFromOffset);
            this.reputFromOffset += result.getSize() - readSize;
        }
    }

    public long getReputFromOffset() {
        return reputFromOffset;
    }

    public void setReputFromOffset(long reputFromOffset) {
        this.reputFromOffset = reputFromOffset;
    }

    @Override
    public String getServiceName() {
        if (messageStore.getBrokerConfig().isInBrokerContainer()) {
            return messageStore.getBrokerIdentity().getIdentifier() + ReputMessageService.class.getSimpleName();
        }
        return ReputMessageService.class.getSimpleName();
    }

}

