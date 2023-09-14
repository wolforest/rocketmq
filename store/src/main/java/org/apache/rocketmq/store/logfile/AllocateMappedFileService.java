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
package org.apache.rocketmq.store.logfile;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.TimeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.BrokerRole;

import java.io.IOException;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Create MappedFile in advance
 */
public class AllocateMappedFileService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int waitTimeOut = 1000 * 5;
    private ConcurrentMap<String, AllocateRequest> requestTable =
        new ConcurrentHashMap<>();
    private PriorityBlockingQueue<AllocateRequest> requestQueue =
        new PriorityBlockingQueue<>();
    private volatile boolean hasException = false;
    private DefaultMessageStore messageStore;

    public AllocateMappedFileService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped() && this.mmapOperation()) {

        }
        log.info(this.getServiceName() + " service end");
    }

    public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
        int canSubmitRequests = 2;

        if (this.messageStore.isTransientStorePoolEnable()) {
            if (this.messageStore.getMessageStoreConfig().isFastFailIfNoBufferInStorePool()
                && BrokerRole.SLAVE != this.messageStore.getMessageStoreConfig().getBrokerRole()) { //if broker is slave, don't fast fail even no buffer in pool
                //if broker is slave, don't fast fail even no buffer in pool
                canSubmitRequests = this.messageStore.remainTransientStoreBufferNumbs() - this.requestQueue.size();
            }
        }

        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;

        if (nextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so create mapped file error, " +
                    "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.remainTransientStoreBufferNumbs());
                this.requestTable.remove(nextFilePath);
                return null;
            }
            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) {
                log.warn("never expected here, add a request to preallocate queue failed");
            }
            canSubmitRequests--;
        }

        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;
        if (nextNextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, " +
                    "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.remainTransientStoreBufferNumbs());
                this.requestTable.remove(nextNextFilePath);
            } else {
                boolean offerOK = this.requestQueue.offer(nextNextReq);
                if (!offerOK) {
                    log.warn("never expected here, add a request to preallocate queue failed");
                }
            }
        }

        if (hasException) {
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }

        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                messageStore.getPerfCounter().startTick("WAIT_MAPFILE_TIME_MS");
                boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
                messageStore.getPerfCounter().endTick("WAIT_MAPFILE_TIME_MS");
                if (!waitOK) {
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                    return null;
                } else {
                    this.requestTable.remove(nextFilePath);
                    return result.getMappedFile();
                }
            } else {
                log.error("find preallocate mmap failed, this never happen");
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }

        return null;
    }

    @Override
    public String getServiceName() {
        if (messageStore != null && messageStore.getBrokerConfig().isInBrokerContainer()) {
            return messageStore.getBrokerIdentity().getIdentifier() + AllocateMappedFileService.class.getSimpleName();
        }
        return AllocateMappedFileService.class.getSimpleName();
    }

    @Override
    public void shutdown() {
        super.shutdown(true);

        for (AllocateRequest req : this.requestTable.values()) {
            if (req.mappedFile == null) {
                continue;
            }

            log.info("delete pre allocated maped file, {}", req.mappedFile.getFileName());
            req.mappedFile.destroy(1000);
        }
    }

    /**
     * Only interrupted by the external thread, will return false
     */
    private boolean mmapOperation() {
        boolean isSuccess = false;
        AllocateRequest req = null;
        try {
            req = this.requestQueue.take();
            AllocateRequest expectedRequest = this.requestTable.get(req.getFilePath());
            if (null == expectedRequest) {
                log.warn("this mmap request expired, maybe cause timeout " + req.getFilePath() + " "
                    + req.getFileSize());
                return true;
            }
            if (expectedRequest != req) {
                log.warn("never expected here,  maybe cause timeout " + req.getFilePath() + " "
                    + req.getFileSize() + ", req:" + req + ", expectedRequest:" + expectedRequest);
                return true;
            }

            if (req.getMappedFile() != null) {
                return true;
            }

            MappedFile mappedFile = createMappedFile(req);
            warmMappedFile(mappedFile);

            req.setMappedFile(mappedFile);
            this.hasException = false;
            isSuccess = true;
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " interrupted, possibly by shutdown.");
            this.hasException = true;
            return false;
        } catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            this.hasException = true;
            if (null != req) {
                requestQueue.offer(req);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        } finally {
            if (req != null && isSuccess)
                req.getCountDownLatch().countDown();
        }
        return true;
    }

    private void warmMappedFile(MappedFile mappedFile) {
        // warm mappedFile
        if (mappedFile.getFileSize() >= this.messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog()
            && this.messageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {

            mappedFile.warmMappedFile(this.messageStore.getMessageStoreConfig().getFlushDiskType(),
                this.messageStore.getMessageStoreConfig().getFlushLeastPagesWhenWarmMapedFile());
        }
    }

    private MappedFile createMappedFile(AllocateRequest req) throws IOException {
        long beginTime = System.currentTimeMillis();

        MappedFile mappedFile;
        if (messageStore.isTransientStorePoolEnable()) {
            try {
                mappedFile = ServiceLoader.load(MappedFile.class).iterator().next();
                mappedFile.init(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
            } catch (RuntimeException e) {
                log.warn("Use default implementation.");
                mappedFile = new DefaultMappedFile(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
            }
        } else {
            mappedFile = new DefaultMappedFile(req.getFilePath(), req.getFileSize());
        }

        long elapsedTime = TimeUtils.computeElapsedTimeMilliseconds(beginTime);
        if (elapsedTime > 10) {
            int queueSize = this.requestQueue.size();
            log.warn("create mappedFile spent time(ms) " + elapsedTime + " queue size " + queueSize
                + " " + req.getFilePath() + " " + req.getFileSize());
        }

        return mappedFile;
    }
}
