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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.lang.thread.ServiceThread;
import org.apache.rocketmq.common.utils.SystemUtils;
import org.apache.rocketmq.common.utils.TimeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.domain.queue.ConsumeQueue;
import org.apache.rocketmq.store.infra.mappedfile.MappedFile;
import org.apache.rocketmq.store.infra.mappedfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.infra.memory.LibC;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import sun.nio.ch.DirectBuffer;

/**
 * @renamed from ColdDataCheckService to ColdDataCheckThread
 *
 * This method will do nothing, in default setting.
 *
 */
public class ColdDataCheckThread extends ServiceThread {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final DefaultMessageStore defaultMessageStore;
    private final ConcurrentHashMap<String, byte[]> pageCacheMap = new ConcurrentHashMap<>();
    private int pageSize = -1;
    private int sampleSteps;

    public ColdDataCheckThread(final DefaultMessageStore messageStore) {
        defaultMessageStore = messageStore;
        sampleSteps = defaultMessageStore.getMessageStoreConfig().getSampleSteps();
        if (sampleSteps <= 0) {
            sampleSteps = 32;
        }
        initPageSize();
        scanFilesInPageCache();
    }

    @Override
    public String getServiceName() {
        return ColdDataCheckThread.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                if (SystemUtils.isWindows()
                    || !defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()
                    || !defaultMessageStore.getMessageStoreConfig().isColdDataScanEnable()) {

                    pageCacheMap.clear();
                    this.waitForRunning(180 * 1000);
                    continue;
                } else {
                    this.waitForRunning(defaultMessageStore.getMessageStoreConfig().getTimerColdDataCheckIntervalMs());
                }

                if (pageSize < 0) {
                    initPageSize();
                }

                long beginClockTimestamp = TimeUtils.now();
                scanFilesInPageCache();
                long costTime = TimeUtils.now() - beginClockTimestamp;
                log.info("[{}] scanFilesInPageCache-cost {} ms.", costTime > 30 * 1000 ? "NOTIFYME" : "OK", costTime);
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has e: {}", e);
            }
        }
        log.info("{} service end", this.getServiceName());
    }

    /**
     *
     * @param offset offset of commitLog while getMessage
     * @return status
     */
    public boolean isDataInPageCache(final long offset) {
        if (!defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()) {
            return true;
        }
        if (pageSize <= 0 || sampleSteps <= 0) {
            return true;
        }
        if (!checkInColdAreaByCommitOffset(offset, defaultMessageStore.getCommitLog().getMaxOffset())) {
            return true;
        }
        if (!defaultMessageStore.getMessageStoreConfig().isColdDataScanEnable()) {
            return false;
        }

        MappedFile mappedFile = defaultMessageStore.getCommitLog().getMappedFileQueue().findMappedFileByOffset(offset, offset == 0);
        if (null == mappedFile) {
            return true;
        }
        byte[] bytes = pageCacheMap.get(mappedFile.getFileName());
        if (null == bytes) {
            return true;
        }

        int pos = (int) (offset % defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog());
        int realIndex = pos / pageSize / sampleSteps;
        return bytes.length - 1 >= realIndex && bytes[realIndex] != 0;
    }

    /**
     * this result is not high accurate.
     */
    public boolean isMsgInColdArea(String group, String topic, int queueId, long offset) {
        if (!defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()) {
            return false;
        }

        try {
            ConsumeQueue consumeQueue = (ConsumeQueue) defaultMessageStore.findConsumeQueue(topic, queueId);
            if (null == consumeQueue) {
                return false;
            }
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);
            if (null == bufferConsumeQueue || null == bufferConsumeQueue.getByteBuffer()) {
                return false;
            }
            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
            return checkInColdAreaByCommitOffset(offsetPy, defaultMessageStore.getCommitLog().getMaxOffset());
        } catch (Exception e) {
            log.error("isMsgInColdArea group: {}, topic: {}, queueId: {}, offset: {}",
                group, topic, queueId, offset, e);
        }
        return false;
    }

    private void scanFilesInPageCache() {
        if (SystemUtils.isWindows()
            || !defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()
            || !defaultMessageStore.getMessageStoreConfig().isColdDataScanEnable()
            || pageSize <= 0) {
            return;
        }

        try {
            log.info("pageCacheMap key size: {}", pageCacheMap.size());
            clearExpireMappedFile();
            defaultMessageStore.getCommitLog().getMappedFileQueue().getMappedFiles().forEach(mappedFile -> {
                byte[] pageCacheTable = checkFileInPageCache(mappedFile);
                if (sampleSteps > 1) {
                    pageCacheTable = sampling(pageCacheTable, sampleSteps);
                }
                pageCacheMap.put(mappedFile.getFileName(), pageCacheTable);
            });
        } catch (Exception e) {
            log.error("scanFilesInPageCache exception", e);
        }
    }

    private void clearExpireMappedFile() {
        Set<String> currentFileSet = defaultMessageStore
            .getCommitLog()
            .getMappedFileQueue()
            .getMappedFiles()
            .stream()
            .map(MappedFile::getFileName)
            .collect(Collectors.toSet());

        pageCacheMap.forEach((key, value) -> {
            if (!currentFileSet.contains(key)) {
                pageCacheMap.remove(key);
                log.info("clearExpireMappedFile fileName: {}, has been clear", key);
            }
        });
    }

    private byte[] sampling(byte[] pageCacheTable, int sampleStep) {
        int sampleLength = (pageCacheTable.length + sampleStep - 1) / sampleStep;
        byte[] sample = new byte[sampleLength];

        for (int i = 0, j = 0; i < pageCacheTable.length && j < sample.length; i += sampleStep) {
            sample[j++] = pageCacheTable[i];
        }
        return sample;
    }

    private byte[] checkFileInPageCache(MappedFile mappedFile) {
        long fileSize = mappedFile.getFileSize();
        final long address = ((DirectBuffer) mappedFile.getMappedByteBuffer()).address();
        int pageNums = (int) (fileSize + this.pageSize - 1) / this.pageSize;
        byte[] pageCacheRst = new byte[pageNums];
        int mincore = LibC.INSTANCE.mincore(new Pointer(address), new NativeLong(fileSize), pageCacheRst);
        if (mincore == 0) {
            return pageCacheRst;
        }

        log.error("checkFileInPageCache call the LibC.INSTANCE.mincore error, fileName: {}, fileSize: {}", mappedFile.getFileName(), fileSize);
        for (int i = 0; i < pageNums; i++) {
            pageCacheRst[i] = 1;
        }
        return pageCacheRst;
    }

    /**
     * get system page size by LibC, disable cold data control if windows
     */
    private void initPageSize() {
        if (pageSize >= 0 || !defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()) {
            return;
        }

        try {
            if (!SystemUtils.isWindows()) {
                pageSize = LibC.INSTANCE.getpagesize();
            } else {
                defaultMessageStore.getMessageStoreConfig().setColdDataFlowControlEnable(false);
                log.info("windows os, coldDataCheckEnable force setting to be false");
            }
            log.info("initPageSize pageSize: {}", pageSize);
        } catch (Exception e) {
            defaultMessageStore.getMessageStoreConfig().setColdDataFlowControlEnable(false);
            log.error("initPageSize error, coldDataCheckEnable force setting to be false ", e);
        }
    }

    /**
     * The ratio val is estimated by the experiment and experience
     * so that the result is not high accurate for different business
     *
     * @param  offsetPy start offset of getting message process
     * @param maxOffsetPy commitLog maxOffset
     * @return bool
     */
    private boolean checkInColdAreaByCommitOffset(long offsetPy, long maxOffsetPy) {
        long memory = (long)(SystemUtils.TOTAL_PHYSICAL_MEMORY_SIZE * (defaultMessageStore.getMessageStoreConfig().getAccessMessageInMemoryHotRatio() / 100.0));
        return (maxOffsetPy - offsetPy) > memory;
    }
}
