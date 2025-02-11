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
package org.apache.rocketmq.store.domain.index;

import org.apache.rocketmq.common.app.AbstractBrokerRunnable;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.common.utils.TimeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.domain.dispatcher.DispatchRequest;
import org.apache.rocketmq.store.server.config.StorePathConfigHelper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * build index for messages with
 * - unique key
 * - keys
 */
public class IndexService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * Maximum times to attempt index file creation.
     */
    private static final int MAX_TRY_IDX_CREATE = 3;
    private final DefaultMessageStore defaultMessageStore;
    private final int hashSlotNum;
    private final int indexNum;
    private final String storePath;
    private final ArrayList<IndexFile> indexFileList = new ArrayList<>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public IndexService(final DefaultMessageStore store) {
        this.defaultMessageStore = store;
        this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
        this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
        this.storePath = StorePathConfigHelper.getStorePathIndex(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
    }

    public boolean load(final boolean lastExitOK) {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files == null) {
            return true;
        }

        // ascending order
        Arrays.sort(files);
        for (File file : files) {
            try {
                loadFile(file, lastExitOK);
            } catch (IOException e) {
                LOGGER.error("load file {} error", file, e);
                return false;
            } catch (NumberFormatException e) {
                LOGGER.error("load file {} error", file, e);
            }
        }

        return true;
    }

    /**
     * build index for messages with
     * - unique key
     * - keys
     *
     * @param req DispatchRequest from CommitLog
     */
    public void buildIndex(DispatchRequest req) {
        IndexFile indexFile = retryGetAndCreateIndexFile();
        if (indexFile == null) {
            LOGGER.error("build index error, stop building index");
            return;
        }

        if (req.getCommitLogOffset() < indexFile.getEndPhyOffset()) {
            return;
        }

        final int tranType = MessageSysFlag.getTransactionValue(req.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_ROLLBACK_TYPE) {
            return;
        }

        // index uniqueKey
        if (req.getUniqKey() != null) {
            indexFile = putKey(indexFile, req, buildKey(req.getTopic(), req.getUniqKey()));
            if (indexFile == null) {
                LOGGER.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                return;
            }
        }

        putKeys(req, indexFile);
    }

    /**
     *
     * @param topic topic
     * @param key key
     * @param maxNum maxNum
     * @param begin begin offset
     * @param end end offset
     * @return offset result
     */
    public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
        List<Long> phyOffsets = new ArrayList<>(maxNum);

        long indexLastUpdateTimestamp = 0;
        long indexLastUpdatePhyoffset = 0;
        maxNum = Math.min(maxNum, this.defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch());
        try {
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
            }

            for (int i = this.indexFileList.size(); i > 0; i--) {
                IndexFile f = this.indexFileList.get(i - 1);
                if (i == this.indexFileList.size()) {
                    indexLastUpdateTimestamp = f.getEndTimestamp();
                    indexLastUpdatePhyoffset = f.getEndPhyOffset();
                }

                if (f.isTimeMatched(begin, end)) {
                    f.selectPhyOffset(phyOffsets, buildKey(topic, key), maxNum, begin, end);
                }

                if (f.getBeginTimestamp() < begin) {
                    break;
                }

                if (phyOffsets.size() >= maxNum) {
                    break;
                }
            }

        } catch (Exception e) {
            LOGGER.error("queryMsg exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
    }

    private void loadFile(File file, final boolean lastExitOK) throws IOException {
        IndexFile f = new IndexFile(file.getPath(), this.hashSlotNum, this.indexNum, 0, 0);
        f.load();

        if (!lastExitOK) {
            if (f.getEndTimestamp() > this.defaultMessageStore.getStoreCheckpoint().getIndexMsgTimestamp()) {
                f.destroy(0);
                return;
            }
        }

        LOGGER.info("load index file OK, " + f.getFileName());
        this.indexFileList.add(f);
    }

    public long getTotalSize() {
        if (indexFileList.isEmpty()) {
            return 0;
        }

        return (long) indexFileList.get(0).getFileSize() * indexFileList.size();
    }

    public void deleteExpiredFile(long offset) {
        Object[] files = getFiles(offset);
        if (null == files) {
            return;
        }

        List<IndexFile> fileList = toFileList(files, offset);
        this.deleteExpiredFile(fileList);
    }

    private Object[] getFiles(long offset) {
        try {
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return null;
            }

            long endPhyOffset = this.indexFileList.get(0).getEndPhyOffset();
            if (endPhyOffset < offset) {
                return this.indexFileList.toArray();
            }
        } catch (Exception e) {
            LOGGER.error("destroy exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return null;
    }

    private List<IndexFile> toFileList(Object[] files, long offset) {
        List<IndexFile> fileList = new ArrayList<>();
        for (int i = 0; i < (files.length - 1); i++) {
            IndexFile f = (IndexFile) files[i];
            if (f.getEndPhyOffset() < offset) {
                fileList.add(f);
            } else {
                break;
            }
        }

        return fileList;
    }

    private void deleteExpiredFile(List<IndexFile> files) {
        if (files.isEmpty()) {
            return;
        }

        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile file : files) {
                boolean destroyed = file.destroy(3000);
                destroyed = destroyed && this.indexFileList.remove(file);

                if (!destroyed) {
                    LOGGER.error("deleteExpiredFile remove failed.");
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.error("deleteExpiredFile has exception.", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }

    public void destroy() {
        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                f.destroy(1000 * 3);
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            LOGGER.error("destroy exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }


    private String buildKey(final String topic, final String key) {
        return topic + "#" + key;
    }

    private void putKeys(DispatchRequest req, IndexFile file) {
        IndexFile indexFile = file;
        if (req.getKeys() == null || req.getKeys().length() <= 0) {
            return;
        }

        String[] keyset = req.getKeys().split(MessageConst.KEY_SEPARATOR);
        for (String key : keyset) {
            if (key.length() <= 0) {
                continue;
            }

            indexFile = putKey(indexFile, req, buildKey(req.getTopic(), key));
            if (indexFile == null) {
                LOGGER.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                return;
            }
        }
    }

    private IndexFile putKey(IndexFile indexFile, DispatchRequest msg, String idxKey) {
        for (boolean ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp()); !ok; ) {
            LOGGER.warn("Index file [" + indexFile.getFileName() + "] is full, trying to create another one");

            indexFile = retryGetAndCreateIndexFile();
            if (null == indexFile) {
                return null;
            }

            ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp());
        }

        return indexFile;
    }

    /**
     * Retries to get or create index file.
     * @renamed from retryGetAndCreateIndexFile to getOrCreateIndexFile
     *
     * @return {@link IndexFile} or null on failure.
     */
    public IndexFile retryGetAndCreateIndexFile() {
        IndexFile indexFile = null;

        for (int times = 0; null == indexFile && times < MAX_TRY_IDX_CREATE; times++) {
            indexFile = this.getAndCreateLastIndexFile();
            if (null != indexFile) {
                break;
            }

            LOGGER.info("Tried to create index file " + times + " times");
            ThreadUtils.sleep(1000, true);
        }

        if (null == indexFile) {
            this.defaultMessageStore.getRunningFlags().makeIndexFileError();
            LOGGER.error("Mark index file cannot build flag");
        }

        return indexFile;
    }

    public IndexFile getAndCreateLastIndexFile() {
        IndexFile indexFile = null;
        IndexFile prevIndexFile = null;
        long lastUpdateEndPhyOffset = 0;
        long lastUpdateIndexTimestamp = 0;

        this.readWriteLock.readLock().lock();
        if (!this.indexFileList.isEmpty()) {
            IndexFile tmp = this.indexFileList.get(this.indexFileList.size() - 1);
            if (!tmp.isWriteFull()) {
                indexFile = tmp;
            } else {
                lastUpdateEndPhyOffset = tmp.getEndPhyOffset();
                lastUpdateIndexTimestamp = tmp.getEndTimestamp();
                prevIndexFile = tmp;
            }
        }
        this.readWriteLock.readLock().unlock();

        if (indexFile != null) {
            return indexFile;
        }

        indexFile = createIndexFile(lastUpdateEndPhyOffset, lastUpdateIndexTimestamp);
        if (indexFile == null) {
            return null;
        }

        createFlushThread(prevIndexFile);
        return indexFile;
    }

    private void createFlushThread(IndexFile prevIndexFile) {
        final IndexFile flushThisFile = prevIndexFile;

        Thread flushThread = new Thread(new AbstractBrokerRunnable(defaultMessageStore.getBrokerConfig()) {
            @Override
            public void run0() {
                IndexService.this.flush(flushThisFile);
            }
        }, "FlushIndexFileThread");

        flushThread.setDaemon(true);
        flushThread.start();
    }

    private IndexFile createIndexFile(long lastUpdateEndPhyOffset, long lastUpdateIndexTimestamp) {
        try {
            String fileName = this.storePath + File.separator + TimeUtils.timeMillisToHumanString(System.currentTimeMillis());
            IndexFile indexFile = new IndexFile(fileName, this.hashSlotNum, this.indexNum, lastUpdateEndPhyOffset, lastUpdateIndexTimestamp);
            this.readWriteLock.writeLock().lock();
            this.indexFileList.add(indexFile);

            return indexFile;
        } catch (Exception e) {
            LOGGER.error("getLastIndexFile exception ", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }

        return null;
    }

    public void flush(final IndexFile f) {
        if (null == f) {
            return;
        }

        long indexMsgTimestamp = 0;

        if (f.isWriteFull()) {
            indexMsgTimestamp = f.getEndTimestamp();
        }

        f.flush();

        if (indexMsgTimestamp > 0) {
            this.defaultMessageStore.getStoreCheckpoint().setIndexMsgTimestamp(indexMsgTimestamp);
            this.defaultMessageStore.getStoreCheckpoint().flush();
        }
    }

    public void start() {

    }

    public void shutdown() {
        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                shutdownIndexFile(f);
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            LOGGER.error("shutdown exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }

    private void shutdownIndexFile(IndexFile f) {
        try {
            f.shutdown();
        } catch (Exception e) {
            LOGGER.error("shutdown " + f.getFileName() + " exception", e);
        }
    }
}
