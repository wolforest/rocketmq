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
package org.apache.rocketmq.store.infra.mappedfile;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.lang.BoundaryType;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.domain.commitlog.CommitLog;
import org.apache.rocketmq.store.domain.queue.ConsumeQueue;
import org.apache.rocketmq.store.infra.memory.Swappable;

public class MappedFileQueue implements Swappable {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    protected final String storePath;

    /**
     * all mappedFiles have same fixed length
     * different implements have different length:
     *  - commitLog
     *  - consumeQueue
     *  - timerLog
     */
    protected final int mappedFileSize;

    /**
     * MappedFile Queue
     */
    protected final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<>();

    protected final AllocateMappedFileService allocateMappedFileService;

    /**
     * @renamed from flushedWhere to flushedPosition
     */
    protected long flushedPosition = 0;
    /**
     * @renamed from committedWhere to committedPosition
     */
    protected long committedPosition = 0;

    /**
     * updated after flushing
     */
    protected volatile long storeTimestamp = 0;

    public MappedFileQueue(final String storePath, int mappedFileSize, AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    /**
     *  checking whether the offset in file queue is sequential
     *      if not log the result, because if not the file queue is damaged.
     *  checking the status of below, and log the result:
     *     (cur.getOffsetInFileName() - pre.getOffsetInFileName() == this.mappedFileSize)
     *  called by commitLog, consumeQueue, and self
     *
     */
    public void checkSelf() {
        List<MappedFile> mappedFiles = new ArrayList<>(this.mappedFiles);
        if (mappedFiles.isEmpty()) {
            return;
        }

        Iterator<MappedFile> iterator = mappedFiles.iterator();
        MappedFile pre = null;
        while (iterator.hasNext()) {
            MappedFile cur = iterator.next();
            checkFileOffset(pre, cur);

            pre = cur;
        }
    }

    /**
     * method for consume queue
     * @param timestamp ts
     * @param commitLog commitLog
     * @param boundaryType boundaryType
     * @return MappedFile
     */
    public MappedFile getConsumeQueueMappedFileByTime(final long timestamp, CommitLog commitLog, BoundaryType boundaryType) {
        Object[] mfs = getMappedFileArray();
        if (null == mfs) {
            return null;
        }

        this.adjustFilesOffset(mfs, commitLog);

        switch (boundaryType) {
            case LOWER: {
                return this.getFileByTimeForLowerBoundary(timestamp, mfs);
            }
            case UPPER: {
                return this.getFileByTimeForUpperBoundary(timestamp, mfs);
            }
            default: {
                log.warn("Unknown boundary type");
                return null;
            }
        }
    }


    public boolean load() {
        File dir = new File(this.storePath);
        File[] ls = dir.listFiles();
        if (ls != null) {
            return doLoad(Arrays.asList(ls));
        }
        return true;
    }

    /**
     * get last mappedFile
     * should be protected/private, only called by self
     *
     * @param startOffset startOffset
     * @param needCreate createIfNotExists
     * @return MappedFile
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }

        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getOffsetInFileName() + this.mappedFileSize;
        }

        if (createOffset != -1 && needCreate) {
            return tryCreateMappedFile(createOffset);
        }

        return mappedFileLast;
    }

    public boolean isMappedFilesEmpty() {
        return this.mappedFiles.isEmpty();
    }

    public boolean isEmptyOrCurrentFileFull() {
        MappedFile mappedFileLast = getLastMappedFile();
        if (mappedFileLast == null) {
            return true;
        }
        return mappedFileLast.isFull();
    }

    public MappedFile tryCreateMappedFile(long createOffset) {
        String nextFilePath = this.storePath + File.separator + IOUtils.offset2FileName(createOffset);
        String nextNextFilePath = this.storePath + File.separator + IOUtils.offset2FileName(createOffset
                + this.mappedFileSize);
        return doCreateMappedFile(nextFilePath, nextNextFilePath);
    }

    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;
        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }
        return mappedFileLast;
    }

    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getOffsetInFileName() +
                mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator(mappedFiles.size());
        List<MappedFile> toRemoves = new ArrayList<>();

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getOffsetInFileName()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                toRemoves.add(mappedFileLast);
            }
        }

        if (!toRemoves.isEmpty()) {
            this.mappedFiles.removeAll(toRemoves);
        }

        return true;
    }

    public long getMinOffset() {
        if (this.mappedFiles.isEmpty()) {
            return -1;
        }

        try {
            return this.mappedFiles.get(0).getOffsetInFileName();
        } catch (IndexOutOfBoundsException e) {
            //continue;
        } catch (Exception e) {
            log.error("getMinOffset has exception.", e);
        }
        return -1;
    }

    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile == null) {
            return 0;
        }

        return mappedFile.getOffsetInFileName() + mappedFile.getWroteOrCommitPosition();
    }

    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile == null) {
            return 0;
        }

        return mappedFile.getOffsetInFileName() + mappedFile.getWrotePosition();
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - getCommittedPosition();
    }

    public long remainHowManyDataToFlush() {
        return getMaxOffset() - this.getFlushedPosition();
    }

    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile == null) {
            return;
        }

        lastMappedFile.destroy(1000);
        this.mappedFiles.remove(lastMappedFile);
        log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());
    }

    public int deleteExpiredFileByTime(final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately,
        final int deleteFileBatchMax) {
        Object[] mfs = this.getMappedFileArray();

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<>();
        int skipFileNum = 0;
        if (null != mfs) {
            //do check before deleting
            checkSelf();
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    if (skipFileNum > 0) {
                        log.info("Delete CommitLog {} but skip {} files", mappedFile.getFileName(), skipFileNum);
                    }
                    if (mappedFile.destroy(intervalForcibly)) {
                        files.add(mappedFile);
                        deleteCount++;

                        if (files.size() >= deleteFileBatchMax) {
                            break;
                        }

                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    skipFileNum++;
                    //avoid deleting files in the middle
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.getMappedFileArray();

        List<MappedFile> files = new ArrayList<>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                            + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    public int deleteExpiredFileByOffsetForTimerLog(long offset, int checkOffset, int unitSize) {
        Object[] mfs = this.getMappedFileArray();

        List<MappedFile> files = new ArrayList<>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy = false;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(checkOffset);
                try {
                    if (result != null) {
                        int position = result.getByteBuffer().position();
                        int size = result.getByteBuffer().getInt();//size
                        result.getByteBuffer().getLong(); //prev pos
                        int magic = result.getByteBuffer().getInt();
                        if (size == unitSize && (magic | 0xF) == 0xF) {
                            result.getByteBuffer().position(position + MQConstants.UNIT_PRE_SIZE_FOR_MSG);
                            long maxOffsetPy = result.getByteBuffer().getLong();
                            destroy = maxOffsetPy < offset;
                            if (destroy) {
                                log.info("physic min commitlog offset " + offset + ", current mappedFile's max offset "
                                    + maxOffsetPy + ", delete it");
                            }
                        } else {
                            log.warn("Found error data in [{}] checkOffset:{} unitSize:{}", mappedFile.getFileName(),
                                checkOffset, unitSize);
                        }
                    } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                        log.warn("Found a hanged consume queue file, attempting to delete it.");
                        destroy = true;
                    } else {
                        log.warn("this being not executed forever.");
                        break;
                    }
                } finally {
                    if (null != result) {
                        result.release();
                    }
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     *
     * @param flushLeastPages flushLeastPages
     * @return status
     */
    public boolean flush(final int flushLeastPages) {
        MappedFile mappedFile = this.findMappedFileByOffset(this.getFlushedPosition(), this.getFlushedPosition() == 0);
        if (mappedFile == null) {
            return true;
        }


        long tmpTimeStamp = mappedFile.getStoreTimestamp();
        int offset = mappedFile.flush(flushLeastPages);
        long where = mappedFile.getOffsetInFileName() + offset;

        boolean result = where == this.getFlushedPosition();
        this.setFlushedPosition(where);

        if (0 == flushLeastPages) {
            this.setStoreTimestamp(tmpTimeStamp);
        }

        return result;
    }

    public synchronized boolean commit(final int commitLeastPages) {
        MappedFile mappedFile = this.findMappedFileByOffset(this.getCommittedPosition(), this.getCommittedPosition() == 0);
        if (mappedFile == null) {
            return true;
        }

        int offset = mappedFile.commit(commitLeastPages);
        long where = mappedFile.getOffsetInFileName() + offset;

        boolean result = where == this.getCommittedPosition();
        this.setCommittedPosition(where);

        return result;
    }

    /**
     * Finds a mapped file by offset.
     *
     * @param offset Offset.
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MappedFile firstMappedFile = this.getFirstMappedFile();
            MappedFile lastMappedFile = this.getLastMappedFile();
            if (null == firstMappedFile || null == lastMappedFile) {
                return null;
            }

            if (offset < firstMappedFile.getOffsetInFileName() || offset >= lastMappedFile.getOffsetInFileName() + this.mappedFileSize) {
                LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                    offset,
                    firstMappedFile.getOffsetInFileName(),
                    lastMappedFile.getOffsetInFileName() + this.mappedFileSize,
                    this.mappedFileSize,
                    this.mappedFiles.size());
            } else {
                int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getOffsetInFileName() / this.mappedFileSize));
                MappedFile targetFile = null;
                try {
                    targetFile = this.mappedFiles.get(index);
                } catch (Exception ignored) {
                }

                if (targetFile != null && offset >= targetFile.getOffsetInFileName()
                    && offset < targetFile.getOffsetInFileName() + this.mappedFileSize) {
                    return targetFile;
                }

                for (MappedFile tmpMappedFile : this.mappedFiles) {
                    if (offset >= tmpMappedFile.getOffsetInFileName()
                        && offset < tmpMappedFile.getOffsetInFileName() + this.mappedFileSize) {
                        return tmpMappedFile;
                    }
                }
            }

            if (returnFirstOnNotFound) {
                return firstMappedFile;
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    public MappedFile getFirstMappedFile() {
        if (this.mappedFiles.isEmpty()) {
            return null;
        }

        try {
            return this.mappedFiles.get(0);
        } catch (IndexOutOfBoundsException e) {
            //ignore
        } catch (Exception e) {
            log.error("getFirstMappedFile has exception.", e);
        }

        return null;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.getMappedFileArray();
        if (mfs == null) {
            return 0;
        }

        for (Object mf : mfs) {
            if (((ReferenceResource) mf).isAvailable()) {
                size += this.mappedFileSize;
            }
        }

        return size;
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile == null) {
            return false;
        }

        if (mappedFile.isAvailable()) {
            return false;
        }

        log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
        boolean result = mappedFile.destroy(intervalForcibly);
        if (!result) {
            log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
            return false;
        }

        log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
        List<MappedFile> tmpFiles = new ArrayList<>();
        tmpFiles.add(mappedFile);
        this.deleteExpiredFile(tmpFiles);

        return true;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.setFlushedPosition(0);

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
        if (mappedFiles.isEmpty()) {
            return;
        }

        if (reserveNum < 3) {
            reserveNum = 3;
        }

        Object[] mfs = this.getMappedFileArray();
        if (null == mfs) {
            return;
        }

        for (int i = mfs.length - reserveNum - 1; i >= 0; i--) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > forceSwapIntervalMs) {
                mappedFile.swapMap();
                continue;
            }
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > normalSwapIntervalMs
                    && mappedFile.getMappedByteBufferAccessCountSinceLastSwap() > 0) {
                mappedFile.swapMap();
            }
        }
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {
        if (mappedFiles.isEmpty()) {
            return;
        }

        int reserveNum = 3;
        Object[] mfs = this.getMappedFileArray();
        if (null == mfs) {
            return;
        }

        for (int i = mfs.length - reserveNum - 1; i >= 0; i--) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > forceCleanSwapIntervalMs) {
                mappedFile.cleanSwapedMap(false);
            }
        }
    }

    /**
     * get MappedFile list by range
     * called by ConsumeQueue
     *
     * @param from startOffset
     * @param to endOffset
     * @return list
     */
    public List<MappedFile> range(final long from, final long to) {
        Object[] mfs = getMappedFileArray();
        if (null == mfs) {
            return new ArrayList<>();
        }

        List<MappedFile> result = new ArrayList<>();
        for (Object mf : mfs) {
            MappedFile mappedFile = (MappedFile) mf;
            if (mappedFile.getOffsetInFileName() + mappedFile.getFileSize() <= from) {
                continue;
            }

            if (to <= mappedFile.getOffsetInFileName()) {
                break;
            }
            result.add(mappedFile);
        }

        return result;
    }

    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<>();

        for (MappedFile file : this.mappedFiles) {
            long fileTailOffset = file.getOffsetInFileName() + this.mappedFileSize;
            if (fileTailOffset <= offset) {
                continue;
            }

            if (offset >= file.getOffsetInFileName()) {
                file.setWrotePosition((int) (offset % this.mappedFileSize));
                file.setCommittedPosition((int) (offset % this.mappedFileSize));
                file.setFlushedPosition((int) (offset % this.mappedFileSize));
            } else {
                file.destroy(1000);
                willRemoveFiles.add(file);
            }
        }

        this.deleteExpiredFile(willRemoveFiles);
    }

    public void deleteExpiredFile(List<MappedFile> files) {
        if (files.isEmpty()) {
            return;
        }

        Iterator<MappedFile> iterator = files.iterator();
        while (iterator.hasNext()) {
            MappedFile cur = iterator.next();
            if (!this.mappedFiles.contains(cur)) {
                iterator.remove();
                log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
            }
        }

        try {
            if (!this.mappedFiles.removeAll(files)) {
                log.error("deleteExpiredFile remove failed.");
            }
        } catch (Exception e) {
            log.error("deleteExpiredFile has exception.", e);
        }
    }

    /***************  private/protected/useless method  **************/
    protected boolean doLoad(List<File> files) {
        // ascending order
        files.sort(Comparator.comparing(File::getName));

        for (int i = 0; i < files.size(); i++) {
            File file = files.get(i);
            if (file.isDirectory()) {
                continue;
            }

            if (file.length() == 0 && i == files.size() - 1) {
                boolean ok = file.delete();
                log.warn("{} size is 0, auto delete. is_ok: {}", file, ok);
                continue;
            }

            if (file.length() != this.mappedFileSize) {
                log.warn(file + "\t" + file.length()
                    + " length not matched message store config value, please check it manually");
                return false;
            }

            try {
                MappedFile mappedFile = new DefaultMappedFile(file.getPath(), mappedFileSize);

                mappedFile.setWrotePosition(this.mappedFileSize);
                mappedFile.setFlushedPosition(this.mappedFileSize);
                mappedFile.setCommittedPosition(this.mappedFileSize);
                this.mappedFiles.add(mappedFile);
                log.info("load " + file.getPath() + " OK");
            } catch (IOException e) {
                log.error("load file " + file + " error", e);
                return false;
            }
        }
        return true;
    }

    protected MappedFile doCreateMappedFile(String nextFilePath, String nextNextFilePath) {
        MappedFile mappedFile = null;

        if (this.allocateMappedFileService != null) {
            mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                nextNextFilePath, this.mappedFileSize);
        } else {
            try {
                mappedFile = new DefaultMappedFile(nextFilePath, this.mappedFileSize);
            } catch (IOException e) {
                log.error("create mappedFile exception", e);
            }
        }

        if (mappedFile != null) {
            if (this.mappedFiles.isEmpty()) {
                mappedFile.setFirstCreateInQueue(true);
            }
            this.mappedFiles.add(mappedFile);
        }

        return mappedFile;
    }

    public Stream<MappedFile> stream() {
        return this.mappedFiles.stream();
    }

    public Stream<MappedFile> reversedStream() {
        return Lists.reverse(this.mappedFiles).stream();
    }

    public Object[] snapshot() {
        // return a safe copy
        return this.mappedFiles.toArray();
    }

    public MappedFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.getMappedFileArray();

        if (null == mfs)
            return null;

        for (Object mf : mfs) {
            MappedFile mappedFile = (MappedFile) mf;
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        return (MappedFile) mfs[mfs.length - 1];
    }

    /**
     * @renamed from copyMappedFiles to getMappedFileArray
     * @return array | null
     */
    protected Object[] getMappedFileArray() {
        Object[] mfs;

        if (this.mappedFiles.isEmpty()) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    private void checkFileOffset(MappedFile pre, MappedFile cur) {
        if (pre == null) {
            return;
        }

        if (cur.getOffsetInFileName() - pre.getOffsetInFileName() == this.mappedFileSize) {
            return;
        }

        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
            pre.getFileName(), cur.getFileName());
    }

    /**
     * Make sure each mapped file in consume queue has accurate start and stop time in accordance with commit log
     * mapped files. Note last modified time from file system is not reliable.
     */
    private void adjustFilesOffset(Object[] mfs, CommitLog commitLog) {
        for (int i = mfs.length - 1; i >= 0; i--) {
            DefaultMappedFile mappedFile = (DefaultMappedFile) mfs[i];
            // Figure out the earliest message store time in the consume queue mapped file.
            if (mappedFile.getStartTimestamp() < 0) {
                this.adjustFilesStartOffset(mappedFile, commitLog);
            }
            // Figure out the latest message store time in the consume queue mapped file.
            if (i < mfs.length - 1 && mappedFile.getStopTimestamp() < 0) {
                this.adjustFilesEndOffset(mappedFile, commitLog);
            }
        }
    }

    private void adjustFilesStartOffset(DefaultMappedFile mappedFile, CommitLog commitLog) {
        SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(0, ConsumeQueue.CQ_STORE_UNIT_SIZE);
        if (null == selectMappedBufferResult) {
            return;
        }
        try {
            ByteBuffer buffer = selectMappedBufferResult.getByteBuffer();
            long physicalOffset = buffer.getLong();
            int messageSize = buffer.getInt();
            long messageStoreTime = commitLog.pickupStoreTimestamp(physicalOffset, messageSize);
            if (messageStoreTime > 0) {
                mappedFile.setStartTimestamp(messageStoreTime);
            }
        } finally {
            selectMappedBufferResult.release();
        }
    }

    private void adjustFilesEndOffset(DefaultMappedFile mappedFile, CommitLog commitLog) {
        SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(mappedFileSize - ConsumeQueue.CQ_STORE_UNIT_SIZE, ConsumeQueue.CQ_STORE_UNIT_SIZE);
        if (null == selectMappedBufferResult) {
            return;
        }

        try {
            ByteBuffer buffer = selectMappedBufferResult.getByteBuffer();
            long physicalOffset = buffer.getLong();
            int messageSize = buffer.getInt();
            long messageStoreTime = commitLog.pickupStoreTimestamp(physicalOffset, messageSize);
            if (messageStoreTime > 0) {
                mappedFile.setStopTimestamp(messageStoreTime);
            }
        } finally {
            selectMappedBufferResult.release();
        }
    }

    private MappedFile getFileByTimeForLowerBoundary(final long timestamp, Object[] mfs) {
        for (int i = 0; i < mfs.length; i++) {
            DefaultMappedFile mappedFile = (DefaultMappedFile) mfs[i];
            if (i < mfs.length - 1) {
                long stopTimestamp = mappedFile.getStopTimestamp();
                if (stopTimestamp >= timestamp) {
                    return mappedFile;
                }
            }

            // Just return the latest one.
            if (i == mfs.length - 1) {
                return mappedFile;
            }
        }

        return null;
    }

    private MappedFile getFileByTimeForUpperBoundary(final long timestamp, Object[] mfs) {
        for (int i = mfs.length - 1; i >= 0; i--) {
            DefaultMappedFile mappedFile = (DefaultMappedFile) mfs[i];
            if (mappedFile.getStartTimestamp() <= timestamp) {
                return mappedFile;
            }
        }

        return null;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.getFlushedPosition();
        if (committed == 0) {
            return 0;
        }

        MappedFile mappedFile = this.getLastMappedFile(0, false);
        if (mappedFile == null) {
            return 0;
        }

        return (mappedFile.getOffsetInFileName() + mappedFile.getWrotePosition()) - committed;
    }

    public boolean shouldRoll(final int msgSize) {
        if (isEmptyOrCurrentFileFull()) {
            return true;
        }
        MappedFile mappedFileLast = getLastMappedFile();
        return mappedFileLast.getWrotePosition() + msgSize > mappedFileLast.getFileSize();
    }

    /***************  getter/setter method  **************/
    public long getFlushedPosition() {
        return flushedPosition;
    }

    public void setFlushedPosition(long flushedPosition) {
        this.flushedPosition = flushedPosition;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedPosition() {
        return committedPosition;
    }

    public void setCommittedPosition(final long committedPosition) {
        this.committedPosition = committedPosition;
    }

    public String getStorePath() {
        return storePath;
    }

    public long getTotalFileSize() {
        return (long) mappedFileSize * mappedFiles.size();
    }
}
