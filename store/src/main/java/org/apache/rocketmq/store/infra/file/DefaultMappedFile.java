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
package org.apache.rocketmq.store.infra.file;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import org.apache.commons.lang3.SystemUtils;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageExtBatch;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.common.utils.TimeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.api.dto.AppendMessageResult;
import org.apache.rocketmq.store.api.dto.AppendMessageStatus;
import org.apache.rocketmq.store.domain.compaction.CompactionAppendMsgCallback;
import org.apache.rocketmq.store.domain.message.PutMessageContext;
import org.apache.rocketmq.store.infra.TransientStorePool;
import org.apache.rocketmq.store.domain.commitlog.AppendMessageCallback;
import org.apache.rocketmq.store.server.config.FlushDiskType;
import org.apache.rocketmq.store.infra.LibC;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultMappedFile extends AbstractMappedFile {
    public static final int OS_PAGE_SIZE = 1024 * 4;
    public static final Unsafe UNSAFE = getUnsafe();
    private static final Method IS_LOADED_METHOD;
    public static final int UNSAFE_PAGE_SIZE = UNSAFE == null ? OS_PAGE_SIZE : UNSAFE.pageSize();

    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * total memory size of mapped files in the jvm
     * increase in method init()
     * decrease in method cleanup()
     */
    protected static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    /**
     * total numbers of mapped files in the jvm
     * increase in method init()
     * decrease in method cleanup()
     */
    protected static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    /**
     * atomic updater for wrotePosition
     */
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> WROTE_POSITION_UPDATER;
    /**
     * atomic updater for committedPosition
     */
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> COMMITTED_POSITION_UPDATER;
    /**
     * atomic updater for flushedPosition
     */
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> FLUSHED_POSITION_UPDATER;

    protected volatile int wrotePosition;
    protected volatile int committedPosition;
    protected volatile int flushedPosition;

    protected String fileName;
    /**
     * offset from filename
     * It is the logical offset of MappedFileQueue
     *
     * @renamed from fileFromOffset to offsetInFileName
     */
    protected long offsetInFileName;
    protected File file;
    protected int fileSize;
    protected FileChannel fileChannel;

    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    protected ByteBuffer writeBuffer = null;
    protected TransientStorePool transientStorePool = null;
    protected MappedByteBuffer mappedByteBuffer;
    
    protected volatile long storeTimestamp = 0;
    protected boolean firstCreateInQueue = false;
    private long lastFlushTime = -1L;

    protected MappedByteBuffer mappedByteBufferWaitToClean = null;
    protected long swapMapTime = 0L;
    protected long mappedByteBufferAccessCountSinceLastSwap = 0L;

    /**
     * If this mapped file belongs to consume queue, this field stores store-timestamp of first message referenced
     * by this logical queue.
     */
    private long startTimestamp = -1;

    /**
     * If this mapped file belongs to consume queue, this field stores store-timestamp of last message referenced
     * by this logical queue.
     */
    private long stopTimestamp = -1;

    static {
        WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "wrotePosition");
        COMMITTED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "committedPosition");
        FLUSHED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "flushedPosition");

        Method isLoaded0method = null;
        // On the windows platform and openjdk 11 method isLoaded0 always returns false.
        // see https://github.com/AdoptOpenJDK/openjdk-jdk11/blob/19fb8f93c59dfd791f62d41f332db9e306bc1422/src/java.base/windows/native/libnio/MappedByteBuffer.c#L34
        if (!SystemUtils.IS_OS_WINDOWS) {
            try {
                isLoaded0method = MappedByteBuffer.class.getDeclaredMethod("isLoaded0", long.class, long.class, int.class);
                isLoaded0method.setAccessible(true);
            } catch (NoSuchMethodException ignore) {
            }
        }
        IS_LOADED_METHOD = isLoaded0method;
    }

    public DefaultMappedFile() {
    }

    public DefaultMappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public DefaultMappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    @Override
    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.offsetInFileName = Long.parseLong(this.file.getName());
        boolean ok = false;

        IOUtils.ensureDirOK(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    @Override
    public boolean renameTo(String fileName) {
        File newFile = new File(fileName);
        boolean rename = file.renameTo(newFile);
        if (!rename) {
            return false;
        }

        this.fileName = fileName;
        this.file = newFile;
        return true;
    }

    @Override
    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public boolean getData(int pos, int size, ByteBuffer byteBuffer) {
        if (byteBuffer.remaining() < size) {
            return false;
        }

        int readPosition = getReadPosition();
        if ((pos + size) > readPosition) {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size + ", fileFromOffset: " + this.offsetInFileName);
            return false;
        }

        if (!this.hold()) {
            log.debug("matched, but hold failed, request pos: " + pos + ", fileFromOffset: " + this.offsetInFileName);
            return false;
        }

        try {
            int readNum = fileChannel.read(byteBuffer, pos);
            return size == readNum;
        } catch (Throwable t) {
            log.warn("Get data failed pos:{} size:{} fileFromOffset:{}", pos, size, this.offsetInFileName);
            return false;
        } finally {
            this.release();
        }
    }

    @Override
    public int getFileSize() {
        return fileSize;
    }

    @Override
    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public AppendMessageResult appendMessage(final ByteBuffer byteBufferMsg, final CompactionAppendMsgCallback cb) {
        assert byteBufferMsg != null;
        assert cb != null;

        int currentPos = WROTE_POSITION_UPDATER.get(this);
        if (currentPos >= this.fileSize) {
            log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
            return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
        }

        ByteBuffer byteBuffer = appendMessageBuffer().slice();
        byteBuffer.position(currentPos);
        AppendMessageResult result = cb.doAppend(byteBuffer, this.offsetInFileName, this.fileSize - currentPos, byteBufferMsg);
        WROTE_POSITION_UPDATER.addAndGet(this, result.getWroteBytes());
        this.storeTimestamp = result.getStoreTimestamp();
        return result;
    }

    @Override
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb,
        PutMessageContext putMessageContext) {
        return appendMessagesInner(msg, cb, putMessageContext);
    }

    @Override
    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb,
        PutMessageContext putMessageContext) {
        return appendMessagesInner(messageExtBatch, cb, putMessageContext);
    }

    private AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb,
        PutMessageContext putMessageContext) {
        assert messageExt != null;
        assert cb != null;

        int currentPos = WROTE_POSITION_UPDATER.get(this);
        if (currentPos >= this.fileSize) {
            log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
            return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
        }

        ByteBuffer byteBuffer = appendMessageBuffer().slice();
        byteBuffer.position(currentPos);
        AppendMessageResult result;
        if (messageExt instanceof MessageExtBatch && !((MessageExtBatch) messageExt).isInnerBatch()) {
            // traditional batch message
            result = cb.doAppend(this.getOffsetInFileName(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt, putMessageContext);
        } else if (messageExt instanceof MessageExtBrokerInner) {
            // traditional single message or newly introduced inner-batch message
            result = cb.doAppend(this.getOffsetInFileName(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt, putMessageContext);
        } else {
            return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
        }
        WROTE_POSITION_UPDATER.addAndGet(this, result.getWroteBytes());
        this.storeTimestamp = result.getStoreTimestamp();
        return result;
    }

    protected ByteBuffer appendMessageBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return writeBuffer != null ? writeBuffer : this.mappedByteBuffer;
    }

    @Override
    public long getOffsetInFileName() {
        return this.offsetInFileName;
    }

    @Override
    public boolean appendMessage(final byte[] data) {
        return appendMessage(data, 0, data.length);
    }

    @Override
    public boolean appendMessage(ByteBuffer data) {
        int currentPos = WROTE_POSITION_UPDATER.get(this);
        int remaining = data.remaining();

        if ((currentPos + remaining) > this.fileSize) {
            return false;
        }

        try {
            this.fileChannel.position(currentPos);
            while (data.hasRemaining()) {
                this.fileChannel.write(data);
            }
        } catch (Throwable e) {
            log.error("Error occurred when append message to mappedFile.", e);
        }
        WROTE_POSITION_UPDATER.addAndGet(this, remaining);
        return true;
    }

    /**
     * Content of data from offset to offset + length will be written to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    @Override
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = WROTE_POSITION_UPDATER.get(this);
        if ((currentPos + length) > this.fileSize) {
            return false;
        }

        try {
            ByteBuffer buf = this.mappedByteBuffer.slice();
            buf.position(currentPos);
            buf.put(data, offset, length);
        } catch (Throwable e) {
            log.error("Error occurred when append message to mappedFile.", e);
        }
        WROTE_POSITION_UPDATER.addAndGet(this, length);
        return true;
    }

    /**
     * @return The current flushed position
     */
    @Override
    public int flush(final int flushLeastPages) {
        if (!this.isAbleToFlush(flushLeastPages)) {
            return this.getFlushedPosition();
        }
        if (!this.hold()) {
            log.warn("in flush, hold failed, flush offset = " + FLUSHED_POSITION_UPDATER.get(this));
            FLUSHED_POSITION_UPDATER.set(this, getReadPosition());

            return this.getFlushedPosition();
        }

        int value = getReadPosition();
        try {
            this.mappedByteBufferAccessCountSinceLastSwap++;

            //We only append data to fileChannel or mappedByteBuffer, never both.
            if (writeBuffer != null || this.fileChannel.position() != 0) {
                this.fileChannel.force(false);
            } else {
                this.mappedByteBuffer.force();
            }
            this.lastFlushTime = System.currentTimeMillis();
        } catch (Throwable e) {
            log.error("Error occurred when force data to disk.", e);
        }

        FLUSHED_POSITION_UPDATER.set(this, value);
        this.release();

        return this.getFlushedPosition();
    }

    @Override
    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return WROTE_POSITION_UPDATER.get(this);
        }

        //no need to commit data to file channel, so just set committedPosition to wrotePosition.
        if (transientStorePool != null && !transientStorePool.isRealCommit()) {
            COMMITTED_POSITION_UPDATER.set(this, WROTE_POSITION_UPDATER.get(this));
        } else if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0();
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + COMMITTED_POSITION_UPDATER.get(this));
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == COMMITTED_POSITION_UPDATER.get(this)) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return COMMITTED_POSITION_UPDATER.get(this);
    }

    protected void commit0() {
        int writePos = WROTE_POSITION_UPDATER.get(this);
        int lastCommittedPosition = COMMITTED_POSITION_UPDATER.get(this);

        if (writePos - lastCommittedPosition <= 0) {
            return;
        }

        try {
            ByteBuffer byteBuffer = writeBuffer.slice();
            byteBuffer.position(lastCommittedPosition);
            byteBuffer.limit(writePos);
            this.fileChannel.position(lastCommittedPosition);
            this.fileChannel.write(byteBuffer);
            COMMITTED_POSITION_UPDATER.set(this, writePos);
        } catch (Throwable e) {
            log.error("Error occurred when commit data to FileChannel.", e);
        }
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = FLUSHED_POSITION_UPDATER.get(this);
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    protected boolean isAbleToCommit(final int commitLeastPages) {
        int commit = COMMITTED_POSITION_UPDATER.get(this);
        int write = WROTE_POSITION_UPDATER.get(this);

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (commit / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > commit;
    }

    @Override
    public int getFlushedPosition() {
        return FLUSHED_POSITION_UPDATER.get(this);
    }

    @Override
    public void setFlushedPosition(int pos) {
        FLUSHED_POSITION_UPDATER.set(this, pos);
    }

    @Override
    public boolean isFull() {
        return this.fileSize == WROTE_POSITION_UPDATER.get(this);
    }

    @Override
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) > readPosition) {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.offsetInFileName);
            return null;
        }

        if (!this.hold()) {
            log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: " + this.offsetInFileName);
            return null;
        }

        this.mappedByteBufferAccessCountSinceLastSwap++;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(pos);
        ByteBuffer byteBufferNew = byteBuffer.slice();
        byteBufferNew.limit(size);
        return new SelectMappedBufferResult(this.offsetInFileName + pos, byteBufferNew, size, this);
    }

    @Override
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos >= readPosition || pos < 0) {
            return null;
        }
        if (!this.hold()) {
            return null;
        }

        this.mappedByteBufferAccessCountSinceLastSwap++;
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(pos);
        int size = readPosition - pos;
        ByteBuffer byteBufferNew = byteBuffer.slice();
        byteBufferNew.limit(size);
        return new SelectMappedBufferResult(this.offsetInFileName + pos, byteBufferNew, size, this);
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        IOUtils.cleanBuffer(this.mappedByteBuffer);
        IOUtils.cleanBuffer(this.mappedByteBufferWaitToClean);
        this.mappedByteBufferWaitToClean = null;
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    @Override
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);
        if (!this.isCleanupOver()) {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
            return false;
        }

        try {
            long lastModified = getLastModifiedTimestamp();
            this.fileChannel.close();
            log.info("close file channel " + this.fileName + " OK");

            long beginTime = System.currentTimeMillis();
            boolean result = this.file.delete();
            log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                + this.getFlushedPosition() + ", "
                + TimeUtils.computeElapsedTimeMilliseconds(beginTime)
                + "," + (System.currentTimeMillis() - lastModified));
        } catch (Exception e) {
            log.warn("close file channel " + this.fileName + " Failed. ", e);
        }

        return true;
    }

    @Override
    public int getWrotePosition() {
        return WROTE_POSITION_UPDATER.get(this);
    }

    @Override
    public void setWrotePosition(int pos) {
        WROTE_POSITION_UPDATER.set(this, pos);
    }

    /**
     * @return The max position which have valid data
     */
    @Override
    public int getReadPosition() {
        return transientStorePool == null || !transientStorePool.isRealCommit() ? WROTE_POSITION_UPDATER.get(this) : COMMITTED_POSITION_UPDATER.get(this);
    }

    @Override
    public void setCommittedPosition(int pos) {
        COMMITTED_POSITION_UPDATER.set(this, pos);
    }

    @Override
    public void warmMappedFile(FlushDiskType type, int pages) {
        this.mappedByteBufferAccessCountSinceLastSwap++;

        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        long flush = 0;
        // long time = System.currentTimeMillis();
        for (long i = 0, j = 0; i < this.fileSize; i += DefaultMappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put((int) i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            // if (j % 1000 == 0) {
            //     log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
            //     time = System.currentTimeMillis();
            //     try {
            //         Thread.sleep(0);
            //     } catch (InterruptedException e) {
            //         log.error("Interrupted", e);
            //     }
            // }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    @Override
    public boolean swapMap() {
        if (getRefCount() != 1 || this.mappedByteBufferWaitToClean != null) {
            log.info("Will not swap file: " + this.fileName + ", ref=" + getRefCount());
            return false;
        }

        if (!hold()) {
            log.warn("in swapMap, hold failed, fileName: " + this.fileName);
            return false;
        }
        try {
            this.mappedByteBufferWaitToClean = this.mappedByteBuffer;
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            this.mappedByteBufferAccessCountSinceLastSwap = 0L;
            this.swapMapTime = System.currentTimeMillis();
            log.info("swap file " + this.fileName + " success.");
            return true;
        } catch (Exception e) {
            log.error("swapMap file " + this.fileName + " Failed. ", e);
        } finally {
            this.release();
        }

        return false;
    }

    @Override
    public void cleanSwapedMap(boolean force) {
        try {
            if (this.mappedByteBufferWaitToClean == null) {
                return;
            }
            long minGapTime = 120 * 1000L;
            long gapTime = System.currentTimeMillis() - this.swapMapTime;
            if (!force && gapTime < minGapTime) {
                Thread.sleep(minGapTime - gapTime);
            }
            IOUtils.cleanBuffer(this.mappedByteBufferWaitToClean);
            mappedByteBufferWaitToClean = null;
            log.info("cleanSwapedMap file " + this.fileName + " success.");
        } catch (Exception e) {
            log.error("cleanSwapedMap file " + this.fileName + " Failed. ", e);
        }
    }

    @Override
    public long getRecentSwapMapTime() {
        return 0;
    }

    @Override
    public long getMappedByteBufferAccessCountSinceLastSwap() {
        return this.mappedByteBufferAccessCountSinceLastSwap;
    }

    @Override
    public long getLastFlushTime() {
        return this.lastFlushTime;
    }

    @Override
    public String getFileName() {
        return fileName;
    }

    @Override
    public MappedByteBuffer getMappedByteBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return mappedByteBuffer;
    }

    @Override
    public ByteBuffer sliceByteBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return this.mappedByteBuffer.slice();
    }

    @Override
    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    @Override
    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    @Override
    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    @Override
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        mlock(pointer, address, beginTime);
        madvise(pointer, address, beginTime);
    }

    private void mlock(Pointer pointer, long address, long beginTime) {
        int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
        log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    private void madvise(Pointer pointer, long address, long beginTime) {
        int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
        log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    @Override
    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    @Override
    public File getFile() {
        return this.file;
    }

    @Override
    public void renameToDelete() {
        if (fileName.endsWith(".delete")) {
            return;
        }

        //use Files.move
        String newFileName = this.fileName + ".delete";
        try {
            Path newFilePath = Paths.get(newFileName);
            if (org.apache.rocketmq.common.utils.SystemUtils.isWindowsPlatform() && mappedByteBuffer != null) {
                renameToDeleteInWindows(newFileName, newFilePath);
            } else {
                Files.move(Paths.get(fileName), newFilePath, StandardCopyOption.ATOMIC_MOVE);
            }

            this.fileName = newFileName;
            this.file = new File(newFileName);
        } catch (IOException e) {
            log.error("move file {} failed", fileName, e);
        }
    }

    private void renameToDeleteInWindows(String newFileName, Path newFilePath) throws IOException {
        // https://bugs.openjdk.org/browse/JDK-4724038
        // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
        // Windows can't move the file when mmap.
        long position = this.fileChannel.position();
        IOUtils.cleanBuffer(this.mappedByteBuffer);
        this.fileChannel.close();
        Files.move(Paths.get(fileName), newFilePath, StandardCopyOption.ATOMIC_MOVE);

        try (RandomAccessFile file = new RandomAccessFile(newFileName, "rw")) {
            this.fileChannel = file.getChannel();
            this.fileChannel.position(position);
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
        }
    }

    @Override
    public void moveToParent() throws IOException {
        Path currentPath = Paths.get(fileName);
        String baseName = currentPath.getFileName().toString();
        Path parentPath = currentPath.getParent().getParent().resolve(baseName);

        if (org.apache.rocketmq.common.utils.SystemUtils.isWindowsPlatform() && mappedByteBuffer != null) {
            moveToParentInWindows(parentPath);
        } else {
            Files.move(Paths.get(fileName), parentPath, StandardCopyOption.ATOMIC_MOVE);
        }
        this.file = parentPath.toFile();
        this.fileName = parentPath.toString();
    }

    private void moveToParentInWindows(Path parentPath) throws IOException {
        // https://bugs.openjdk.org/browse/JDK-4724038
        // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
        // Windows can't move the file when mmap.
        long position = this.fileChannel.position();
        IOUtils.cleanBuffer(this.mappedByteBuffer);
        this.fileChannel.close();
        Files.move(Paths.get(fileName), parentPath, StandardCopyOption.ATOMIC_MOVE);

        try (RandomAccessFile file = new RandomAccessFile(parentPath.toFile(), "rw")) {
            this.fileChannel = file.getChannel();
            this.fileChannel.position(position);
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
        }
    }

    @Override
    public String toString() {
        return this.fileName;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public long getStopTimestamp() {
        return stopTimestamp;
    }

    public void setStopTimestamp(long stopTimestamp) {
        this.stopTimestamp = stopTimestamp;
    }


    public Iterator<SelectMappedBufferResult> iterator(int startPos) {
        return new DefaultMappedFileIterator(this, startPos);
    }

    public static Unsafe getUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        } catch (Exception ignore) {

        }
        return null;
    }

    public static long mappingAddr(long addr) {
        long offset = addr % UNSAFE_PAGE_SIZE;
        offset = (offset >= 0) ? offset : (UNSAFE_PAGE_SIZE + offset);
        return addr - offset;
    }

    public static int pageCount(long size) {
        return (int) (size + (long) UNSAFE_PAGE_SIZE - 1L) / UNSAFE_PAGE_SIZE;
    }

    @Override
    public boolean isLoaded(long position, int size) {
        if (IS_LOADED_METHOD == null) {
            return true;
        }
        try {
            long addr = ((DirectBuffer) mappedByteBuffer).address() + position;
            return (boolean) IS_LOADED_METHOD.invoke(mappedByteBuffer, mappingAddr(addr), size, pageCount(size));
        } catch (Exception e) {
            log.info("invoke isLoaded0 of file {} error:", file.getAbsolutePath(), e);
        }
        return true;
    }

}
