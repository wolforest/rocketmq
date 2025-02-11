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
package org.apache.rocketmq.store.domain.timer.persistence.wheel;

import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.infra.mappedfile.MappedFile;
import org.apache.rocketmq.store.infra.mappedfile.MappedFileQueue;
import org.apache.rocketmq.store.infra.mappedfile.SelectMappedBufferResult;

import java.nio.ByteBuffer;

/**
 * TimerLog stores TimerMessage's delayInfo and offsetInfo
 * TimerLog has 3 operations:
 *   - append timer message
 *   - fetch timer message by offset
 *   - calculate offset for cleaning expired files
 */
public class TimerLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    public final static int BLANK_MAGIC_CODE = 0xBBCCDDEE ^ 1880681586 + 8;
    private final static int MIN_BLANK_LEN = 4 + 8 + 4;
    public final static int UNIT_SIZE = Block.SIZE;

    /**
     * size for:
     *  unitSize + prePos + magic
     *  + currWriteTime + delayedTime
     */
    public final static int UNIT_PRE_SIZE_FOR_MSG = 28;
    public final static int UNIT_PRE_SIZE_FOR_METRIC = 40;
    private final MappedFileQueue mappedFileQueue;

    private final int fileSize;

    public TimerLog(final String storePath, final int fileSize) {
        this.fileSize = fileSize;
        this.mappedFileQueue = new MappedFileQueue(storePath, fileSize, null);
    }

    public boolean load() {
        return this.mappedFileQueue.load();
    }

    /**
     * append by block unit object
     * the only one public append API
     *
     * called by TimerWheelPersistence,
     *      and pos is always 0;
     *      and len is always Block.SIZE;
     *
     * @param block block object
     * @param pos   position or offset
     * @param len   len of block,current is fixed length Block.SIZE
     * @return offset
     */
    public long append(Block block, int pos, int len) {
        return append(block.bytes(), pos, len);
    }

    /**
     * just for test, no public usage.
     *
     * @param data data
     * @return offset
     */
    public long append(byte[] data) {
        return append(data, 0, data.length);
    }

    private long append(byte[] data, int pos, int len) {
        MappedFile mappedFile = chooseLastMappedFile(len);
        assert mappedFile != null;

        long currPosition = mappedFile.getOffsetInFileName() + mappedFile.getWrotePosition();
        if (!mappedFile.appendMessage(data, pos, len)) {
            LOGGER.error("Append error for timer log");
            return -1;
        }
        return currPosition;
    }

    /**
     * fetch TimerMessage by offset
     *
     * @param offsetPy offset
     * @return TimerMessage related SelectMappedBufferResult
     */
    public SelectMappedBufferResult getTimerMessage(long offsetPy) {
        MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(offsetPy);
        if (null == mappedFile)
            return null;
        return mappedFile.selectMappedBuffer((int) (offsetPy % mappedFile.getFileSize()));
    }

    /**
     * fetch the whole TimerRequest result
     *
     * @param offsetPy offset
     * @return whole timer message in the related MappedFile
     */
    public SelectMappedBufferResult getWholeBuffer(long offsetPy) {
        MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(offsetPy);
        if (null == mappedFile)
            return null;
        return mappedFile.selectMappedBuffer(0);
    }

    public MappedFileQueue getMappedFileQueue() {
        return mappedFileQueue;
    }

    public void shutdown() {
        this.mappedFileQueue.flush(0);
        //it seems not need to call shutdown
    }

    /**
     * calculate offset for last unit
     * be careful.
     * if the format of timerLog changed, this offset has to be changed too
     * so dose the batch writing
     * @return offset
     */
    public int getOffsetForLastUnit() {
        return fileSize - (fileSize - MIN_BLANK_LEN) % UNIT_SIZE - MIN_BLANK_LEN - UNIT_SIZE;
    }

    private MappedFile chooseLastMappedFile(int len) {
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (null == mappedFile || mappedFile.isFull()) {
            mappedFile = this.mappedFileQueue.getLastMappedFile(0);
        }

        if (null == mappedFile) {
            LOGGER.error("Create mapped file1 error for timer log");
            return null;
        }

        if (len + MIN_BLANK_LEN <= mappedFile.getFileSize() - mappedFile.getWrotePosition()) {
            return mappedFile;
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(MIN_BLANK_LEN);
        byteBuffer.putInt(mappedFile.getFileSize() - mappedFile.getWrotePosition());
        byteBuffer.putLong(0);
        byteBuffer.putInt(BLANK_MAGIC_CODE);

        if (mappedFile.appendMessage(byteBuffer.array())) {
            //need to set the wrote position
            mappedFile.setWrotePosition(mappedFile.getFileSize());
        } else {
            LOGGER.error("Append blank error for timer log");
            return null;
        }

        mappedFile = this.mappedFileQueue.getLastMappedFile(0);
        if (null == mappedFile) {
            LOGGER.error("create mapped file2 error for timer log");
            return null;
        }

        return mappedFile;
    }


}
