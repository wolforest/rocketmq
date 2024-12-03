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
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class TimerWheel {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    public static final int BLANK = -1, IGNORE = -2;

    /**
     * total slots number
     * slotsTotal = TIMER_WHEEL_TTL_DAY * DAY_SECS
     * default 7 days;
     */
    public final int slotsTotal;
    /**
     * precision, defined in MessageStoreConfig
     * default is 1000ms, AKA 1s.
     */
    public final int precisionMs;
    /**
     * wheelLength = totalSlotsNum * 2 * SlotSize
     * it is 2 times of totalSlotsNum:
     *  - half for byteBuffer
     *  - half for localBuffer
     */
    private final int wheelLength;

    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;

    private final ByteBuffer byteBuffer;
    private final ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return byteBuffer.duplicate();
        }
    };

    public TimerWheel(String fileName, int slotsTotal, int precisionMs) throws IOException {
        this.slotsTotal = slotsTotal;
        this.precisionMs = precisionMs;
        this.wheelLength = this.slotsTotal * 2 * Slot.SIZE;

        File file = new File(fileName);
        IOUtils.ensureDirOK(file.getParent());

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(fileName, "rw")) {
            if (file.exists() && randomAccessFile.length() != 0 && randomAccessFile.length() != wheelLength) {
                throw new RuntimeException(String.format("Timer wheel length:%d != expected:%s", randomAccessFile.length(), wheelLength));
            }

            randomAccessFile.setLength(wheelLength);
            fileChannel = randomAccessFile.getChannel();
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, wheelLength);
            assert wheelLength == mappedByteBuffer.remaining();
            this.byteBuffer = ByteBuffer.allocateDirect(wheelLength);
            this.byteBuffer.put(mappedByteBuffer);
        } catch (FileNotFoundException e) {
            log.error("create file channel " + fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + fileName + " Failed. ", e);
            throw e;
        }
    }

    public void shutdown() {
        shutdown(true);
    }

    public void shutdown(boolean flush) {
        if (flush)
            this.flush();

        // unmap mappedByteBuffer
        IOUtils.cleanBuffer(this.mappedByteBuffer);
        IOUtils.cleanBuffer(this.byteBuffer);

        try {
            this.fileChannel.close();
        } catch (IOException e) {
            log.error("Shutdown error in timer wheel", e);
        }
    }

    /**
     * call by TimerFlushService and self;
     *  - self called while shutdown
     *  - called by TimerFlushService every 1s (defined in config file)
     */
    public void flush() {
        ByteBuffer bf = localBuffer.get();
        bf.position(0);
        bf.limit(wheelLength);
        mappedByteBuffer.position(0);
        mappedByteBuffer.limit(wheelLength);

        for (int i = 0; i < wheelLength; i++) {
            if (bf.get(i) != mappedByteBuffer.get(i)) {
                mappedByteBuffer.put(i, bf.get(i));
            }
        }
        this.mappedByteBuffer.force();
    }

    /**
     *
     * @param timeMs delayedTime in ms
     * @return slot
     */
    public Slot getSlot(long timeMs) {
        Slot slot = getRawSlot(timeMs);
        if (slot.timeMs != timeMs / precisionMs * precisionMs) {
            return new Slot(-1, -1, -1);
        }
        return slot;
    }

    //testable
    public Slot getRawSlot(long timeMs) {
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        return new Slot(
            localBuffer.get().getLong() * precisionMs,
            localBuffer.get().getLong(),
            localBuffer.get().getLong(),
            localBuffer.get().getInt(),
            localBuffer.get().getInt()
        );
    }

    /**
     * calculate slot index by delayedTime;
     * delayedTime is less than 3 days by config
     * totalSlots * 2 is 14 days
     * so ... ?
     *
     * @param timeMs delayedTime
     * @return slot index
     */
    public int getSlotIndex(long timeMs) {
        return (int) (timeMs / precisionMs % (slotsTotal * 2));
    }

    /**
     * called by self.reviseSlot
     *
     * @param timeMs delayedTime
     * @param firstPos firstPos
     * @param lastPos lastPos
     */
    public void putSlot(long timeMs, long firstPos, long lastPos) {
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        // To be compatible with previous version.
        // The previous version's precision is fixed at 1000ms, and it stores timeMs / 1000 in slot.
        localBuffer.get().putLong(timeMs / precisionMs);
        localBuffer.get().putLong(firstPos);
        localBuffer.get().putLong(lastPos);
    }

    /**
     * called by TimerWheelPersistence.putTimerWheelSlot
     *
     * @param timeMs delayedTime
     * @param firstPos firstPos
     * @param lastPos lastPos
     * @param num num
     * @param magic magic
     */
    public void putSlot(long timeMs, long firstPos, long lastPos, int num, int magic) {
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        localBuffer.get().putLong(timeMs / precisionMs);
        localBuffer.get().putLong(firstPos);
        localBuffer.get().putLong(lastPos);
        localBuffer.get().putInt(num);
        localBuffer.get().putInt(magic);
    }

    /**
     * called by TimerMessageRecover.recoverAndRevise
     * 
     * @param timeMs delayedTime
     * @param firstPos firstPos
     * @param lastPos lastPos
     * @param force force
     */
    public void reviseSlot(long timeMs, long firstPos, long lastPos, boolean force) {
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);

        if (timeMs / precisionMs != localBuffer.get().getLong()) {
            if (force) {
                putSlot(timeMs, firstPos != IGNORE ? firstPos : lastPos, lastPos);
            }
            return;
        }

        if (IGNORE != firstPos) {
            localBuffer.get().putLong(firstPos);
        } else {
            localBuffer.get().getLong();
        }

        if (IGNORE != lastPos) {
            localBuffer.get().putLong(lastPos);
        }
    }

    //check the timerWheel to see if its stored offset > maxOffset in timerlog
    public long checkPhyPos(long timeStartMs, long maxOffset) {
        long minFirst = Long.MAX_VALUE;
        int firstSlotIndex = getSlotIndex(timeStartMs);

        for (int i = 0; i < slotsTotal * 2; i++) {
            int slotIndex = (firstSlotIndex + i) % (slotsTotal * 2);
            localBuffer.get().position(slotIndex * Slot.SIZE);

            if ((timeStartMs + (long) i * precisionMs) / precisionMs != localBuffer.get().getLong()) {
                continue;
            }

            minFirst = calculateMinFirst(minFirst, maxOffset);
        }

        return minFirst;
    }

    private long calculateMinFirst(long minFirst, long maxOffset) {
        long first = localBuffer.get().getLong();
        long last = localBuffer.get().getLong();
        if (last > maxOffset && first < minFirst) {
            return first;
        }

        return minFirst;
    }

    public long getNum(long timeMs) {
        return getSlot(timeMs).num;
    }

    public long getAllNum(long timeStartMs) {
        int allNum = 0;
        int firstSlotIndex = getSlotIndex(timeStartMs);
        for (int i = 0; i < slotsTotal * 2; i++) {
            int slotIndex = (firstSlotIndex + i) % (slotsTotal * 2);
            localBuffer.get().position(slotIndex * Slot.SIZE);

            if ((timeStartMs + (long) i * precisionMs) / precisionMs != localBuffer.get().getLong()) {
                continue;
            }

            localBuffer.get().getLong(); //first pos
            localBuffer.get().getLong(); //last pos
            allNum = allNum + localBuffer.get().getInt();
        }
        return allNum;
    }
}
