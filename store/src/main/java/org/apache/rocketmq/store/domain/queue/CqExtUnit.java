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

package org.apache.rocketmq.store.domain.queue;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Store unit.
 */
public class CqExtUnit {
    public static final short MIN_EXT_UNIT_SIZE
        = 2 * 1 // size, 32k max
        + 8 * 2 // msg time + tagCode
        + 2; // bitMapSize

    public static final int MAX_EXT_UNIT_SIZE = Short.MAX_VALUE;

    public CqExtUnit() {
    }

    public CqExtUnit(Long tagsCode, long msgStoreTime, byte[] filterBitMap) {
        this.tagsCode = tagsCode == null ? 0 : tagsCode;
        this.msgStoreTime = msgStoreTime;
        this.filterBitMap = filterBitMap;
        this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
        this.size = (short) (MIN_EXT_UNIT_SIZE + this.bitMapSize);
    }

    /**
     * unit size
     */
    private short size;
    /**
     * has code of tags
     */
    private long tagsCode;
    /**
     * the time to store into commit log of message
     */
    private long msgStoreTime;
    /**
     * size of bit map
     */
    private short bitMapSize;
    /**
     * filter bit map
     */
    private byte[] filterBitMap;

    /**
     * build unit from buffer from current position.
     */
    public boolean read(final ByteBuffer buffer) {
        if (buffer.position() + 2 > buffer.limit()) {
            return false;
        }

        this.size = buffer.getShort();

        if (this.size < 1) {
            return false;
        }

        this.tagsCode = buffer.getLong();
        this.msgStoreTime = buffer.getLong();
        this.bitMapSize = buffer.getShort();

        if (this.bitMapSize < 1) {
            return true;
        }

        if (this.filterBitMap == null || this.filterBitMap.length != this.bitMapSize) {
            this.filterBitMap = new byte[bitMapSize];
        }

        buffer.get(this.filterBitMap);
        return true;
    }

    /**
     * Only read first 2 byte to get unit size.
     * <p>
     * if size > 0, then skip buffer position with size.
     * </p>
     * <p>
     * if size <= 0, nothing to do.
     * </p>
     */
    public void readBySkip(final ByteBuffer buffer) {
        ByteBuffer temp = buffer.slice();

        short tempSize = temp.getShort();
        this.size = tempSize;

        if (tempSize > 0) {
            buffer.position(buffer.position() + this.size);
        }
    }

    /**
     * Transform unit data to byte array.
     * <p/>
     * <li>1. @{code container} can be null, it will be created if null.</li>
     * <li>2. if capacity of @{code container} is less than unit size, it will be created also.</li>
     * <li>3. Pls be sure that size of unit is not greater than {@link #MAX_EXT_UNIT_SIZE}</li>
     */
    public byte[] write(final ByteBuffer container) {
        this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
        this.size = (short) (MIN_EXT_UNIT_SIZE + this.bitMapSize);

        ByteBuffer temp = container;

        if (temp == null || temp.capacity() < this.size) {
            temp = ByteBuffer.allocate(this.size);
        }

        temp.flip();
        temp.limit(this.size);

        temp.putShort(this.size);
        temp.putLong(this.tagsCode);
        temp.putLong(this.msgStoreTime);
        temp.putShort(this.bitMapSize);
        if (this.bitMapSize > 0) {
            temp.put(this.filterBitMap);
        }

        return temp.array();
    }

    /**
     * Calculate unit size by current data.
     */
    public int calcUnitSize() {
        return MIN_EXT_UNIT_SIZE + (filterBitMap == null ? 0 : filterBitMap.length);
    }

    public long getTagsCode() {
        return tagsCode;
    }

    public void setTagsCode(final long tagsCode) {
        this.tagsCode = tagsCode;
    }

    public long getMsgStoreTime() {
        return msgStoreTime;
    }

    public void setMsgStoreTime(final long msgStoreTime) {
        this.msgStoreTime = msgStoreTime;
    }

    public byte[] getFilterBitMap() {
        if (this.bitMapSize < 1) {
            return null;
        }
        return filterBitMap;
    }

    public void setFilterBitMap(final byte[] filterBitMap) {
        this.filterBitMap = filterBitMap;
        // not safe transform, but size will be calculate by #calcUnitSize
        this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
    }

    public short getSize() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof CqExtUnit))
            return false;

        CqExtUnit cqExtUnit = (CqExtUnit) o;

        if (bitMapSize != cqExtUnit.bitMapSize)
            return false;
        if (msgStoreTime != cqExtUnit.msgStoreTime)
            return false;
        if (size != cqExtUnit.size)
            return false;
        if (tagsCode != cqExtUnit.tagsCode)
            return false;
        if (!Arrays.equals(filterBitMap, cqExtUnit.filterBitMap))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) size;
        result = 31 * result + (int) (tagsCode ^ (tagsCode >>> 32));
        result = 31 * result + (int) (msgStoreTime ^ (msgStoreTime >>> 32));
        result = 31 * result + (int) bitMapSize;
        result = 31 * result + (filterBitMap != null ? Arrays.hashCode(filterBitMap) : 0);
        return result;
    }

    @Override
    public String toString() {
        return "CqExtUnit{" +
            "size=" + size +
            ", tagsCode=" + tagsCode +
            ", msgStoreTime=" + msgStoreTime +
            ", bitMapSize=" + bitMapSize +
            ", filterBitMap=" + Arrays.toString(filterBitMap) +
            '}';
    }
}