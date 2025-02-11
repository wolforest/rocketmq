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
package org.apache.rocketmq.store.api.dto;

import org.apache.rocketmq.store.infra.mappedfile.SelectMappedBufferResult;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class QueryMessageResult {
    private final List<SelectMappedBufferResult> messageMappedList = new ArrayList<>(100);
    private final List<ByteBuffer> messageBufferList = new ArrayList<>(100);
    private long indexLastUpdateTimestamp;
    private long indexLastUpdateOffset;

    private int bufferTotalSize = 0;

    public void addMessage(final SelectMappedBufferResult mappedBuffer) {
        this.messageMappedList.add(mappedBuffer);
        this.messageBufferList.add(mappedBuffer.getByteBuffer());
        this.bufferTotalSize += mappedBuffer.getSize();
    }

    public void release() {
        for (SelectMappedBufferResult select : this.messageMappedList) {
            select.release();
        }
    }

    public long getIndexLastUpdateTimestamp() {
        return indexLastUpdateTimestamp;
    }

    public void setIndexLastUpdateTimestamp(long indexLastUpdateTimestamp) {
        this.indexLastUpdateTimestamp = indexLastUpdateTimestamp;
    }

    public long getIndexLastUpdateOffset() {
        return indexLastUpdateOffset;
    }

    public void setIndexLastUpdateOffset(long indexLastUpdateOffset) {
        this.indexLastUpdateOffset = indexLastUpdateOffset;
    }

    public List<ByteBuffer> getMessageBufferList() {
        return messageBufferList;
    }

    public int getBufferTotalSize() {
        return bufferTotalSize;
    }

    public List<SelectMappedBufferResult> getMessageMappedList() {
        return messageMappedList;
    }
}
