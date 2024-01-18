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

import org.apache.rocketmq.store.infra.mappedfile.SelectMappedBufferResult;

import java.nio.ByteBuffer;

public class BatchConsumeQueueIterator implements ReferredIterator<CqUnit> {
    private SelectMappedBufferResult sbr;
    private int relativePos = 0;

    public BatchConsumeQueueIterator(SelectMappedBufferResult sbr) {
        this.sbr = sbr;
        if (sbr != null && sbr.getByteBuffer() != null) {
            relativePos = sbr.getByteBuffer().position();
        }
    }

    @Override
    public boolean hasNext() {
        if (sbr == null || sbr.getByteBuffer() == null) {
            return false;
        }

        return sbr.getByteBuffer().hasRemaining();
    }

    @Override
    public CqUnit next() {
        if (!hasNext()) {
            return null;
        }
        ByteBuffer tmpBuffer = sbr.getByteBuffer().slice();
        tmpBuffer.position(BatchConsumeQueue.MSG_COMPACT_OFFSET_INDEX);
        ByteBuffer compactOffsetStoreBuffer = tmpBuffer.slice();
        compactOffsetStoreBuffer.limit(BatchConsumeQueue.MSG_COMPACT_OFFSET_LENGTH);

        int relativePos = sbr.getByteBuffer().position();
        long offsetPy = sbr.getByteBuffer().getLong();
        int sizePy = sbr.getByteBuffer().getInt();
        long tagsCode = sbr.getByteBuffer().getLong(); //tagscode
        sbr.getByteBuffer().getLong();//timestamp
        long msgBaseOffset = sbr.getByteBuffer().getLong();
        short batchSize = sbr.getByteBuffer().getShort();
        int compactedOffset = sbr.getByteBuffer().getInt();
        sbr.getByteBuffer().position(relativePos + BatchConsumeQueue.CQ_STORE_UNIT_SIZE);

        return new CqUnit(msgBaseOffset, offsetPy, sizePy, tagsCode, batchSize, compactedOffset, compactOffsetStoreBuffer);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

    @Override
    public void release() {
        if (sbr != null) {
            sbr.release();
            sbr = null;
        }
    }

    @Override
    public CqUnit nextAndRelease() {
        try {
            return next();
        } finally {
            release();
        }
    }
}
