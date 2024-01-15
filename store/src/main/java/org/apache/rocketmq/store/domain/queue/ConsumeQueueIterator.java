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

import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.infra.file.SelectMappedBufferResult;

public class ConsumeQueueIterator implements ReferredIterator<CqUnit> {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final ConsumeQueue consumeQueue;
    private SelectMappedBufferResult sbr;
    private int relativePos = 0;

    public ConsumeQueueIterator(SelectMappedBufferResult sbr, ConsumeQueue consumeQueue) {
        this.sbr = sbr;
        if (sbr != null && sbr.getByteBuffer() != null) {
            relativePos = sbr.getByteBuffer().position();
        }
        this.consumeQueue = consumeQueue;
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
        long queueOffset = (sbr.getStartOffset() + sbr.getByteBuffer().position() - relativePos) / ConsumeQueue.CQ_STORE_UNIT_SIZE;
        CqUnit cqUnit = new CqUnit(queueOffset,
            sbr.getByteBuffer().getLong(),
            sbr.getByteBuffer().getInt(),
            sbr.getByteBuffer().getLong());

        if (consumeQueue.isExtAddr(cqUnit.getTagsCode())) {
            CqExtUnit cqExtUnit = new CqExtUnit();
            boolean extRet = consumeQueue.getExt(cqUnit.getTagsCode(), cqExtUnit);
            if (extRet) {
                cqUnit.setTagsCode(cqExtUnit.getTagsCode());
                cqUnit.setCqExtUnit(cqExtUnit);
            } else {
                // can't find ext content.Client will filter messages by tag also.
                log.error("[BUG] can't find consume queue extend file content! addr={}, offsetPy={}, sizePy={}, topic={}",
                    cqUnit.getTagsCode(), cqUnit.getPos(), cqUnit.getPos(), consumeQueue.getTopic());
            }
        }
        return cqUnit;
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
