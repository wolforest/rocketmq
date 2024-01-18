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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.Consumer;

class DefaultMappedFileIterator implements Iterator<SelectMappedBufferResult> {
    private final DefaultMappedFile mappedFile;
    private int start;
    private int current;
    private ByteBuffer buf;

    public DefaultMappedFileIterator(DefaultMappedFile mappedFile, int pos) {
        this.mappedFile = mappedFile;

        this.start = pos;
        this.current = pos;

        this.buf = mappedFile.mappedByteBuffer.slice();
        this.buf.position(start);
    }

    @Override
    public boolean hasNext() {
        return current < mappedFile.getReadPosition();
    }

    @Override
    public SelectMappedBufferResult next() {
        int readPosition = mappedFile.getReadPosition();
        if (current < readPosition && current >= 0) {
            if (mappedFile.hold()) {
                ByteBuffer byteBuffer = buf.slice();
                byteBuffer.position(current);
                int size = byteBuffer.getInt(current);
                ByteBuffer bufferResult = byteBuffer.slice();
                bufferResult.limit(size);
                current += size;
                return new SelectMappedBufferResult(mappedFile.getOffsetInFileName() + current, bufferResult, size,
                    mappedFile);
            }
        }
        return null;
    }

    @Override
    public void forEachRemaining(Consumer<? super SelectMappedBufferResult> action) {
        Iterator.super.forEachRemaining(action);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
