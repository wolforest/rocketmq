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
import org.apache.rocketmq.common.domain.message.MessageExtBatch;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.api.dto.AppendMessageResult;
import org.apache.rocketmq.store.domain.message.PutMessageContext;

/**
 * Write messages callback interface
 */
public interface AppendMessageCallback {

    /**
     * After message serialization, write MappedByteBuffer
     *
     * @param offsetInFileName logical offset of MappedFileQueue stored in mapped file name
     * @param byteBuffer msg related byteBuffer, sliced from MappedFile.appendMessageBuffer()
     * @param maxBlank fileSize - currentWritePos
     * @param putMessageContext context with queue key(topic-queueId), batchSize, positionArray
     * @return How many bytes to write
     */
    AppendMessageResult doAppend(final long offsetInFileName, final ByteBuffer byteBuffer,
        final int maxBlank, final MessageExtBrokerInner msg, PutMessageContext putMessageContext);

    /**
     * After batched message serialization, write MappedByteBuffer
     *
     * @param offsetInFileName logical offset of MappedFileQueue stored in mapped file name
     * @param byteBuffer batch related byteBuffer, sliced from MappedFile.appendMessageBuffer()
     * @param maxBlank fileSize - currentWritePos
     * @param messageExtBatch, backed up by a byte array
     * @param putMessageContext context with queue key(topic-queueId), batchSize, positionArray
     * @return How many bytes to write
     */
    AppendMessageResult doAppend(final long offsetInFileName, final ByteBuffer byteBuffer,
        final int maxBlank, final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext);
}
