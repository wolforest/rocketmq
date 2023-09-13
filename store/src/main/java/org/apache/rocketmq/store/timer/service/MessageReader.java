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
package org.apache.rocketmq.store.timer.service;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.nio.ByteBuffer;

public class MessageReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final ThreadLocal<ByteBuffer> bufferLocal;
    private MessageStore messageStore;
    private MessageStoreConfig storeConfig;
    public MessageReader(MessageStore messageStore,MessageStoreConfig storeConfig){
        bufferLocal = new ThreadLocal<ByteBuffer>() {
            @Override
            protected ByteBuffer initialValue() {
                return ByteBuffer.allocateDirect(storeConfig.getMaxMessageSize() + 100);
            }
        };
    }
    public MessageExt getMessageByCommitOffset(long offsetPy, int sizePy) {
        for (int i = 0; i < 3; i++) {
            MessageExt msgExt = null;
            bufferLocal.get().position(0);
            bufferLocal.get().limit(sizePy);
            boolean res = messageStore.getData(offsetPy, sizePy, bufferLocal.get());
            if (res) {
                bufferLocal.get().flip();
                msgExt = MessageDecoder.decode(bufferLocal.get(), true, false, false);
            }
            if (null == msgExt) {
                LOGGER.warn("Fail to read msg from commitLog offsetPy:{} sizePy:{}", offsetPy, sizePy);
            } else {
                return msgExt;
            }
        }
        return null;
    }
}
