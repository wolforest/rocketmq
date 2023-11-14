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
package org.apache.rocketmq.store.timer;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.queue.ConsumeQueue;

import java.nio.ByteBuffer;

/**
 * raw message related service, this is the source and sink of timer package
 *
 * working flow:
 * -> load data from consume queue
 * -> time wheel/time level schedule
 * -> put due message back to commitLog
 *
 * class functionality:
 * 1. load data from consume queue <- getConsumeQueue()
 * 2. put due message back to commitLog -> putMessage()
 * 3. readMessageByCommitOffset ?
 */
public class MessageOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final ThreadLocal<ByteBuffer> bufferLocal;
    private final MessageStore messageStore;
    private final MessageStoreConfig storeConfig;

    /**
     * Just need commitLog and consumeQueue
     * @param messageStore No
     * @param storeConfig No
     */
    public MessageOperator(MessageStore messageStore, MessageStoreConfig storeConfig) {
        this.messageStore = messageStore;
        this.storeConfig = storeConfig;
        bufferLocal = new ThreadLocal<ByteBuffer>() {
            @Override
            protected ByteBuffer initialValue() {
                return ByteBuffer.allocateDirect(storeConfig.getMaxMessageSize() + 100);
            }
        };

    }

    public ConsumeQueue getConsumeQueue(String topic, int queueId) {
        return (ConsumeQueue) this.messageStore.getConsumeQueue(topic, queueId);
    }

    public PutMessageResult putMessage(MessageExtBrokerInner message) {
        return messageStore.putMessage(message);
    }

    public MessageExt readMessageByCommitOffset(long offsetPy, int sizePy) {
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

    public void clean() {
        IOUtils.cleanBuffer(this.bufferLocal.get());
        this.bufferLocal.remove();
    }

    public String getRealTopic(MessageExt msgExt) {
        if (msgExt == null) {
            return null;
        }
        return msgExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC);
    }
}
