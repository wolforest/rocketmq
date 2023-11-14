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
package org.apache.rocketmq.store.commitlog;

import java.nio.ByteBuffer;
import java.util.function.Supplier;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.BinaryUtils;
import org.apache.rocketmq.common.utils.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MultiDispatch;
import org.apache.rocketmq.store.PutMessageContext;


public class DefaultAppendMessageCallback implements AppendMessageCallback {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final DefaultMessageStore defaultMessageStore;
    private final CommitLog commitLog;

    // File at the end of the minimum fixed length empty
    private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
    // Store the message content
    private final ByteBuffer msgStoreItemMemory;
    private final int crc32ReservedLength = CommitLog.CRC32_RESERVED_LEN;
    private final boolean enabledAppendPropCRC;

    protected final MultiDispatch multiDispatch;

    public DefaultAppendMessageCallback(final DefaultMessageStore messageStore, CommitLog commitLog) {
        this.defaultMessageStore = messageStore;
        this.commitLog = commitLog;
        this.msgStoreItemMemory = ByteBuffer.allocate(END_FILE_MIN_BLANK_LENGTH);
        this.enabledAppendPropCRC = defaultMessageStore.getMessageStoreConfig().isEnabledAppendPropCRC();
        this.multiDispatch = new MultiDispatch(defaultMessageStore);
    }

    public AppendMessageResult handlePropertiesForLmqMsg(ByteBuffer preEncodeBuffer, final MessageExtBrokerInner msgInner) {
        if (msgInner.isEncodeCompleted()) {
            return null;
        }

        multiDispatch.wrapMultiDispatch(msgInner);

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        final byte[] propertiesData =
            msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

        boolean needAppendLastPropertySeparator = enabledAppendPropCRC && propertiesData != null && propertiesData.length > 0
            && propertiesData[propertiesData.length - 1] != MessageDecoder.PROPERTY_SEPARATOR;

        final int propertiesLength = (propertiesData == null ? 0 : propertiesData.length) + (needAppendLastPropertySeparator ? 1 : 0) + crc32ReservedLength;

        if (propertiesLength > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long. length={}", propertiesData.length);
            return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
        }

        int msgLenWithoutProperties = preEncodeBuffer.getInt(0);

        int msgLen = msgLenWithoutProperties + 2 + propertiesLength;

        // Exceeds the maximum message
        if (msgLen > this.defaultMessageStore.getMessageStoreConfig().getMaxMessageSize()) {
            log.warn("message size exceeded, msg total size: " + msgLen + ", maxMessageSize: " + this.defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
            return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
        }

        // Back filling total message length
        preEncodeBuffer.putInt(0, msgLen);
        // Modify position to msgLenWithoutProperties
        preEncodeBuffer.position(msgLenWithoutProperties);

        preEncodeBuffer.putShort((short) propertiesLength);

        if (propertiesLength > crc32ReservedLength) {
            preEncodeBuffer.put(propertiesData);
        }

        if (needAppendLastPropertySeparator) {
            preEncodeBuffer.put((byte) MessageDecoder.PROPERTY_SEPARATOR);
        }
        // 18 CRC32
        preEncodeBuffer.position(preEncodeBuffer.position() + crc32ReservedLength);

        msgInner.setEncodeCompleted(true);

        return null;
    }


    public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
        final MessageExtBrokerInner msgInner, PutMessageContext putMessageContext) {
        // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

        ByteBuffer preEncodeBuffer = msgInner.getEncodedBuff();
        boolean isMultiDispatchMsg = defaultMessageStore.getMessageStoreConfig().isEnableMultiDispatch() && CommitLog.isMultiDispatchMsg(msgInner);
        if (isMultiDispatchMsg) {
            AppendMessageResult appendMessageResult = handlePropertiesForLmqMsg(preEncodeBuffer, msgInner);
            if (appendMessageResult != null) {
                return appendMessageResult;
            }
        }

        final int msgLen = preEncodeBuffer.getInt(0);
        preEncodeBuffer.position(0);
        preEncodeBuffer.limit(msgLen);


        // PHY OFFSET
        long wroteOffset = fileFromOffset + byteBuffer.position();

        Supplier<String> msgIdSupplier = () -> {
            int sysflag = msgInner.getSysFlag();
            int msgIdLen = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 + 8 : 16 + 4 + 8;
            ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
            MessageExt.socketAddress2ByteBuffer(msgInner.getStoreHost(), msgIdBuffer);
            msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer
            msgIdBuffer.putLong(msgIdLen - 8, wroteOffset);
            return StringUtils.bytes2string(msgIdBuffer.array());
        };

        // Record ConsumeQueue information
        long queueOffset = msgInner.getQueueOffset();

        // this msg maybe a inner-batch msg.
        short messageNum = commitLog.getMessageNum(msgInner);

        // Transaction messages that require special handling
        final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
        switch (tranType) {
            // Prepared and Rollback message is not consumed, will not enter the consume queue
            case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
            case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                queueOffset = 0L;
                break;
            case MessageSysFlag.TRANSACTION_NOT_TYPE:
            case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
            default:
                break;
        }

        // Determines whether there is sufficient free space
        if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
            this.msgStoreItemMemory.clear();
            // 1 TOTALSIZE
            this.msgStoreItemMemory.putInt(maxBlank);
            // 2 MAGICCODE
            this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
            // 3 The remaining space may be any value
            // Here the length of the specially set maxBlank
            final long beginTimeMills = defaultMessageStore.now();
            byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
            return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset,
                maxBlank, /* only wrote 8 bytes, but declare wrote maxBlank for compute write position */
                msgIdSupplier, msgInner.getStoreTimestamp(),
                queueOffset, defaultMessageStore.now() - beginTimeMills);
        }

        int pos = 4 + 4 + 4 + 4 + 4;
        // 6 QUEUEOFFSET
        preEncodeBuffer.putLong(pos, queueOffset);
        pos += 8;
        // 7 PHYSICALOFFSET
        preEncodeBuffer.putLong(pos, fileFromOffset + byteBuffer.position());
        int ipLen = (msgInner.getSysFlag() & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
        // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
        pos += 8 + 4 + 8 + ipLen;
        // refresh store time stamp in lock
        preEncodeBuffer.putLong(pos, msgInner.getStoreTimestamp());
        if (enabledAppendPropCRC) {
            // 18 CRC32
            int checkSize = msgLen - crc32ReservedLength;
            ByteBuffer tmpBuffer = preEncodeBuffer.duplicate();
            tmpBuffer.limit(tmpBuffer.position() + checkSize);
            int crc32 = BinaryUtils.crc32(tmpBuffer);
            tmpBuffer.limit(tmpBuffer.position() + crc32ReservedLength);
            MessageDecoder.createCrc32(tmpBuffer, crc32);
        }

        final long beginTimeMills = defaultMessageStore.now();
        defaultMessageStore.getPerfCounter().startTick("WRITE_MEMORY_TIME_MS");
        // Write messages to the queue buffer
        byteBuffer.put(preEncodeBuffer);
        defaultMessageStore.getPerfCounter().endTick("WRITE_MEMORY_TIME_MS");
        msgInner.setEncodedBuff(null);

        if (isMultiDispatchMsg) {
            multiDispatch.updateMultiQueueOffset(msgInner);
        }

        return new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgIdSupplier,
            msgInner.getStoreTimestamp(), queueOffset, defaultMessageStore.now() - beginTimeMills, messageNum);
    }

    public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
        final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext) {
        byteBuffer.mark();
        //physical offset
        long wroteOffset = fileFromOffset + byteBuffer.position();
        // Record ConsumeQueue information
        long queueOffset = messageExtBatch.getQueueOffset();
        long beginQueueOffset = queueOffset;
        int totalMsgLen = 0;
        int msgNum = 0;

        final long beginTimeMills = defaultMessageStore.now();
        ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();

        int sysFlag = messageExtBatch.getSysFlag();
        int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
        int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
        Supplier<String> msgIdSupplier = () -> {
            int msgIdLen = storeHostLength + 8;
            int batchCount = putMessageContext.getBatchSize();
            long[] phyPosArray = putMessageContext.getPhyPos();
            ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
            MessageExt.socketAddress2ByteBuffer(messageExtBatch.getStoreHost(), msgIdBuffer);
            msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer

            StringBuilder buffer = new StringBuilder(batchCount * msgIdLen * 2 + batchCount - 1);
            for (int i = 0; i < phyPosArray.length; i++) {
                msgIdBuffer.putLong(msgIdLen - 8, phyPosArray[i]);
                String msgId = StringUtils.bytes2string(msgIdBuffer.array());
                if (i != 0) {
                    buffer.append(',');
                }
                buffer.append(msgId);
            }
            return buffer.toString();
        };

        messagesByteBuff.mark();
        int index = 0;
        while (messagesByteBuff.hasRemaining()) {
            // 1 TOTALSIZE
            final int msgPos = messagesByteBuff.position();
            final int msgLen = messagesByteBuff.getInt();

            totalMsgLen += msgLen;
            // Determines whether there is sufficient free space
            if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.msgStoreItemMemory.clear();
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                // 3 The remaining space may be any value
                //ignore previous read
                messagesByteBuff.reset();
                // Here the length of the specially set maxBlank
                byteBuffer.reset(); //ignore the previous appended messages
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdSupplier, messageExtBatch.getStoreTimestamp(),
                    beginQueueOffset, defaultMessageStore.now() - beginTimeMills);
            }
            //move to add queue offset and commitlog offset
            int pos = msgPos + 20;
            messagesByteBuff.putLong(pos, queueOffset);
            pos += 8;
            messagesByteBuff.putLong(pos, wroteOffset + totalMsgLen - msgLen);
            // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
            pos += 8 + 4 + 8 + bornHostLength;
            // refresh store time stamp in lock
            messagesByteBuff.putLong(pos, messageExtBatch.getStoreTimestamp());
            if (enabledAppendPropCRC) {
                //append crc32
                int checkSize = msgLen - crc32ReservedLength;
                ByteBuffer tmpBuffer = messagesByteBuff.duplicate();
                tmpBuffer.position(msgPos).limit(msgPos + checkSize);
                int crc32 = BinaryUtils.crc32(tmpBuffer);
                messagesByteBuff.position(msgPos + checkSize);
                MessageDecoder.createCrc32(messagesByteBuff, crc32);
            }

            putMessageContext.getPhyPos()[index++] = wroteOffset + totalMsgLen - msgLen;
            queueOffset++;
            msgNum++;
            messagesByteBuff.position(msgPos + msgLen);
        }

        messagesByteBuff.position(0);
        messagesByteBuff.limit(totalMsgLen);
        byteBuffer.put(messagesByteBuff);
        messageExtBatch.setEncodedBuff(null);
        AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdSupplier,
            messageExtBatch.getStoreTimestamp(), beginQueueOffset, defaultMessageStore.now() - beginTimeMills);
        result.setMsgNum(msgNum);

        return result;
    }

}
