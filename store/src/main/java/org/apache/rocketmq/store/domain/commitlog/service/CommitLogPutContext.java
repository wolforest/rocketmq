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
package org.apache.rocketmq.store.domain.commitlog.service;

import org.apache.rocketmq.common.domain.message.MessageExtBatch;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.api.dto.AppendMessageResult;
import org.apache.rocketmq.store.domain.message.MessageExtEncoder;
import org.apache.rocketmq.store.infra.file.MappedFile;

public class CommitLogPutContext {
    private MessageExtBrokerInner msg;
    private MessageExtBatch messageExtBatch;
    private AppendMessageResult result;
    private MessageExtEncoder.PutMessageThreadLocal putMessageThreadLocal;
    private String topicQueueKey;
    private long elapsedTimeInLock;
    private MappedFile unlockMappedFile;
    private MappedFile mappedFile;
    private long currOffset;

    private int needAckNums;
    private boolean needHandleHA;

    public CommitLogPutContext() {
        this.result = null;
        this.elapsedTimeInLock = 0;
        this.unlockMappedFile = null;
    }

    public int getNeedAckNums() {
        return needAckNums;
    }

    public void setNeedAckNums(int needAckNums) {
        this.needAckNums = needAckNums;
    }

    public MessageExtBrokerInner getMsg() {
        return msg;
    }

    public void setMsg(MessageExtBrokerInner msg) {
        this.msg = msg;
    }

    public MessageExtBatch getMessageExtBatch() {
        return messageExtBatch;
    }

    public void setMessageExtBatch(MessageExtBatch messageExtBatch) {
        this.messageExtBatch = messageExtBatch;
    }

    public AppendMessageResult getResult() {
        return result;
    }

    public void setResult(AppendMessageResult result) {
        this.result = result;
    }

    public MessageExtEncoder.PutMessageThreadLocal getPutMessageThreadLocal() {
        return putMessageThreadLocal;
    }

    public void setPutMessageThreadLocal(MessageExtEncoder.PutMessageThreadLocal putMessageThreadLocal) {
        this.putMessageThreadLocal = putMessageThreadLocal;
    }

    public String getTopicQueueKey() {
        return topicQueueKey;
    }

    public void setTopicQueueKey(String topicQueueKey) {
        this.topicQueueKey = topicQueueKey;
    }

    public long getElapsedTimeInLock() {
        return elapsedTimeInLock;
    }

    public void setElapsedTimeInLock(long elapsedTimeInLock) {
        this.elapsedTimeInLock = elapsedTimeInLock;
    }

    public MappedFile getUnlockMappedFile() {
        return unlockMappedFile;
    }

    public void setUnlockMappedFile(MappedFile unlockMappedFile) {
        this.unlockMappedFile = unlockMappedFile;
    }

    public MappedFile getMappedFile() {
        return mappedFile;
    }

    public void setMappedFile(MappedFile mappedFile) {
        this.mappedFile = mappedFile;
    }

    public long getCurrOffset() {
        return currOffset;
    }

    public void setCurrOffset(long currOffset) {
        this.currOffset = currOffset;
    }

    public boolean isNeedHandleHA() {
        return needHandleHA;
    }

    public void setNeedHandleHA(boolean needHandleHA) {
        this.needHandleHA = needHandleHA;
    }




}
