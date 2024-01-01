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
package org.apache.rocketmq.broker.transaction.queue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageQueue;

public class CheckContext {
    private final MessageQueue messageQueue;
    private final long transactionTimeout;
    private final int transactionCheckMax;

    private final AbstractTransactionalMessageCheckListener listener;
    private final long startTime;

    private final List<Long> doneOpOffset = new ArrayList<>();
    private final HashMap<Long, Long> removeMap = new HashMap<>();
    private final HashMap<Long, HashSet<Long>> opMsgMap = new HashMap<>();

    private MessageQueue opQueue;
    private long halfOffset;
    private long opOffset;
    private PullResult pullResult;

    private int getMessageNullCount = 1;
    long newOffset;
    long counter;
    long nextOpOffset;
    private int putInQueueCount = 0;
    private int escapeFailCnt = 0;

    private MessageExt msgExt;

    public CheckContext(MessageQueue messageQueue, long transactionTimeout, int transactionCheckMax, AbstractTransactionalMessageCheckListener listener) {
        this.messageQueue = messageQueue;
        this.transactionTimeout = transactionTimeout;
        this.transactionCheckMax = transactionCheckMax;
        this.listener = listener;
        this.startTime = System.currentTimeMillis();
    }

    public void initOffset() {
        this.setNewOffset(this.getHalfOffset());
        this.setCounter(this.getHalfOffset());
        this.setNextOpOffset(this.getPullResult().getNextBeginOffset());
    }

    public void incCounter() {
        this.counter++;
    }

    public void incEscapeFailCnt() {
        this.escapeFailCnt++;
    }

    public void incPutInQueueCount() {
        this.putInQueueCount++;
    }

    public void incGetMessageNullCount() {
        this.getMessageNullCount++;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public long getTransactionTimeout() {
        return transactionTimeout;
    }

    public int getTransactionCheckMax() {
        return transactionCheckMax;
    }

    public AbstractTransactionalMessageCheckListener getListener() {
        return listener;
    }

    public long getStartTime() {
        return startTime;
    }

    public List<Long> getDoneOpOffset() {
        return doneOpOffset;
    }

    public HashMap<Long, Long> getRemoveMap() {
        return removeMap;
    }

    public HashMap<Long, HashSet<Long>> getOpMsgMap() {
        return opMsgMap;
    }

    public MessageQueue getOpQueue() {
        return opQueue;
    }

    public void setOpQueue(MessageQueue opQueue) {
        this.opQueue = opQueue;
    }

    public long getHalfOffset() {
        return halfOffset;
    }

    public void setHalfOffset(long halfOffset) {
        this.halfOffset = halfOffset;
    }

    public long getOpOffset() {
        return opOffset;
    }

    public void setOpOffset(long opOffset) {
        this.opOffset = opOffset;
    }

    public PullResult getPullResult() {
        return pullResult;
    }

    public void setPullResult(PullResult pullResult) {
        this.pullResult = pullResult;
    }

    public int getGetMessageNullCount() {
        return getMessageNullCount;
    }

    public long getNewOffset() {
        return newOffset;
    }

    public void setNewOffset(long newOffset) {
        this.newOffset = newOffset;
    }

    public long getCounter() {
        return counter;
    }

    public void setCounter(long counter) {
        this.counter = counter;
    }

    public long getNextOpOffset() {
        return nextOpOffset;
    }

    public void setNextOpOffset(long nextOpOffset) {
        this.nextOpOffset = nextOpOffset;
    }

    public int getPutInQueueCount() {
        return putInQueueCount;
    }

    public void setPutInQueueCount(int putInQueueCount) {
        this.putInQueueCount = putInQueueCount;
    }

    public int getEscapeFailCnt() {
        return escapeFailCnt;
    }

    public void setEscapeFailCnt(int escapeFailCnt) {
        this.escapeFailCnt = escapeFailCnt;
    }

    public MessageExt getMsgExt() {
        return msgExt;
    }

    public void setMsgExt(MessageExt msgExt) {
        this.msgExt = msgExt;
    }

}
