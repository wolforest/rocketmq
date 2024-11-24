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
package org.apache.rocketmq.broker.server.daemon.pop;

import com.alibaba.fastjson.JSON;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.metrics.PopMetricsManager;
import org.apache.rocketmq.broker.domain.queue.offset.ConsumerOffsetManager;
import org.apache.rocketmq.common.domain.topic.KeyBuilder;
import org.apache.rocketmq.common.domain.constant.PopConstants;
import org.apache.rocketmq.common.lang.thread.ServiceThread;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;
import org.apache.rocketmq.store.server.config.BrokerRole;
import org.apache.rocketmq.store.api.broker.pop.AckMsg;
import org.apache.rocketmq.store.api.broker.pop.BatchAckMsg;
import org.apache.rocketmq.store.api.broker.pop.PopCheckPoint;
import org.apache.rocketmq.store.api.broker.pop.PopCheckPointWrapper;
import org.apache.rocketmq.store.api.broker.pop.PopKeyBuilder;
import org.apache.rocketmq.store.api.broker.pop.QueueWithTime;

/**
 * manage checkPoint and ack info of pop message
 * @renamed from PopBufferMergeService to PopAckService
 *
 * with default config:
 * - this class is just the proxy of revive queue operations
 * - the check point operations are useless
 */
public class PopBufferMergeThread extends ServiceThread {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    /**
     *
     * @renamed from buffer to checkPointMap
     *
     * use cases:
     * - scan: iterate buffer
     * - addAckMsg: get check point from buffer and mark ack state of Check Point
     *
     * Key: topic + group + queueId + startOffset + popTime + brokerName
     */
    ConcurrentHashMap<String/*mergeKey*/, PopCheckPointWrapper> buffer = new ConcurrentHashMap<>(1024 * 16);

    /**
     * manager check point for given consumer and given queue
     * @renamed from commitOffsets checkPointQueueMap
     *
     * use cases:
     * - getLatestOffset: get consumer next start offset of given queue
     * - scanGarbage
     * - getOffsetTotalSize: get total popping num
     * - isQueueFull
     *
     * Key: topic@cid@queueId
     * Value: check point queue of specific consumer and queue
     */
    ConcurrentHashMap<String/*topic@cid@queueId*/, QueueWithTime<PopCheckPointWrapper>> checkPointQueueMap = new ConcurrentHashMap<>();
    private volatile boolean serving = true;
    private final AtomicInteger counter = new AtomicInteger(0);
    private int scanTimes = 0;
    private final Broker broker;
    private final String reviveTopic;

    private final long interval = 5;
    private final int countOfSecond1 = (int) (1000 / interval);

    /**
     * this is a temporary Byte List, can be replaced by local var,
     * defined as instance property to avoid create a lot of local vars.
     */
    private final List<Byte> batchAckIndexList = new ArrayList<>(32);

    /**
     * whether in the master flag
     */
    private volatile boolean master = false;

    public PopBufferMergeThread(Broker broker) {
        this.broker = broker;
        this.reviveTopic = KeyBuilder.buildClusterReviveTopic(this.broker.getBrokerConfig().getBrokerClusterName());
    }

    @Override
    public String getServiceName() {
        if (this.broker != null && this.broker.getBrokerConfig().isInBrokerContainer()) {
            return broker.getBrokerIdentity().getIdentifier() + PopBufferMergeThread.class.getSimpleName();
        }
        return PopBufferMergeThread.class.getSimpleName();
    }

    /**
     * with default config, this method is useless, will do nothing but empty loop
     */
    @Override
    public void run() {
        // scan
        while (!this.isStopped()) {
            try {
                if (shouldSkip()) {
                    clearBuffer();
                    continue;
                }

                scan();
                int countOfSecond30 = (int) (30 * 1000 / interval);
                if (scanTimes % countOfSecond30 == 0) {
                    scanGarbage();
                }

                this.waitForRunning(interval);

                if (!this.serving && this.buffer.size() == 0 && getOffsetTotalSize() == 0) {
                    this.serving = true;
                }
            } catch (Throwable e) {
                POP_LOGGER.error("PopBufferMergeService error", e);
                this.waitForRunning(3000);
            }
        }

        this.serving = false;
        ThreadUtils.sleep(2000);
        if (shouldSkip()) {
            return;
        }

        while (this.buffer.size() > 0 || getOffsetTotalSize() > 0) {
            scan();
        }
    }

    /**
     *
     * @param lockKey topic@group@queueId
     * @return offset
     */
    public long getLatestOffset(String lockKey) {
        QueueWithTime<PopCheckPointWrapper> queue = this.checkPointQueueMap.get(lockKey);
        if (queue == null) {
            return -1;
        }
        PopCheckPointWrapper pointWrapper = queue.get().peekLast();
        if (pointWrapper != null) {
            return pointWrapper.getNextBeginOffset();
        }
        return -1;
    }

    public long getLatestOffset(String topic, String group, int queueId) {
        return getLatestOffset(KeyBuilder.buildConsumeKey(topic, group, queueId));
    }

    /**
     * put to store && add to buffer.
     * @renamed from addCkJustOffset to storeCheckPoint
     *
     * @param point check point
     * @param reviveQueueId revive queue id
     * @param reviveQueueOffset revive queue offset
     * @param nextBeginOffset next begin offset
     * @return boolean
     */
    public boolean storeCheckPoint(PopCheckPoint point, int reviveQueueId, long reviveQueueOffset, long nextBeginOffset) {
        PopCheckPointWrapper pointWrapper = new PopCheckPointWrapper(reviveQueueId, reviveQueueOffset, point, nextBeginOffset, true);

        if (this.buffer.containsKey(pointWrapper.getMergeKey())) {
            // when mergeKey conflict
            // will cause PopBufferMergeService.scanCommitOffset cannot poll PopCheckPointWrapper
            POP_LOGGER.warn("[PopBuffer]mergeKey conflict when add ckJustOffset. ck:{}, mergeKey:{}", pointWrapper, pointWrapper.getMergeKey());
            return false;
        }

        this.enqueueReviveQueue(pointWrapper, isQueueFull(pointWrapper));

        // put pointWrapper to commitOffsets and buffer
        // with default config, the following two operations are useless,
        // and they are better to be deleted
        putCheckPointQueue(pointWrapper);
        this.buffer.put(pointWrapper.getMergeKey(), pointWrapper);


        this.counter.incrementAndGet();
        if (broker.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]add ck just offset, {}", pointWrapper);
        }
        return  true;
    }

    /**
     * @renamed from addCkMock to mockCheckPoint
     *
     * add check point when pop message is:
     * - NO_MATCHED_MESSAGE
     * - OFFSET_FOUND_NULL
     * - MESSAGE_WAS_REMOVING
     * - NO_MATCHED_LOGIC_QUEUE
     */
    public void mockCheckPoint(String group, String topic, int queueId, long startOffset, long invisibleTime,
        long popTime, int reviveQueueId, long nextBeginOffset, String brokerName) {
        final PopCheckPoint ck = new PopCheckPoint();
        ck.setBitMap(0);
        ck.setNum((byte) 0);
        ck.setPopTime(popTime);
        ck.setInvisibleTime(invisibleTime);
        ck.setStartOffset(startOffset);
        ck.setCId(group);
        ck.setTopic(topic);
        ck.setQueueId(queueId);
        ck.setBrokerName(brokerName);

        PopCheckPointWrapper pointWrapper = new PopCheckPointWrapper(reviveQueueId, Long.MAX_VALUE, ck, nextBeginOffset, true);
        pointWrapper.setCkStored(true);

        putCheckPointQueue(pointWrapper);
        if (broker.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]add ck just offset, mocked, {}", pointWrapper);
        }
    }

    /**
     * with default config, this method is useless, always return false
     * @renamed from addCk to addCheckPoint
     * @renamed from addCheckPoint to cacheCheckPoint
     *
     * add pop checkPoint to buffer(memory), after stored in memory:
     * 1. checkPoints will be stored periodically
     *    when this.run() method is executing
     * 2. while method run executing
     *    checkPoints will be stored after ackMsg
     *    to make sure every message consumed by consumer
     *
     * @param point PopCheckPoint
     * @param reviveQueueId reviveQueueId
     * @param reviveQueueOffset reviveQueueOffset
     * @param nextBeginOffset nextBeginOffset
     * @return boolean add status
     */
    public boolean cacheCheckPoint(PopCheckPoint point, int reviveQueueId, long reviveQueueOffset, long nextBeginOffset) {
        // key: point.getT() + point.getC() + point.getQ() + point.getSo() + point.getPt()
        if (!broker.getBrokerConfig().isEnablePopBufferMerge()) {
            return false;
        }
        if (!serving) {
            return false;
        }

        long now = System.currentTimeMillis();
        if (point.getReviveTime() - now < broker.getBrokerConfig().getPopCkStayBufferTimeOut() + 1500) {
            if (broker.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.warn("[PopBuffer]add ck, timeout, {}, {}", point, now);
            }
            return false;
        }

        if (this.counter.get() > broker.getBrokerConfig().getPopCkMaxBufferSize()) {
            POP_LOGGER.warn("[PopBuffer]add ck, max size, {}, {}", point, this.counter.get());
            return false;
        }

        PopCheckPointWrapper pointWrapper = new PopCheckPointWrapper(reviveQueueId, reviveQueueOffset, point, nextBeginOffset);
        // check whether queue size exceed max size
        if (isQueueFull(pointWrapper)) {
            return false;
        }

        if (this.buffer.containsKey(pointWrapper.getMergeKey())) {
            // when mergeKey conflict
            // will cause PopBufferMergeService.scanCommitOffset cannot poll PopCheckPointWrapper
            POP_LOGGER.warn("[PopBuffer]mergeKey conflict when add ck. ck:{}, mergeKey:{}", pointWrapper, pointWrapper.getMergeKey());
            return false;
        }

        putCheckPointQueue(pointWrapper);
        this.buffer.put(pointWrapper.getMergeKey(), pointWrapper);
        this.counter.incrementAndGet();
        if (broker.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]add ck, {}", pointWrapper);
        }
        return true;
    }

    /**
     * add ackMsg to buffer, ackMsgs will be stored in buffer(memory) and persist periodically
     * This method will do nothing, in default setting
     *
     * @renamed from addAk to addAckMsg
     *
     * @param reviveQid reviveQid
     * @param ackMsg AckMsg
     * @return adding status
     */
    public boolean addAckMsg(int reviveQid, AckMsg ackMsg) {
        if (!broker.getBrokerConfig().isEnablePopBufferMerge()) {
            return false;
        }

        if (!serving) {
            return false;
        }

        try {
            PopCheckPointWrapper pointWrapper = this.buffer.get(PopKeyBuilder.buildKey(ackMsg));
            if (pointWrapper == null) {
                if (broker.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.warn("[PopBuffer]add ack fail, rqId={}, no ck, {}", reviveQid, ackMsg);
                }
                return false;
            }

            if (pointWrapper.isJustOffset()) {
                return false;
            }

            PopCheckPoint point = pointWrapper.getCk();
            long now = System.currentTimeMillis();

            if (point.getReviveTime() - now < broker.getBrokerConfig().getPopCkStayBufferTimeOut() + 1500) {
                if (broker.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.warn("[PopBuffer]add ack fail, rqId={}, almost timeout for revive, {}, {}, {}", reviveQid, pointWrapper, ackMsg, now);
                }
                return false;
            }

            if (now - point.getPopTime() > broker.getBrokerConfig().getPopCkStayBufferTime() - 1500) {
                if (broker.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.warn("[PopBuffer]add ack fail, rqId={}, stay too long, {}, {}, {}", reviveQid, pointWrapper, ackMsg, now);
                }
                return false;
            }

            if (ackMsg instanceof BatchAckMsg) {
                for (Long ackOffset : ((BatchAckMsg) ackMsg).getAckOffsetList()) {
                    int indexOfAck = point.indexOfAck(ackOffset);
                    if (indexOfAck > -1) {
                        markBitCAS(pointWrapper.getBits(), indexOfAck);
                    } else {
                        POP_LOGGER.error("[PopBuffer]Invalid index of ack, reviveQid={}, {}, {}", reviveQid, ackMsg, point);
                    }
                }
            } else {
                int indexOfAck = point.indexOfAck(ackMsg.getAckOffset());
                if (indexOfAck > -1) {
                    markBitCAS(pointWrapper.getBits(), indexOfAck);
                } else {
                    POP_LOGGER.error("[PopBuffer]Invalid index of ack, reviveQid={}, {}, {}", reviveQid, ackMsg, point);
                    return true;
                }
            }

            if (broker.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("[PopBuffer]add ack, rqId={}, {}, {}", reviveQid, pointWrapper, ackMsg);
            }

//            // check ak done
//            if (isCkDone(pointWrapper)) {
//                // cancel ck for timer
//                cancelCkTimer(pointWrapper);
//            }
            return true;
        } catch (Throwable e) {
            POP_LOGGER.error("[PopBuffer]add ack error, rqId=" + reviveQid + ", " + ackMsg, e);
        }

        return false;
    }

    public void clearOffsetQueue(String lockKey) {
        this.checkPointQueueMap.remove(lockKey);
    }

    /**
     * @renamed from isShouldRunning to shouldRun
     *
     * service will run in follow cases:
     * - is Master
     * - is Slave and with enableSlaveActingMaster = true
     *
     * @return service running flag:
     */
    private boolean shouldSkip() {
        if (this.broker.getBrokerConfig().isEnableSlaveActingMaster()) {
            return false;
        }

        this.master = broker.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE;
        return !this.master;
    }

    private void clearBuffer() {
        // slave
        this.waitForRunning(interval * 200 * 5);
        POP_LOGGER.info("Broker is {}, {}, clear all data",
            broker.getMessageStoreConfig().getBrokerRole(), this.master);
        this.buffer.clear();
        this.checkPointQueueMap.clear();
    }

    /**
     *
     * @renamed from scanCommitOffset CheckPointQueueMap
     */
    private int scanCheckPointQueueMap() {
        Iterator<Map.Entry<String, QueueWithTime<PopCheckPointWrapper>>> iterator = this.checkPointQueueMap.entrySet().iterator();
        int count = 0;
        while (iterator.hasNext()) {
            Map.Entry<String, QueueWithTime<PopCheckPointWrapper>> entry = iterator.next();
            LinkedBlockingDeque<PopCheckPointWrapper> queue = entry.getValue().get();

            scanCheckPointQueueMap(queue);

            final int qs = queue.size();
            count += qs;
            if (qs > 5000 && scanTimes % countOfSecond1 == 0) {
                POP_LOGGER.info("[PopBuffer] offset queue size too long, {}, {}", entry.getKey(), qs);
            }
        }
        return count;
    }

    private void scanCheckPointQueueMap(LinkedBlockingDeque<PopCheckPointWrapper> queue) {
        PopCheckPointWrapper pointWrapper;
        while ((pointWrapper = queue.peek()) != null) {
            if (isPointValid(pointWrapper)) {
                if (!commitOffset(pointWrapper)) {
                    break;
                }

                queue.poll();
                continue;
            }

            long popCkStayBufferTime = broker.getBrokerConfig().getPopCkStayBufferTime();
            long tsFromPop = System.currentTimeMillis() - pointWrapper.getCk().getPopTime();
            if (tsFromPop > popCkStayBufferTime * 2L) {
                POP_LOGGER.warn("[PopBuffer] ck offset long time not commit, {}", pointWrapper);
            }
            break;
        }
    }

    private void scanGarbage() {
        Iterator<Map.Entry<String, QueueWithTime<PopCheckPointWrapper>>> iterator = checkPointQueueMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, QueueWithTime<PopCheckPointWrapper>> entry = iterator.next();
            if (entry.getKey() == null) {
                continue;
            }
            String[] keyArray = entry.getKey().split(PopConstants.SPLIT);
            if (keyArray.length != 3) {
                continue;
            }

            String topic = keyArray[0];
            String cid = keyArray[1];
            if (broker.getTopicConfigManager().selectTopicConfig(topic) == null) {
                POP_LOGGER.info("[PopBuffer]remove not exit topic {} in buffer!", topic);
                iterator.remove();
                continue;
            }
            if (!broker.getSubscriptionGroupManager().getSubscriptionGroupTable().containsKey(cid)) {
                POP_LOGGER.info("[PopBuffer]remove not exit sub {} of topic {} in buffer!", cid, topic);
                iterator.remove();
                continue;
            }
            long minute5 = 5 * 60 * 1000;
            if (System.currentTimeMillis() - entry.getValue().getTime() > minute5) {
                POP_LOGGER.info("[PopBuffer]remove long time not used sub {} of topic {} in buffer!", cid, topic);
                iterator.remove();
            }
        }
    }

    // with default config, this method will do nothing
    private void scan() {
        long startTime = System.currentTimeMillis();
        AtomicInteger count = new AtomicInteger(0);
        int countCk = 0;
        Iterator<Map.Entry<String, PopCheckPointWrapper>> iterator = buffer.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, PopCheckPointWrapper> entry = iterator.next();
            PopCheckPointWrapper pointWrapper = entry.getValue();

            // with default config, validatePointWrapper will always return false
            // and the while process will always stop here
            if (!validatePointWrapper(iterator, pointWrapper)) {
                continue;
            }

            boolean removeFlag = getRemoveFlag(pointWrapper);

            // double check
            if (isCkDone(pointWrapper)) {
                continue;
            }

            // with default config, justOffset is always true
            if (pointWrapper.isJustOffset()) {
                // just offset should be in store.
                if (pointWrapper.getReviveQueueOffset() < 0) {
<<<<<<< HEAD:broker/src/main/java/org/apache/rocketmq/broker/server/daemon/pop/PopBufferMergeThread.java
                    enqueueReviveQueue(pointWrapper, false);
                    countCk++;
                }
                continue;
=======
                    putCkToStore(pointWrapper, this.brokerController.getBrokerConfig().isAppendCkAsync());
                    countCk++;
                }
                continue;
            } else if (removeCk) {
                // put buffer ak to store
                if (pointWrapper.getReviveQueueOffset() < 0) {
                    putCkToStore(pointWrapper, this.brokerController.getBrokerConfig().isAppendCkAsync());
                    countCk++;
                }

                if (!pointWrapper.isCkStored()) {
                    continue;
                }

                if (brokerController.getBrokerConfig().isEnablePopBatchAck()) {
                    List<Byte> indexList = this.batchAckIndexList;
                    try {
                        for (byte i = 0; i < point.getNum(); i++) {
                            // reput buffer ak to store
                            if (DataConverter.getBit(pointWrapper.getBits().get(), i)
                                && !DataConverter.getBit(pointWrapper.getToStoreBits().get(), i)) {
                                indexList.add(i);
                            }
                        }
                        if (indexList.size() > 0) {
                            putBatchAckToStore(pointWrapper, indexList, count);
                        }
                    } finally {
                        indexList.clear();
                    }
                } else {
                    for (byte i = 0; i < point.getNum(); i++) {
                        // reput buffer ak to store
                        if (DataConverter.getBit(pointWrapper.getBits().get(), i)
                            && !DataConverter.getBit(pointWrapper.getToStoreBits().get(), i)) {
                            putAckToStore(pointWrapper, i, count);
                        }
                    }
                }

                if (isCkDoneForFinish(pointWrapper) && pointWrapper.isCkStored()) {
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("[PopBuffer]ck finish, {}", pointWrapper);
                    }
                    iterator.remove();
                    counter.decrementAndGet();
                }
>>>>>>> develop:broker/src/main/java/org/apache/rocketmq/broker/processor/PopBufferMergeService.java
            }

            if (!removeFlag) {
                continue;
            }

            // put buffer ak to store, make sure store check point before storing ack msg
            if (pointWrapper.getReviveQueueOffset() < 0) {
                enqueueReviveQueue(pointWrapper, false);
                countCk++;
            }

            if (!pointWrapper.isCkStored()) {
                continue;
            }

            //store ack info after check point info
            count = storeAckInfo(count, pointWrapper);
            removeIterator(iterator, pointWrapper);
        }

        int offsetBufferSize = scanCheckPointQueueMap();

        long eclipse = System.currentTimeMillis() - startTime;
<<<<<<< HEAD:broker/src/main/java/org/apache/rocketmq/broker/server/daemon/pop/PopBufferMergeThread.java
        resetServing(eclipse, count, countCk, offsetBufferSize);
        increaseScanCounter(eclipse);
    }

    private boolean validatePointWrapper(Iterator<Map.Entry<String, PopCheckPointWrapper>> iterator, PopCheckPointWrapper pointWrapper) {
        // just process offset(already stored at pull thread), or buffer ck(not stored and ack finish)
        if (!isPointValid(pointWrapper)) {
            return true;
=======
        if (eclipse > brokerController.getBrokerConfig().getPopCkStayBufferTimeOut() - 1000) {
            POP_LOGGER.warn("[PopBuffer]scan stop, because eclipse too long, PopBufferEclipse={}, " +
                    "PopBufferToStoreAck={}, PopBufferToStoreCk={}, PopBufferSize={}, PopBufferOffsetSize={}",
                eclipse, count.get(), countCk, counter.get(), offsetBufferSize);
            this.serving = false;
        } else {
            if (scanTimes % countOfSecond1 == 0) {
                POP_LOGGER.info("[PopBuffer]scan, PopBufferEclipse={}, " +
                        "PopBufferToStoreAck={}, PopBufferToStoreCk={}, PopBufferSize={}, PopBufferOffsetSize={}",
                    eclipse, count.get(), countCk, counter.get(), offsetBufferSize);
            }
>>>>>>> develop:broker/src/main/java/org/apache/rocketmq/broker/processor/PopBufferMergeService.java
        }

        if (broker.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]ck done, {}", pointWrapper);
        }
        iterator.remove();
        counter.decrementAndGet();
        return false;
    }

    /**
     * with default config, always return true
     *
     * 1. just offset & stored, not processed by scan
     * 2. ck is buffer(ack)
     * 3. ck is buffer(not all ack), all ak are stored and ck is stored
     */
    private boolean isPointValid(PopCheckPointWrapper pointWrapper) {
        // with default config, justOffset is always true,
        // and all the check point will be enqueue to revive queue
        // and ckStored will be marked as true
        // so this method will always return true with default config
        if (pointWrapper.isJustOffset() && pointWrapper.isCkStored()) {
            return true;
        }

        if (isCkDone(pointWrapper)) {
            return true;
        }

        return isCkDoneForFinish(pointWrapper) && pointWrapper.isCkStored();
    }

    private int storeAckInfo(int count, PopCheckPointWrapper pointWrapper) {
        if (broker.getBrokerConfig().isEnablePopBatchAck()) {
            return storeBatchAckInfo(count, pointWrapper);
        }

        PopCheckPoint point = pointWrapper.getCk();
        for (byte i = 0; i < point.getNum(); i++) {
            // reput buffer ak to store
            if (!DataConverter.getBit(pointWrapper.getBits().get(), i)) {
                continue;
            }

            if (DataConverter.getBit(pointWrapper.getToStoreBits().get(), i)) {
                continue;
            }

            if (!putAckToStore(pointWrapper, i)) {
                continue;
            }

            count++;
            markBitCAS(pointWrapper.getToStoreBits(), i);
        }

        return count;
    }

    private int storeBatchAckInfo(int count, PopCheckPointWrapper pointWrapper) {
        PopCheckPoint point = pointWrapper.getCk();
        List<Byte> indexList = this.batchAckIndexList;
        try {
            for (byte i = 0; i < point.getNum(); i++) {
                // reput buffer ak to store
                if (DataConverter.getBit(pointWrapper.getBits().get(), i)
                    && !DataConverter.getBit(pointWrapper.getToStoreBits().get(), i)) {
                    indexList.add(i);
                }
            }

            if (indexList.size() <= 0) {
                return count;
            }

            if (!putBatchAckToStore(pointWrapper, indexList)) {
                return count;
            }

            count += indexList.size();
            for (Byte i : indexList) {
                markBitCAS(pointWrapper.getToStoreBits(), i);
            }
        } finally {
            indexList.clear();
        }

        return count;
    }

    private void removeIterator(Iterator<Map.Entry<String, PopCheckPointWrapper>> iterator, PopCheckPointWrapper pointWrapper) {
        if (!isCkDoneForFinish(pointWrapper) || !pointWrapper.isCkStored()) {
            return;
        }

        if (broker.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]ck finish, {}", pointWrapper);
        }
        iterator.remove();
        counter.decrementAndGet();
    }

    /**
     * @renamed from getRemoveCK to getRemoveFlag
     */
    private boolean getRemoveFlag(PopCheckPointWrapper pointWrapper) {
        PopCheckPoint point = pointWrapper.getCk();
        long now = System.currentTimeMillis();

        boolean removeCk = !this.serving;
        // ck will be timeout
        if (point.getReviveTime() - now < broker.getBrokerConfig().getPopCkStayBufferTimeOut()) {
            removeCk = true;
        }

        // the time stayed is too long
        if (now - point.getPopTime() > broker.getBrokerConfig().getPopCkStayBufferTime()) {
            removeCk = true;
        }

        if (now - point.getPopTime() > broker.getBrokerConfig().getPopCkStayBufferTime() * 2L) {
            POP_LOGGER.warn("[PopBuffer]ck finish fail, stay too long, {}", pointWrapper);
        }

        return removeCk;
    }

    private void resetServing(long eclipse, int count, int countCk, int offsetBufferSize) {
        if (eclipse > broker.getBrokerConfig().getPopCkStayBufferTimeOut() - 1000) {
            POP_LOGGER.warn("[PopBuffer]scan stop, because eclipse too long, PopBufferEclipse={}, PopBufferToStoreAck={}, PopBufferToStoreCk={}, PopBufferSize={}, PopBufferOffsetSize={}", eclipse, count, countCk, counter.get(), offsetBufferSize);
            this.serving = false;
            return;
        }

        if (scanTimes % countOfSecond1 == 0) {
            POP_LOGGER.info("[PopBuffer]scan, PopBufferEclipse={}, PopBufferToStoreAck={}, PopBufferToStoreCk={}, PopBufferSize={}, PopBufferOffsetSize={}", eclipse, count, countCk, counter.get(), offsetBufferSize);
        }
    }

    private void increaseScanCounter(long eclipse) {
        PopMetricsManager.recordPopBufferScanTimeConsume(eclipse);
        scanTimes++;

        int countOfMinute1 = (int) (60 * 1000 / interval);
        if (scanTimes >= countOfMinute1) {
            counter.set(this.buffer.size());
            scanTimes = 0;
        }
    }

    public int getOffsetTotalSize() {
        int count = 0;
        for (Map.Entry<String, QueueWithTime<PopCheckPointWrapper>> entry : this.checkPointQueueMap.entrySet()) {
            LinkedBlockingDeque<PopCheckPointWrapper> queue = entry.getValue().get();
            count += queue.size();
        }
        return count;
    }

    public int getBufferedCKSize() {
        return this.counter.get();
    }

    private void markBitCAS(AtomicInteger setBits, int index) {
        while (true) {
            int bits = setBits.get();
            if (DataConverter.getBit(bits, index)) {
                break;
            }

            int newBits = DataConverter.setBit(bits, index, true);
            if (setBits.compareAndSet(bits, newBits)) {
                break;
            }
        }
    }

    private boolean commitOffset(final PopCheckPointWrapper wrapper) {
        if (wrapper.getNextBeginOffset() < 0) {
            return true;
        }

        final PopCheckPoint popCheckPoint = wrapper.getCk();
        final String lockKey = wrapper.getLockKey();
        QueueLockManager queueLockManager = this.broker.getBrokerNettyServer().getPopServiceManager().getQueueLockManager();
        if (!queueLockManager.tryLock(lockKey)) {
            return false;
        }

        try {
            ConsumerOffsetManager offsetManager = broker.getConsumerOffsetManager();
            long offset = offsetManager.queryOffset(popCheckPoint.getCId(), popCheckPoint.getTopic(), popCheckPoint.getQueueId());

            if (wrapper.getNextBeginOffset() > offset) {
                if (broker.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.info("Commit offset, {}, {}", wrapper, offset);
                }
            } else {
                // maybe store offset is not correct.
                POP_LOGGER.warn("Commit offset, consumer offset less than store, {}, {}", wrapper, offset);
            }
            offsetManager.commitOffset(getServiceName(), popCheckPoint.getCId(), popCheckPoint.getTopic(), popCheckPoint.getQueueId(), wrapper.getNextBeginOffset());
        } finally {
            queueLockManager.unLock(lockKey);
        }

        return true;
    }

    /**
     * @renamed from putOffsetQueue to putCheckPointQueue
     *
     * @param pointWrapper checkPointWrapper
     */
<<<<<<< HEAD:broker/src/main/java/org/apache/rocketmq/broker/server/daemon/pop/PopBufferMergeThread.java
    private void putCheckPointQueue(PopCheckPointWrapper pointWrapper) {
        QueueWithTime<PopCheckPointWrapper> queue = initCheckPointQueue(pointWrapper);
        queue.setTime(pointWrapper.getCk().getPopTime());
        queue.get().offer(pointWrapper);
=======
    public boolean addCkJustOffset(PopCheckPoint point, int reviveQueueId, long reviveQueueOffset,
        long nextBeginOffset) {
        PopCheckPointWrapper pointWrapper = new PopCheckPointWrapper(reviveQueueId, reviveQueueOffset, point, nextBeginOffset, true);

        if (this.buffer.containsKey(pointWrapper.getMergeKey())) {
            // when mergeKey conflict
            // will cause PopBufferMergeService.scanCommitOffset cannot poll PopCheckPointWrapper
            POP_LOGGER.warn("[PopBuffer]mergeKey conflict when add ckJustOffset. ck:{}, mergeKey:{}", pointWrapper, pointWrapper.getMergeKey());
            return false;
        }

        this.putCkToStore(pointWrapper, checkQueueOk(pointWrapper));

        putOffsetQueue(pointWrapper);
        this.buffer.put(pointWrapper.getMergeKey(), pointWrapper);
        this.counter.incrementAndGet();
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]add ck just offset, {}", pointWrapper);
        }
        return true;
>>>>>>> develop:broker/src/main/java/org/apache/rocketmq/broker/processor/PopBufferMergeService.java
    }

    private QueueWithTime<PopCheckPointWrapper> initCheckPointQueue(PopCheckPointWrapper pointWrapper) {
        QueueWithTime<PopCheckPointWrapper> queue = this.checkPointQueueMap.get(pointWrapper.getLockKey());
        if (queue != null) {
            return queue;
        }

        queue = new QueueWithTime<>();
        QueueWithTime<PopCheckPointWrapper> tmp = this.checkPointQueueMap.putIfAbsent(pointWrapper.getLockKey(), queue);
        if (tmp != null) {
            return tmp;
        }

        return queue;
    }

    /**
     * @renamed from checkQueueOk to isQueueFull
     */
    private boolean isQueueFull(PopCheckPointWrapper pointWrapper) {
        QueueWithTime<PopCheckPointWrapper> queue = this.checkPointQueueMap.get(pointWrapper.getLockKey());
        if (queue == null) {
            return false;
        }
        return queue.get().size() >= broker.getBrokerConfig().getPopCkOffsetMaxQueueSize();
    }

    /**
     * convert check point to msg, and enqueue msg to revive queue
     * @renamed from putCkToStore to enqueueReviveQueue
     *
     * @param pointWrapper check point with wrapper
     * @param runInCurrent runInCurrent
     */
    private void enqueueReviveQueue(final PopCheckPointWrapper pointWrapper, final boolean runInCurrent) {
        if (pointWrapper.getReviveQueueOffset() >= 0) {
            return;
        }

<<<<<<< HEAD:broker/src/main/java/org/apache/rocketmq/broker/server/daemon/pop/PopBufferMergeThread.java
        //build msg for revive topic from checkPoint
        MessageExtBrokerInner msgInner = broker.getBrokerNettyServer().getPopServiceManager().buildCkMsg(pointWrapper.getCk(), pointWrapper.getReviveQueueId());

        //put msg to revive topic through escapeBridge
        PutMessageResult putMessageResult = broker.getEscapeBridge().putMessageToSpecificQueue(msgInner);

=======
        MessageExtBrokerInner msgInner = popMessageProcessor.buildCkMsg(pointWrapper.getCk(), pointWrapper.getReviveQueueId());

        // Indicates that ck message is storing
        pointWrapper.setReviveQueueOffset(Long.MAX_VALUE);
        if (brokerController.getBrokerConfig().isAppendCkAsync() && runInCurrent) {
            brokerController.getEscapeBridge().asyncPutMessageToSpecificQueue(msgInner).thenAccept(putMessageResult -> {
                handleCkMessagePutResult(putMessageResult, pointWrapper);
            }).exceptionally(throwable -> {
                POP_LOGGER.error("[PopBuffer]put ck to store fail: {}", pointWrapper, throwable);
                pointWrapper.setReviveQueueOffset(-1);
                return null;
            });
        } else {
            PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
            handleCkMessagePutResult(putMessageResult, pointWrapper);
        }
    }

    private void handleCkMessagePutResult(PutMessageResult putMessageResult, final PopCheckPointWrapper pointWrapper) {
>>>>>>> develop:broker/src/main/java/org/apache/rocketmq/broker/processor/PopBufferMergeService.java
        PopMetricsManager.incPopReviveCkPutCount(pointWrapper.getCk(), putMessageResult.getPutMessageStatus());
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            pointWrapper.setReviveQueueOffset(-1);
            POP_LOGGER.error("[PopBuffer]put ck to store fail: {}, {}", pointWrapper, putMessageResult);
            return;
        }
        pointWrapper.setCkStored(true);

        if (putMessageResult.isRemotePut()) {
            //No AppendMessageResult when escaping remotely
            pointWrapper.setReviveQueueOffset(0);
        } else {
            pointWrapper.setReviveQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());
        }

        if (broker.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]put ck to store ok: {}, {}", pointWrapper, putMessageResult);
        }
    }

    private void putAckToStore(final PopCheckPointWrapper pointWrapper, byte msgIndex, AtomicInteger count) {
        PopCheckPoint point = pointWrapper.getCk();
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        final AckMsg ackMsg = new AckMsg();

        ackMsg.setAckOffset(point.ackOffsetByIndex(msgIndex));
        ackMsg.setStartOffset(point.getStartOffset());
        ackMsg.setConsumerGroup(point.getCId());
        ackMsg.setTopic(point.getTopic());
        ackMsg.setQueueId(point.getQueueId());
        ackMsg.setPopTime(point.getPopTime());
<<<<<<< HEAD:broker/src/main/java/org/apache/rocketmq/broker/server/daemon/pop/PopBufferMergeThread.java
        msgInner.setTopic(this.reviveTopic);
=======
        ackMsg.setBrokerName(point.getBrokerName());
        msgInner.setTopic(popMessageProcessor.reviveTopic);
>>>>>>> develop:broker/src/main/java/org/apache/rocketmq/broker/processor/PopBufferMergeService.java
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(DataConverter.CHARSET_UTF8));
        msgInner.setQueueId(pointWrapper.getReviveQueueId());
        msgInner.setTags(PopConstants.ACK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(broker.getStoreHost());
        msgInner.setStoreHost(broker.getStoreHost());
        msgInner.setDeliverTimeMs(point.getReviveTime());
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopKeyBuilder.genAckUniqueId(ackMsg));

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
<<<<<<< HEAD:broker/src/main/java/org/apache/rocketmq/broker/server/daemon/pop/PopBufferMergeThread.java
        PutMessageResult putMessageResult = broker.getEscapeBridge().putMessageToSpecificQueue(msgInner);
=======

        if (brokerController.getBrokerConfig().isAppendAckAsync()) {
            brokerController.getEscapeBridge().asyncPutMessageToSpecificQueue(msgInner).thenAccept(putMessageResult -> {
                handleAckPutMessageResult(ackMsg, putMessageResult, pointWrapper, count, msgIndex);
            }).exceptionally(throwable -> {
                POP_LOGGER.error("[PopBuffer]put ack to store fail: {}, {}", pointWrapper, ackMsg, throwable);
                return null;
            });
        } else {
            PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
            handleAckPutMessageResult(ackMsg, putMessageResult, pointWrapper, count, msgIndex);
        }
    }

    private void handleAckPutMessageResult(AckMsg ackMsg, PutMessageResult putMessageResult,
        PopCheckPointWrapper pointWrapper, AtomicInteger count, byte msgIndex) {
>>>>>>> develop:broker/src/main/java/org/apache/rocketmq/broker/processor/PopBufferMergeService.java
        PopMetricsManager.incPopReviveAckPutCount(ackMsg, putMessageResult.getPutMessageStatus());
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("[PopBuffer]put ack to store fail: {}, {}, {}", pointWrapper, ackMsg, putMessageResult);
            return;
        }
        if (broker.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]put ack to store ok: {}, {}, {}", pointWrapper, ackMsg, putMessageResult);
        }
        count.incrementAndGet();
        markBitCAS(pointWrapper.getToStoreBits(), msgIndex);
    }

    private void putBatchAckToStore(final PopCheckPointWrapper pointWrapper, final List<Byte> msgIndexList,
        AtomicInteger count) {
        PopCheckPoint point = pointWrapper.getCk();
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        final BatchAckMsg batchAckMsg = new BatchAckMsg();

        for (Byte msgIndex : msgIndexList) {
            batchAckMsg.getAckOffsetList().add(point.ackOffsetByIndex(msgIndex));
        }
        batchAckMsg.setStartOffset(point.getStartOffset());
        batchAckMsg.setConsumerGroup(point.getCId());
        batchAckMsg.setTopic(point.getTopic());
        batchAckMsg.setQueueId(point.getQueueId());
        batchAckMsg.setPopTime(point.getPopTime());
        msgInner.setTopic(this.reviveTopic);
        msgInner.setBody(JSON.toJSONString(batchAckMsg).getBytes(DataConverter.CHARSET_UTF8));
        msgInner.setQueueId(pointWrapper.getReviveQueueId());
        msgInner.setTags(PopConstants.BATCH_ACK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(broker.getStoreHost());
        msgInner.setStoreHost(broker.getStoreHost());
        msgInner.setDeliverTimeMs(point.getReviveTime());
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopKeyBuilder.genBatchAckUniqueId(batchAckMsg));

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
<<<<<<< HEAD:broker/src/main/java/org/apache/rocketmq/broker/server/daemon/pop/PopBufferMergeThread.java
        PutMessageResult putMessageResult = broker.getEscapeBridge().putMessageToSpecificQueue(msgInner);
=======
        if (brokerController.getBrokerConfig().isAppendAckAsync()) {
            brokerController.getEscapeBridge().asyncPutMessageToSpecificQueue(msgInner).thenAccept(putMessageResult -> {
                handleBatchAckPutMessageResult(batchAckMsg, putMessageResult, pointWrapper, count, msgIndexList);
            }).exceptionally(throwable -> {
                POP_LOGGER.error("[PopBuffer]put batchAckMsg to store fail: {}, {}", pointWrapper, batchAckMsg, throwable);
                return null;
            });
        } else {
            PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
            handleBatchAckPutMessageResult(batchAckMsg, putMessageResult, pointWrapper, count, msgIndexList);
        }
    }

    private void handleBatchAckPutMessageResult(BatchAckMsg batchAckMsg, PutMessageResult putMessageResult,
        PopCheckPointWrapper pointWrapper, AtomicInteger count, List<Byte> msgIndexList) {
>>>>>>> develop:broker/src/main/java/org/apache/rocketmq/broker/processor/PopBufferMergeService.java
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("[PopBuffer]put batch ack to store fail: {}, {}, {}", pointWrapper, batchAckMsg, putMessageResult);
            return;
        }
        if (broker.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]put batch ack to store ok: {}, {}, {}", pointWrapper, batchAckMsg, putMessageResult);
        }

        count.addAndGet(msgIndexList.size());
        for (Byte i : msgIndexList) {
            markBitCAS(pointWrapper.getToStoreBits(), i);
        }
    }

    private boolean cancelCkTimer(final PopCheckPointWrapper pointWrapper) {
        // not stored, no need cancel
        if (pointWrapper.getReviveQueueOffset() < 0) {
            return true;
        }
        PopCheckPoint point = pointWrapper.getCk();
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(this.reviveTopic);
        msgInner.setBody((pointWrapper.getReviveQueueId() + "-" + pointWrapper.getReviveQueueOffset()).getBytes(StandardCharsets.UTF_8));
        msgInner.setQueueId(pointWrapper.getReviveQueueId());
        msgInner.setTags(PopConstants.CK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(broker.getStoreHost());
        msgInner.setStoreHost(broker.getStoreHost());

        msgInner.setDeliverTimeMs(point.getReviveTime() - PopConstants.ackTimeInterval);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        PutMessageResult putMessageResult = broker.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("[PopBuffer]PutMessageCallback cancelCheckPoint fail, {}, {}", pointWrapper, putMessageResult);
            return false;
        }
        if (broker.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]cancelCheckPoint, {}", pointWrapper);
        }
        return true;
    }

    private boolean isCkDone(PopCheckPointWrapper pointWrapper) {
        byte num = pointWrapper.getCk().getNum();
        for (byte i = 0; i < num; i++) {
            if (!DataConverter.getBit(pointWrapper.getBits().get(), i)) {
                return false;
            }
        }
        return true;
    }

    private boolean isCkDoneForFinish(PopCheckPointWrapper pointWrapper) {
        byte num = pointWrapper.getCk().getNum();
        int bits = pointWrapper.getBits().get() ^ pointWrapper.getToStoreBits().get();
        for (byte i = 0; i < num; i++) {
            if (DataConverter.getBit(bits, i)) {
                return false;
            }
        }
        return true;
    }

}
