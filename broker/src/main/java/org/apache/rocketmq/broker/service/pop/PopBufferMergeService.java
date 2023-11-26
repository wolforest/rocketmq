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
package org.apache.rocketmq.broker.service.pop;

import com.alibaba.fastjson.JSON;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.PopMetricsManager;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.constant.PopConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.BatchAckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;
import org.apache.rocketmq.store.pop.PopCheckPointWrapper;
import org.apache.rocketmq.store.pop.PopKeyBuilder;
import org.apache.rocketmq.store.pop.QueueWithTime;

/**
 * manage checkPoint and ack info of pop message
 * @renamed from PopBufferMergeService to PopAckService
 *
 * with default config:
 * - this class is just the proxy of revive queue operations
 * - the check point operations are useless
 */
public class PopBufferMergeService extends ServiceThread {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    /**
     *
     * @renamed from buffer to checkPointMap
     *
     * use cases:
     * - scan
     * - addAckMsg: mark ack state of Check Point
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
    ConcurrentHashMap<String/*topic@cid@queueId*/, QueueWithTime<PopCheckPointWrapper>> commitOffsets = new ConcurrentHashMap<>();
    private volatile boolean serving = true;
    private final AtomicInteger counter = new AtomicInteger(0);
    private int scanTimes = 0;
    private final BrokerController brokerController;
    private final String reviveTopic;

    private final long interval = 5;
    private final long minute5 = 5 * 60 * 1000;
    private final int countOfMinute1 = (int) (60 * 1000 / interval);
    private final int countOfSecond1 = (int) (1000 / interval);
    private final int countOfSecond30 = (int) (30 * 1000 / interval);

    /**
     * this is a temporary Byte List, can be replaced by local var,
     * defined as instance property to avoid create a lot of local vars.
     */
    private final List<Byte> batchAckIndexList = new ArrayList<>(32);

    /**
     * whether in the master flag
     */
    private volatile boolean master = false;

    public PopBufferMergeService(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.reviveTopic = KeyBuilder.buildClusterReviveTopic(this.brokerController.getBrokerConfig().getBrokerClusterName());
    }

    @Override
    public String getServiceName() {
        if (this.brokerController != null && this.brokerController.getBrokerConfig().isInBrokerContainer()) {
            return brokerController.getBrokerIdentity().getIdentifier() + PopBufferMergeService.class.getSimpleName();
        }
        return PopBufferMergeService.class.getSimpleName();
    }

    /**
     * with default config, this method is useless, will do nothing but empty loop
     */
    @Override
    public void run() {
        // scan
        while (!this.isStopped()) {
            try {
                if (!shouldRun()) {
                    clearBuffer();
                    continue;
                }

                scan();
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
        if (!shouldRun()) {
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
        QueueWithTime<PopCheckPointWrapper> queue = this.commitOffsets.get(lockKey);
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
        putOffsetQueue(pointWrapper);
        this.buffer.put(pointWrapper.getMergeKey(), pointWrapper);


        this.counter.incrementAndGet();
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]add ck just offset, {}", pointWrapper);
        }
        return  true;
    }

    /**
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

        putOffsetQueue(pointWrapper);
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]add ck just offset, mocked, {}", pointWrapper);
        }
    }

    /**
     * with default config, this method is useless, always return false
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
    public boolean addCheckPoint(PopCheckPoint point, int reviveQueueId, long reviveQueueOffset, long nextBeginOffset) {
        // key: point.getT() + point.getC() + point.getQ() + point.getSo() + point.getPt()
        if (!brokerController.getBrokerConfig().isEnablePopBufferMerge()) {
            return false;
        }
        if (!serving) {
            return false;
        }

        long now = System.currentTimeMillis();
        if (point.getReviveTime() - now < brokerController.getBrokerConfig().getPopCkStayBufferTimeOut() + 1500) {
            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.warn("[PopBuffer]add ck, timeout, {}, {}", point, now);
            }
            return false;
        }

        if (this.counter.get() > brokerController.getBrokerConfig().getPopCkMaxBufferSize()) {
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

        putOffsetQueue(pointWrapper);
        this.buffer.put(pointWrapper.getMergeKey(), pointWrapper);
        this.counter.incrementAndGet();
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]add ck, {}", pointWrapper);
        }
        return true;
    }

    /**
     * add ackMsg to buffer
     * ackMsgs will be stored in buffer(memory)
     * and persist periodically
     *
     * @param reviveQid reviveQid
     * @param ackMsg AckMsg
     * @return adding status
     */
    public boolean addAckMsg(int reviveQid, AckMsg ackMsg) {
        if (!brokerController.getBrokerConfig().isEnablePopBufferMerge()) {
            return false;
        }
        if (!serving) {
            return false;
        }
        try {
            PopCheckPointWrapper pointWrapper = this.buffer.get(PopKeyBuilder.buildKey(ackMsg));
            if (pointWrapper == null) {
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.warn("[PopBuffer]add ack fail, rqId={}, no ck, {}", reviveQid, ackMsg);
                }
                return false;
            }

            if (pointWrapper.isJustOffset()) {
                return false;
            }

            PopCheckPoint point = pointWrapper.getCk();
            long now = System.currentTimeMillis();

            if (point.getReviveTime() - now < brokerController.getBrokerConfig().getPopCkStayBufferTimeOut() + 1500) {
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.warn("[PopBuffer]add ack fail, rqId={}, almost timeout for revive, {}, {}, {}", reviveQid, pointWrapper, ackMsg, now);
                }
                return false;
            }

            if (now - point.getPopTime() > brokerController.getBrokerConfig().getPopCkStayBufferTime() - 1500) {
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
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

            if (brokerController.getBrokerConfig().isEnablePopLog()) {
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
        this.commitOffsets.remove(lockKey);
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
    private boolean shouldRun() {
        if (this.brokerController.getBrokerConfig().isEnableSlaveActingMaster()) {
            return true;
        }

        this.master = brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE;
        return this.master;
    }

    private void clearBuffer() {
        // slave
        this.waitForRunning(interval * 200 * 5);
        POP_LOGGER.info("Broker is {}, {}, clear all data",
            brokerController.getMessageStoreConfig().getBrokerRole(), this.master);
        this.buffer.clear();
        this.commitOffsets.clear();
    }

    private int scanCommitOffset() {
        Iterator<Map.Entry<String, QueueWithTime<PopCheckPointWrapper>>> iterator = this.commitOffsets.entrySet().iterator();
        int count = 0;
        while (iterator.hasNext()) {
            Map.Entry<String, QueueWithTime<PopCheckPointWrapper>> entry = iterator.next();
            LinkedBlockingDeque<PopCheckPointWrapper> queue = entry.getValue().get();

            scanCommitOffset(queue);

            final int qs = queue.size();
            count += qs;
            if (qs > 5000 && scanTimes % countOfSecond1 == 0) {
                POP_LOGGER.info("[PopBuffer] offset queue size too long, {}, {}", entry.getKey(), qs);
            }
        }
        return count;
    }

    private void scanCommitOffset(LinkedBlockingDeque<PopCheckPointWrapper> queue) {
        PopCheckPointWrapper pointWrapper;
        while ((pointWrapper = queue.peek()) != null) {
            if (isPointValid(pointWrapper)) {
                if (!commitOffset(pointWrapper)) {
                    break;
                }

                queue.poll();
            } else if (System.currentTimeMillis() - pointWrapper.getCk().getPopTime()
                > brokerController.getBrokerConfig().getPopCkStayBufferTime() * 2L) {
                POP_LOGGER.warn("[PopBuffer] ck offset long time not commit, {}", pointWrapper);
            }
        }
    }

    private void scanGarbage() {
        Iterator<Map.Entry<String, QueueWithTime<PopCheckPointWrapper>>> iterator = commitOffsets.entrySet().iterator();
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
            if (brokerController.getTopicConfigManager().selectTopicConfig(topic) == null) {
                POP_LOGGER.info("[PopBuffer]remove not exit topic {} in buffer!", topic);
                iterator.remove();
                continue;
            }
            if (!brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable().containsKey(cid)) {
                POP_LOGGER.info("[PopBuffer]remove not exit sub {} of topic {} in buffer!", cid, topic);
                iterator.remove();
                continue;
            }
            if (System.currentTimeMillis() - entry.getValue().getTime() > minute5) {
                POP_LOGGER.info("[PopBuffer]remove long time not used sub {} of topic {} in buffer!", cid, topic);
                iterator.remove();
            }
        }
    }

    // with default config, this method will do nothing
    private void scan() {
        long startTime = System.currentTimeMillis();
        int count = 0, countCk = 0;
        Iterator<Map.Entry<String, PopCheckPointWrapper>> iterator = buffer.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, PopCheckPointWrapper> entry = iterator.next();
            PopCheckPointWrapper pointWrapper = entry.getValue();

            // with default config, validatePointWrapper will always return false
            // and the while process will always stop here
            if (!validatePointWrapper(iterator, pointWrapper)) {
                continue;
            }

            // double check
            if (isCkDone(pointWrapper)) {
                continue;
            }

            // with default config, justOffset is always true
            if (pointWrapper.isJustOffset()) {
                // just offset should be in store.
                if (pointWrapper.getReviveQueueOffset() < 0) {
                    enqueueReviveQueue(pointWrapper, false);
                    countCk++;
                }
                continue;
            }

            boolean removeFlag = getRemoveFlag(pointWrapper);
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

        long eclipse = System.currentTimeMillis() - startTime;
        resetServing(eclipse, count, countCk);
        increaseScanCounter(eclipse);
    }

    private boolean validatePointWrapper(Iterator<Map.Entry<String, PopCheckPointWrapper>> iterator, PopCheckPointWrapper pointWrapper) {
        // just process offset(already stored at pull thread), or buffer ck(not stored and ack finish)
        if (!isPointValid(pointWrapper)) {
            return true;
        }

        if (brokerController.getBrokerConfig().isEnablePopLog()) {
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

        if (isCkDoneForFinish(pointWrapper) && pointWrapper.isCkStored()) {
            return true;
        }

        return isCkDone(pointWrapper);
    }

    private int storeAckInfo(int count, PopCheckPointWrapper pointWrapper) {
        if (brokerController.getBrokerConfig().isEnablePopBatchAck()) {
            return storeBatchAckInfo(count, pointWrapper);
        }

        PopCheckPoint point = pointWrapper.getCk();
        for (byte i = 0; i < point.getNum(); i++) {
            // reput buffer ak to store
            if (DataConverter.getBit(pointWrapper.getBits().get(), i)
                && !DataConverter.getBit(pointWrapper.getToStoreBits().get(), i)) {
                if (putAckToStore(pointWrapper, i)) {
                    count++;
                    markBitCAS(pointWrapper.getToStoreBits(), i);
                }
            }
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
            if (indexList.size() > 0) {
                if (putBatchAckToStore(pointWrapper, indexList)) {
                    count += indexList.size();
                    for (Byte i : indexList) {
                        markBitCAS(pointWrapper.getToStoreBits(), i);
                    }
                }
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

        if (brokerController.getBrokerConfig().isEnablePopLog()) {
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
        if (point.getReviveTime() - now < brokerController.getBrokerConfig().getPopCkStayBufferTimeOut()) {
            removeCk = true;
        }

        // the time stayed is too long
        if (now - point.getPopTime() > brokerController.getBrokerConfig().getPopCkStayBufferTime()) {
            removeCk = true;
        }

        if (now - point.getPopTime() > brokerController.getBrokerConfig().getPopCkStayBufferTime() * 2L) {
            POP_LOGGER.warn("[PopBuffer]ck finish fail, stay too long, {}", pointWrapper);
        }

        return removeCk;
    }

    private void resetServing(long eclipse, int count, int countCk) {
        int offsetBufferSize = scanCommitOffset();

        if (eclipse > brokerController.getBrokerConfig().getPopCkStayBufferTimeOut() - 1000) {
            POP_LOGGER.warn("[PopBuffer]scan stop, because eclipse too long, PopBufferEclipse={}, " +
                    "PopBufferToStoreAck={}, PopBufferToStoreCk={}, PopBufferSize={}, PopBufferOffsetSize={}",
                eclipse, count, countCk, counter.get(), offsetBufferSize);
            this.serving = false;
        } else {
            if (scanTimes % countOfSecond1 == 0) {
                POP_LOGGER.info("[PopBuffer]scan, PopBufferEclipse={}, " +
                        "PopBufferToStoreAck={}, PopBufferToStoreCk={}, PopBufferSize={}, PopBufferOffsetSize={}",
                    eclipse, count, countCk, counter.get(), offsetBufferSize);
            }
        }
    }

    private void increaseScanCounter(long eclipse) {
        PopMetricsManager.recordPopBufferScanTimeConsume(eclipse);
        scanTimes++;

        if (scanTimes >= countOfMinute1) {
            counter.set(this.buffer.size());
            scanTimes = 0;
        }
    }

    public int getOffsetTotalSize() {
        int count = 0;
        for (Map.Entry<String, QueueWithTime<PopCheckPointWrapper>> entry : this.commitOffsets.entrySet()) {
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
        QueueLockManager queueLockManager = this.brokerController.getBrokerNettyServer().getPopServiceManager().getQueueLockManager();
        if (!queueLockManager.tryLock(lockKey)) {
            return false;
        }
        try {
            final long offset = brokerController.getConsumerOffsetManager().queryOffset(popCheckPoint.getCId(), popCheckPoint.getTopic(), popCheckPoint.getQueueId());
            if (wrapper.getNextBeginOffset() > offset) {
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.info("Commit offset, {}, {}", wrapper, offset);
                }
            } else {
                // maybe store offset is not correct.
                POP_LOGGER.warn("Commit offset, consumer offset less than store, {}, {}", wrapper, offset);
            }
            brokerController.getConsumerOffsetManager().commitOffset(getServiceName(),
                popCheckPoint.getCId(), popCheckPoint.getTopic(), popCheckPoint.getQueueId(), wrapper.getNextBeginOffset());
        } finally {
            queueLockManager.unLock(lockKey);
        }
        return true;
    }

    private boolean putOffsetQueue(PopCheckPointWrapper pointWrapper) {
        QueueWithTime<PopCheckPointWrapper> queue = this.commitOffsets.get(pointWrapper.getLockKey());
        if (queue == null) {
            queue = new QueueWithTime<>();
            QueueWithTime<PopCheckPointWrapper> old = this.commitOffsets.putIfAbsent(pointWrapper.getLockKey(), queue);
            if (old != null) {
                queue = old;
            }
        }
        queue.setTime(pointWrapper.getCk().getPopTime());
        return queue.get().offer(pointWrapper);
    }

    /**
     * @renamed from checkQueueOk to isQueueFull
     */
    private boolean isQueueFull(PopCheckPointWrapper pointWrapper) {
        QueueWithTime<PopCheckPointWrapper> queue = this.commitOffsets.get(pointWrapper.getLockKey());
        if (queue == null) {
            return false;
        }
        return queue.get().size() >= brokerController.getBrokerConfig().getPopCkOffsetMaxQueueSize();
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

        //build msg for revive topic from checkPoint
        MessageExtBrokerInner msgInner = brokerController.getBrokerNettyServer().getPopServiceManager().buildCkMsg(pointWrapper.getCk(), pointWrapper.getReviveQueueId());

        //put msg to revive topic through escapeBridge
        PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);

        PopMetricsManager.incPopReviveCkPutCount(pointWrapper.getCk(), putMessageResult.getPutMessageStatus());
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
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

        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]put ck to store ok: {}, {}", pointWrapper, putMessageResult);
        }
    }

    private boolean putAckToStore(final PopCheckPointWrapper pointWrapper, byte msgIndex) {
        PopCheckPoint point = pointWrapper.getCk();
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        final AckMsg ackMsg = new AckMsg();

        ackMsg.setAckOffset(point.ackOffsetByIndex(msgIndex));
        ackMsg.setStartOffset(point.getStartOffset());
        ackMsg.setConsumerGroup(point.getCId());
        ackMsg.setTopic(point.getTopic());
        ackMsg.setQueueId(point.getQueueId());
        ackMsg.setPopTime(point.getPopTime());
        msgInner.setTopic(this.reviveTopic);
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(DataConverter.CHARSET_UTF8));
        msgInner.setQueueId(pointWrapper.getReviveQueueId());
        msgInner.setTags(PopConstants.ACK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());
        msgInner.setDeliverTimeMs(point.getReviveTime());
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopKeyBuilder.genAckUniqueId(ackMsg));

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        PopMetricsManager.incPopReviveAckPutCount(ackMsg, putMessageResult.getPutMessageStatus());
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("[PopBuffer]put ack to store fail: {}, {}, {}", pointWrapper, ackMsg, putMessageResult);
            return false;
        }
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]put ack to store ok: {}, {}, {}", pointWrapper, ackMsg, putMessageResult);
        }

        return true;
    }

    private boolean putBatchAckToStore(final PopCheckPointWrapper pointWrapper, final List<Byte> msgIndexList) {
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
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());
        msgInner.setDeliverTimeMs(point.getReviveTime());
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopKeyBuilder.genBatchAckUniqueId(batchAckMsg));

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
                && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
                && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
                && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("[PopBuffer]put batch ack to store fail: {}, {}, {}", pointWrapper, batchAckMsg, putMessageResult);
            return false;
        }
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]put batch ack to store ok: {}, {}, {}", pointWrapper, batchAckMsg, putMessageResult);
        }

        return true;
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
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());

        msgInner.setDeliverTimeMs(point.getReviveTime() - PopConstants.ackTimeInterval);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("[PopBuffer]PutMessageCallback cancelCheckPoint fail, {}, {}", pointWrapper, putMessageResult);
            return false;
        }
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
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
