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
package org.apache.rocketmq.broker.server.schedule;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.domain.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;
import org.apache.rocketmq.store.domain.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.domain.queue.CqUnit;
import org.apache.rocketmq.store.domain.queue.ReferredIterator;

public class DeliverDelayedMessageTimerTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final long DELAY_FOR_A_PERIOD = 10000L;
    private static final long DELAY_FOR_A_WHILE = 100L;

    private final int delayLevel;
    private final long offset;

    private final ScheduleMessageService scheduleMessageService;

    public DeliverDelayedMessageTimerTask(ScheduleMessageService scheduleMessageService, int delayLevel, long offset) {
        this.scheduleMessageService = scheduleMessageService;
        this.delayLevel = delayLevel;
        this.offset = offset;
    }

    @Override
    public void run() {
        try {
            if (scheduleMessageService.isStarted()) {
                this.executeOnTimeUp();
            }
        } catch (Exception e) {
            // XXX: warn and notify me
            log.error("ScheduleMessageService, executeOnTimeUp exception", e);
            this.scheduleNextTimerTask(this.offset, DELAY_FOR_A_PERIOD);
        }
    }

    private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {

        long result = deliverTimestamp;

        long maxTimestamp = now + scheduleMessageService.getDelayLevelTable().get(this.delayLevel);
        if (deliverTimestamp > maxTimestamp) {
            result = now;
        }

        return result;
    }

    public void executeOnTimeUp() {
        ConsumeQueueInterface cq =
            scheduleMessageService.getBrokerController().getMessageStore().getConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                scheduleMessageService.delayLevel2QueueId(delayLevel));

        if (cq == null) {
            this.scheduleNextTimerTask(this.offset, DELAY_FOR_A_WHILE);
            return;
        }

        ReferredIterator<CqUnit> bufferCQ = cq.iterateFrom(this.offset);
        if (bufferCQ == null) {
            long resetOffset;
            if ((resetOffset = cq.getMinOffsetInQueue()) > this.offset) {
                log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, queueId={}",
                    this.offset, resetOffset, cq.getQueueId());
            } else if ((resetOffset = cq.getMaxOffsetInQueue()) < this.offset) {
                log.error("schedule CQ offset invalid. offset={}, cqMaxOffset={}, queueId={}",
                    this.offset, resetOffset, cq.getQueueId());
            } else {
                resetOffset = this.offset;
            }

            this.scheduleNextTimerTask(resetOffset, DELAY_FOR_A_WHILE);
            return;
        }

        long nextOffset = this.offset;
        try {
            while (bufferCQ.hasNext() && scheduleMessageService.isStarted()) {
                CqUnit cqUnit = bufferCQ.next();
                long offsetPy = cqUnit.getPos();
                int sizePy = cqUnit.getSize();
                long tagsCode = cqUnit.getTagsCode();

                if (!cqUnit.isTagsCodeValid()) {
                    //can't find ext content.So re compute tags code.
                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                        tagsCode, offsetPy, sizePy);
                    long msgStoreTime = scheduleMessageService.getBrokerController().getMessageStore().getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                    tagsCode = scheduleMessageService.computeDeliverTimestamp(delayLevel, msgStoreTime);
                }

                long now = System.currentTimeMillis();
                long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                long currOffset = cqUnit.getQueueOffset();
                assert cqUnit.getBatchNum() == 1;
                nextOffset = currOffset + cqUnit.getBatchNum();

                long countdown = deliverTimestamp - now;
                if (countdown > 0) {
                    this.scheduleNextTimerTask(currOffset, DELAY_FOR_A_WHILE);
                    scheduleMessageService.updateOffset(this.delayLevel, currOffset);
                    return;
                }

                MessageExt msgExt = scheduleMessageService.getBrokerController().getMessageStore().lookMessageByOffset(offsetPy, sizePy);
                if (msgExt == null) {
                    continue;
                }

                MessageExtBrokerInner msgInner = scheduleMessageService.messageTimeUp(msgExt);
                if (TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                    log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}",
                        msgInner.getTopic(), msgInner);
                    continue;
                }

                boolean deliverSuc;
                if (scheduleMessageService.isEnableAsyncDeliver()) {
                    deliverSuc = this.asyncDeliver(msgInner, msgExt.getMsgId(), currOffset, offsetPy, sizePy);
                } else {
                    deliverSuc = this.syncDeliver(msgInner, msgExt.getMsgId(), currOffset, offsetPy, sizePy);
                }

                if (!deliverSuc) {
                    this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
                    return;
                }
            }
        } catch (Exception e) {
            log.error("ScheduleMessageService, messageTimeUp execute error, offset = {}", nextOffset, e);
        } finally {
            bufferCQ.release();
        }

        this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
    }

    public void scheduleNextTimerTask(long offset, long delay) {
        scheduleMessageService.getDeliverExecutorService().schedule(
            new DeliverDelayedMessageTimerTask(scheduleMessageService, this.delayLevel, offset),
            delay, TimeUnit.MILLISECONDS);
    }

    private boolean syncDeliver(MessageExtBrokerInner msgInner, String msgId, long offset, long offsetPy,
        int sizePy) {
        PutResultProcess resultProcess = deliverMessage(msgInner, msgId, offset, offsetPy, sizePy, false);
        PutMessageResult result = resultProcess.get();
        boolean sendStatus = result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK;
        if (sendStatus) {
            scheduleMessageService.updateOffset(this.delayLevel, resultProcess.getNextOffset());
        }
        return sendStatus;
    }

    private boolean asyncDeliver(MessageExtBrokerInner msgInner, String msgId, long offset, long offsetPy,
        int sizePy) {
        Queue<PutResultProcess> processesQueue = scheduleMessageService.getDeliverPendingTable().get(this.delayLevel);

        //Flow Control
        int currentPendingNum = processesQueue.size();
        int maxPendingLimit = scheduleMessageService.getBrokerController().getMessageStoreConfig()
            .getScheduleAsyncDeliverMaxPendingLimit();
        if (currentPendingNum > maxPendingLimit) {
            log.warn("Asynchronous deliver triggers flow control, " +
                "currentPendingNum={}, maxPendingLimit={}", currentPendingNum, maxPendingLimit);
            return false;
        }

        //Blocked
        PutResultProcess firstProcess = processesQueue.peek();
        if (firstProcess != null && firstProcess.need2Blocked()) {
            log.warn("Asynchronous deliver block. info={}", firstProcess.toString());
            return false;
        }

        PutResultProcess resultProcess = deliverMessage(msgInner, msgId, offset, offsetPy, sizePy, true);
        processesQueue.add(resultProcess);
        return true;
    }

    private PutResultProcess deliverMessage(MessageExtBrokerInner msgInner, String msgId, long offset,
        long offsetPy, int sizePy, boolean autoResend) {
        CompletableFuture<PutMessageResult> future =
            scheduleMessageService.getBrokerController().getEscapeBridge().asyncPutMessage(msgInner);
        return new PutResultProcess(scheduleMessageService)
            .setTopic(msgInner.getTopic())
            .setDelayLevel(this.delayLevel)
            .setOffset(offset)
            .setPhysicOffset(offsetPy)
            .setPhysicSize(sizePy)
            .setMsgId(msgId)
            .setAutoResend(autoResend)
            .setFuture(future)
            .thenProcess();
    }
}
