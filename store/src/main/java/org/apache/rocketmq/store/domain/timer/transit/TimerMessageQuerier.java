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
package org.apache.rocketmq.store.domain.timer.transit;

import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageClientIDSetter;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.domain.timer.model.TimerRequest;
import org.apache.rocketmq.store.domain.timer.model.TimerState;
import org.apache.rocketmq.store.server.metrics.PerfCounter;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @renamed from TimerDequeueGetMessageService to TimerMessageQuerier
 * poll msg from timerMessageQueryQueue, then:
 *  1. release the msg should be deleted
 *  2. enqueue timerMessageDeliverQueue
 */
public class TimerMessageQuerier extends AbstractStateThread {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final TimerState timerState;
    private final MessageStoreConfig storeConfig;
    private final MessageOperator messageReader;
    private final BlockingQueue<TimerRequest> timerMessageDeliverQueue;
    private final BlockingQueue<List<TimerRequest>> timerMessageQueryQueue;
    private final PerfCounter.Ticks perfCounterTicks;

    public TimerMessageQuerier(
            TimerState timerState,
            MessageStoreConfig storeConfig,
            MessageOperator messageReader,
            BlockingQueue<TimerRequest> timerMessageDeliverQueue,
            BlockingQueue<List<TimerRequest>> timerMessageQueryQueue,
            PerfCounter.Ticks perfCounterTicks) {
        this.messageReader = messageReader;
        this.timerMessageDeliverQueue = timerMessageDeliverQueue;
        this.timerMessageQueryQueue = timerMessageQueryQueue;
        this.perfCounterTicks = perfCounterTicks;
        this.storeConfig = storeConfig;
        this.timerState = timerState;
    }

    @Override
    public String getServiceName() {
        return timerState.getServiceThreadName() + this.getClass().getSimpleName();
    }

    @Override
    public void run() {
        setState(AbstractStateThread.START);
        LOGGER.info(this.getServiceName() + " service start");
        while (!this.isStopped()) {
            try {
                setState(AbstractStateThread.WAITING);
                List<TimerRequest> timerRequestList = timerMessageQueryQueue.poll(100L * timerState.precisionMs / 1000, TimeUnit.MILLISECONDS);
                if (null == timerRequestList || timerRequestList.isEmpty()) {
                    continue;
                }

                setState(AbstractStateThread.RUNNING);
                run(timerRequestList);
                timerRequestList.clear();
            } catch (Throwable e) {
                LOGGER.error("Error occurred in " + getServiceName(), e);
            }
        }
        LOGGER.info(this.getServiceName() + " service end");
        setState(AbstractStateThread.END);
    }

    private void run(List<TimerRequest> timerRequestList) {
        for (int i = 0; i < timerRequestList.size(); ) {
            TimerRequest timerRequest = timerRequestList.get(i);
            i = run(timerRequest, i);
        }
    }

    private int run(TimerRequest timerRequest, int i) {
        boolean doRes = false;
        try {
            long start = System.currentTimeMillis();
            MessageExt msgExt = messageReader.readMessageByCommitOffset(timerRequest.getCommitLogOffset(), timerRequest.getMessageSize());
            if (null == msgExt) {
                doRes = handleNoMsgFound(doRes, timerRequest, start);
                return i;
            }

            if (timerState.needDelete(timerRequest.getMagic()) && !timerState.needRoll(timerRequest.getMagic())) {
                doRes = deleteTimerRequest(doRes, timerRequest, msgExt);
            } else {
                doRes = enqueueDeliverQueue(doRes, timerRequest, msgExt);
            }

            perfCounterTicks.getCounter("dequeue_get_msg").flow(System.currentTimeMillis() - start);
        } catch (Throwable e) {
            doRes = handleUnknownException(e, timerRequest, doRes);
        } finally {
            if (doRes) {
                i++;
            }
        }

        return i;
    }

    private boolean handleNoMsgFound(boolean doRes, TimerRequest timerRequest, long start) {
        //the timerRequest will never be processed afterwards, so idempotentRelease it
        timerRequest.idempotentRelease();
        doRes = true;
        perfCounterTicks.getCounter("dequeue_get_msg_miss").flow(System.currentTimeMillis() - start);

        return doRes;
    }

    private boolean deleteTimerRequest(boolean doRes, TimerRequest timerRequest, MessageExt msgExt) {
        if (msgExt.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY) != null && timerRequest.getDeleteList() != null) {
            timerRequest.getDeleteList().add(msgExt.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY));
        }
        timerRequest.idempotentRelease();
        doRes = true;

        return doRes;
    }

    private boolean enqueueDeliverQueue(boolean doRes, TimerRequest timerRequest, MessageExt msgExt) throws InterruptedException {
        String uniqueKey = MessageClientIDSetter.getUniqID(msgExt);
        if (null == uniqueKey) {
            LOGGER.warn("No uniqueKey for msg:{}", msgExt);
        }
        if (null != uniqueKey && timerRequest.getDeleteList() != null && !timerRequest.getDeleteList().isEmpty() && timerRequest.getDeleteList().contains(uniqueKey)) {
            doRes = true;
            timerRequest.idempotentRelease();
            perfCounterTicks.getCounter("dequeue_delete").flow(1);
        } else {
            timerRequest.setMsg(msgExt);
            while (!isStopped() && !doRes) {
                doRes = timerMessageDeliverQueue.offer(timerRequest, 3, TimeUnit.SECONDS);
            }
        }

        return doRes;
    }

    private boolean handleUnknownException(Throwable e, TimerRequest timerRequest, boolean doRes) {
        LOGGER.error("Unknown exception", e);
        if (storeConfig.isTimerSkipUnknownError()) {
            timerRequest.idempotentRelease();
            doRes = true;
        } else {
            ThreadUtils.sleep(50);
        }

        return doRes;
    }
}

