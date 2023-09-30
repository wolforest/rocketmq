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
package org.apache.rocketmq.store.timer.transit;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.timer.AbstractStateService;
import org.apache.rocketmq.store.timer.MessageOperator;
import org.apache.rocketmq.store.timer.TimerRequest;
import org.apache.rocketmq.store.timer.TimerState;
import org.apache.rocketmq.store.util.PerfCounter;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class TimerMessageQuery extends AbstractStateService {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final TimerState timerState;
    private final MessageStoreConfig storeConfig;
    private final MessageOperator messageReader;
    private final BlockingQueue<TimerRequest> timerMessageDeliverQueue;
    private final BlockingQueue<List<TimerRequest>> timerMessageQueryQueue;
    private final PerfCounter.Ticks perfCounterTicks;

    public TimerMessageQuery(
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
    public void run() {
        setState(AbstractStateService.START);
        LOGGER.info(this.getServiceName() + " service start");
        while (!this.isStopped()) {
            try {
                setState(AbstractStateService.WAITING);
                List<TimerRequest> timerRequestList = timerMessageQueryQueue.poll(100L * timerState.precisionMs / 1000, TimeUnit.MILLISECONDS);
                if (null == timerRequestList || timerRequestList.size() == 0) {
                    continue;
                }
                setState(AbstractStateService.RUNNING);
                for (int i = 0; i < timerRequestList.size(); ) {
                    TimerRequest timerRequest = timerRequestList.get(i);
                    boolean doRes = false;
                    try {
                        long start = System.currentTimeMillis();
                        MessageExt msgExt = messageReader.readMessageByCommitOffset(timerRequest.getOffsetPy(), timerRequest.getSizePy());
                        if (null != msgExt) {
                            if (timerState.needDelete(timerRequest.getMagic()) && !timerState.needRoll(timerRequest.getMagic())) {
                                if (msgExt.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY) != null && timerRequest.getDeleteList() != null) {
                                    timerRequest.getDeleteList().add(msgExt.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY));
                                }
                                timerRequest.idempotentRelease();
                                doRes = true;
                            } else {
                                String uniqueKey = MessageClientIDSetter.getUniqID(msgExt);
                                if (null == uniqueKey) {
                                    LOGGER.warn("No uniqueKey for msg:{}", msgExt);
                                }
                                if (null != uniqueKey && timerRequest.getDeleteList() != null && timerRequest.getDeleteList().size() > 0 && timerRequest.getDeleteList().contains(uniqueKey)) {
                                    doRes = true;
                                    timerRequest.idempotentRelease();
                                    perfCounterTicks.getCounter("dequeue_delete").flow(1);
                                } else {
                                    timerRequest.setMsg(msgExt);
                                    while (!isStopped() && !doRes) {
                                        doRes = timerMessageDeliverQueue.offer(timerRequest, 3, TimeUnit.SECONDS);
                                    }
                                }
                            }
                            perfCounterTicks.getCounter("dequeue_get_msg").flow(System.currentTimeMillis() - start);
                        } else {
                            //the tr will never be processed afterwards, so idempotentRelease it
                            timerRequest.idempotentRelease();
                            doRes = true;
                            perfCounterTicks.getCounter("dequeue_get_msg_miss").flow(System.currentTimeMillis() - start);
                        }
                    } catch (Throwable e) {
                        LOGGER.error("Unknown exception", e);
                        if (storeConfig.isTimerSkipUnknownError()) {
                            timerRequest.idempotentRelease();
                            doRes = true;
                        } else {
                            ThreadUtils.sleep(50);
                        }
                    } finally {
                        if (doRes) {
                            i++;
                        }
                    }
                }
                timerRequestList.clear();
            } catch (Throwable e) {
                LOGGER.error("Error occurred in " + getServiceName(), e);
            }
        }
        LOGGER.info(this.getServiceName() + " service end");
        setState(AbstractStateService.END);
    }

    @Override
    public String getServiceName() {
        return timerState.getServiceThreadName() + this.getClass().getSimpleName();
    }
}

