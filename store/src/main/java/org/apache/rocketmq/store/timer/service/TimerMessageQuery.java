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
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.timer.TimerState;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.timer.TimerRequest;
import org.apache.rocketmq.store.util.PerfCounter;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class TimerMessageQuery extends AbstractStateService {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    @Override
    public String getServiceName() {
        return timerMessageStore.getServiceThreadName() + this.getClass().getSimpleName();
    }

    private TimerMessageStore timerMessageStore;
    private BlockingQueue<TimerRequest> dequeuePutQueue;
    private PerfCounter.Ticks perfCounterTicks;
    private TimerCheckpoint timerCheckpoint;
    private TimerState timerState;
    private MessageStoreConfig storeConfig;
    private BlockingQueue<List<TimerRequest>> dequeueGetQueue;
    private MessageReader messageReader;
    public TimerMessageQuery(TimerMessageStore timerMessageStore,TimerState timerState,TimerCheckpoint timerCheckpoint,MessageReader messageReader) {
        this.timerMessageStore = timerMessageStore;
        this.messageReader = messageReader;
        dequeuePutQueue = timerMessageStore.getTimerMessageDeliverQueue();
        dequeueGetQueue = timerMessageStore.getTimerMessageQueryQueue();
        perfCounterTicks = timerMessageStore.getPerfCounterTicks();
        this.timerState = timerState;
        storeConfig = timerMessageStore.getMessageStore().getMessageStoreConfig();
        this.timerCheckpoint = timerCheckpoint;
        timerMessageStore.getTimerMessageQueryQueue();
    }
    @Override
    public void run() {
        setState(AbstractStateService.START);
        LOGGER.info(this.getServiceName() + " service start");
        while (!this.isStopped()) {
            try {
                setState(AbstractStateService.WAITING);
                List<TimerRequest> trs = dequeueGetQueue.poll(100L * timerMessageStore.getPrecisionMs() / 1000, TimeUnit.MILLISECONDS);
                if (null == trs || trs.size() == 0) {
                    continue;
                }
                setState(AbstractStateService.RUNNING);
                for (int i = 0; i < trs.size(); ) {
                    TimerRequest tr = trs.get(i);
                    boolean doRes = false;
                    try {
                        long start = System.currentTimeMillis();
                        MessageExt msgExt = messageReader.getMessageByCommitOffset(tr.getOffsetPy(), tr.getSizePy());
                        if (null != msgExt) {
                            if (timerMessageStore.timerState.needDelete(tr.getMagic()) && !timerMessageStore.timerState.needRoll(tr.getMagic())) {
                                if (msgExt.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY) != null && tr.getDeleteList() != null) {
                                    tr.getDeleteList().add(msgExt.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY));
                                }
                                tr.idempotentRelease();
                                doRes = true;
                            } else {
                                String uniqueKey = MessageClientIDSetter.getUniqID(msgExt);
                                if (null == uniqueKey) {
                                    LOGGER.warn("No uniqueKey for msg:{}", msgExt);
                                }
                                if (null != uniqueKey && tr.getDeleteList() != null && tr.getDeleteList().size() > 0 && tr.getDeleteList().contains(uniqueKey)) {
                                    doRes = true;
                                    tr.idempotentRelease();
                                    perfCounterTicks.getCounter("dequeue_delete").flow(1);
                                } else {
                                    tr.setMsg(msgExt);
                                    while (!isStopped() && !doRes) {
                                        doRes = dequeuePutQueue.offer(tr, 3, TimeUnit.SECONDS);
                                    }
                                }
                            }
                            perfCounterTicks.getCounter("dequeue_get_msg").flow(System.currentTimeMillis() - start);
                        } else {
                            //the tr will never be processed afterwards, so idempotentRelease it
                            tr.idempotentRelease();
                            doRes = true;
                            perfCounterTicks.getCounter("dequeue_get_msg_miss").flow(System.currentTimeMillis() - start);
                        }
                    } catch (Throwable e) {
                        LOGGER.error("Unknown exception", e);
                        if (storeConfig.isTimerSkipUnknownError()) {
                            tr.idempotentRelease();
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
                trs.clear();
            } catch (Throwable e) {
                LOGGER.error("Error occurred in " + getServiceName(), e);
            }
        }
        LOGGER.info(this.getServiceName() + " service end");
        setState(AbstractStateService.END);
    }
}

