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
package org.apache.rocketmq.store.domain.timer;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerLog;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerWheel;
import org.apache.rocketmq.store.infra.mappedfile.SelectMappedBufferResult;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class TimerMetricManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final TimerState timerState;
    private final MessageStoreConfig storeConfig;
    private final TimerWheel timerWheel;
    private final TimerLog timerLog;
    private final MessageOperator messageReader;
    private final TimerMetrics timerMetrics;
    private final int timerLogFileSize;
    private final AtomicInteger frequency = new AtomicInteger(0);

    public TimerMetricManager(TimerState timerState,
                              MessageStoreConfig storeConfig,
                              TimerWheel timerWheel,
                              TimerLog timerLog,
                              MessageOperator messageReader,
                              TimerMetrics timerMetrics) {
        this.timerMetrics = timerMetrics;
        this.storeConfig = storeConfig;
        this.messageReader = messageReader;
        this.timerWheel = timerWheel;
        this.timerLog = timerLog;
        this.timerState = timerState;
        this.timerLogFileSize = storeConfig.getMappedFileSizeTimerLog();
    }

    public int hashTopicForMetrics(String topic) {
        return null == topic ? 0 : topic.hashCode();
    }

    public void addMetric(MessageExt msg, int value) {
        try {
            if (null == msg || null == msg.getProperty(MessageConst.PROPERTY_REAL_TOPIC)) {
                return;
            }
            if (msg.getProperty(TimerState.TIMER_ENQUEUE_MS) != null
                && NumberUtils.toLong(msg.getProperty(TimerState.TIMER_ENQUEUE_MS)) == Long.MAX_VALUE) {
                return;
            }
            // pass msg into addAndGet, for further more judgement extension.
            timerMetrics.addAndGet(msg, value);
        } catch (Throwable t) {
            if (frequency.incrementAndGet() % 1000 == 0) {
                LOGGER.error("error in adding metric", t);
            }
        }

    }

    public void checkAndReviseMetrics() {
        Map<String, TimerMetrics.Metric> smallOnes = new HashMap<>();
        Map<String, TimerMetrics.Metric> bigOnes = new HashMap<>();
        Map<Integer, String> smallHashs = new HashMap<>();
        Set<Integer> smallHashCollisions = new HashSet<>();
        for (Map.Entry<String, TimerMetrics.Metric> entry : timerMetrics.getTimingCount().entrySet()) {
            if (entry.getValue().getCount().get() >= storeConfig.getTimerMetricSmallThreshold()) {
                bigOnes.put(entry.getKey(), entry.getValue());
                continue;
            }

            smallOnes.put(entry.getKey(), entry.getValue());
            int hash = hashTopicForMetrics(entry.getKey());
            if (smallHashs.containsKey(hash)) {
                LOGGER.warn("[CheckAndReviseMetrics]Metric hash collision between small-small code:{} small topic:{}{} small topic:{}{}", hash,
                        entry.getKey(), entry.getValue(),
                        smallHashs.get(hash), smallOnes.get(smallHashs.get(hash)));
                smallHashCollisions.add(hash);
            }
            smallHashs.put(hash, entry.getKey());
        }
        //check the hash collision between small ons and big ons
        for (Map.Entry<String, TimerMetrics.Metric> bjgEntry : bigOnes.entrySet()) {
            if (!smallHashs.containsKey(hashTopicForMetrics(bjgEntry.getKey()))) {
                continue;
            }

            Iterator<Map.Entry<String, TimerMetrics.Metric>> smallIt = smallOnes.entrySet().iterator();
            while (smallIt.hasNext()) {
                Map.Entry<String, TimerMetrics.Metric> smallEntry = smallIt.next();
                if (hashTopicForMetrics(smallEntry.getKey()) == hashTopicForMetrics(bjgEntry.getKey())) {
                    LOGGER.warn("[CheckAndReviseMetrics]Metric hash collision between small-big code:{} small topic:{}{} big topic:{}{}", hashTopicForMetrics(smallEntry.getKey()),
                            smallEntry.getKey(), smallEntry.getValue(),
                            bjgEntry.getKey(), bjgEntry.getValue());
                    smallIt.remove();
                }
            }
        }
        //refresh
        smallHashs.clear();
        Map<String, TimerMetrics.Metric> newSmallOnes = new HashMap<>();
        for (String topic : smallOnes.keySet()) {
            newSmallOnes.put(topic, new TimerMetrics.Metric());
            smallHashs.put(hashTopicForMetrics(topic), topic);
        }

        //travel the timer log
        long readTimeMs = timerState.currReadTimeMs;
        long currOffsetPy = timerWheel.checkPhyPos(readTimeMs, 0);
        LinkedList<SelectMappedBufferResult> sbrs = new LinkedList<>();
        boolean hasError = false;
        try {
            while (true) {
                SelectMappedBufferResult timeSbr = timerLog.getWholeBuffer(currOffsetPy);
                if (timeSbr == null) {
                    break;
                } else {
                    sbrs.add(timeSbr);
                }
                ByteBuffer bf = timeSbr.getByteBuffer();
                for (int position = 0; position < timeSbr.getSize(); position += TimerLog.UNIT_SIZE) {
                    bf.position(position);
                    bf.getInt();//size
                    bf.getLong();//prev pos
                    int magic = bf.getInt(); //magic
                    long enqueueTime = bf.getLong();
                    long delayedTime = bf.getInt() + enqueueTime;
                    long offsetPy = bf.getLong();
                    int sizePy = bf.getInt();
                    int hashCode = bf.getInt();
                    if (delayedTime < readTimeMs) {
                        continue;
                    }
                    if (!smallHashs.containsKey(hashCode)) {
                        continue;
                    }
                    String topic = null;
                    if (smallHashCollisions.contains(hashCode)) {
                        MessageExt messageExt = messageReader.readMessageByCommitOffset(offsetPy, sizePy);
                        if (null != messageExt) {
                            topic = messageExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC);
                        }
                    } else {
                        topic = smallHashs.get(hashCode);
                    }
                    if (null != topic && newSmallOnes.containsKey(topic)) {
                        newSmallOnes.get(topic).getCount().addAndGet(timerState.needDelete(magic) ? -1 : 1);
                    } else {
                        LOGGER.warn("[CheckAndReviseMetrics]Unexpected topic in checking timer metrics topic:{} code:{} offsetPy:{} size:{}", topic, hashCode, offsetPy, sizePy);
                    }
                }
                if (timeSbr.getSize() < timerLogFileSize) {
                    break;
                } else {
                    currOffsetPy = currOffsetPy + timerLogFileSize;
                }
            }

        } catch (Exception e) {
            hasError = true;
            LOGGER.error("[CheckAndReviseMetrics]Unknown error in checkAndReviseMetrics and abort", e);
        } finally {
            for (SelectMappedBufferResult sbr : sbrs) {
                if (null != sbr) {
                    sbr.release();
                }
            }
        }

        if (!hasError) {
            //update
            for (String topic : newSmallOnes.keySet()) {
                LOGGER.info("[CheckAndReviseMetrics]Revise metric for topic {} from {} to {}", topic, smallOnes.get(topic), newSmallOnes.get(topic));
            }
            timerMetrics.getTimingCount().putAll(newSmallOnes);
        }

    }

}