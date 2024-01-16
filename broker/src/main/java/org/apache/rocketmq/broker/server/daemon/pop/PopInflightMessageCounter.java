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

import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.common.lang.Pair;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.api.pop.PopCheckPoint;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PopInflightMessageCounter {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final String TOPIC_GROUP_SEPARATOR = "@";
    private final Map<String /* topic@group */, Map<Integer /* queueId */, AtomicLong>> topicInFlightMessageNum =
        new ConcurrentHashMap<>(512);
    private final Broker broker;

    public PopInflightMessageCounter(Broker broker) {
        this.broker = broker;
    }

    public void incrementInFlightMessageNum(String topic, String group, int queueId, int num) {
        if (num <= 0) {
            return;
        }
        topicInFlightMessageNum.compute(buildKey(topic, group), (key, queueNum) -> {
            if (queueNum == null) {
                queueNum = new ConcurrentHashMap<>(8);
            }
            queueNum.compute(queueId, (queueIdKey, counter) -> {
                if (counter == null) {
                    return new AtomicLong(num);
                }
                if (counter.addAndGet(num) <= 0) {
                    return null;
                }
                return counter;
            });
            return queueNum;
        });
    }

    public void decrementInFlightMessageNum(String topic, String group, long popTime, int qId, int delta) {
        if (popTime < this.broker.getShouldStartTime()) {
            return;
        }
        decrementInFlightMessageNum(topic, group, qId, delta);
    }

    public void decrementInFlightMessageNum(PopCheckPoint checkPoint) {
        if (checkPoint.getPopTime() < this.broker.getShouldStartTime()) {
            return;
        }
        decrementInFlightMessageNum(checkPoint.getTopic(), checkPoint.getCId(), checkPoint.getQueueId(), 1);
    }

    private void decrementInFlightMessageNum(String topic, String group, int queueId, int delta) {
        topicInFlightMessageNum.computeIfPresent(buildKey(topic, group), (key, queueNum) -> {
            queueNum.computeIfPresent(queueId, (queueIdKey, counter) -> {
                if (counter.addAndGet(-delta) <= 0) {
                    return null;
                }
                return counter;
            });
            if (queueNum.isEmpty()) {
                return null;
            }
            return queueNum;
        });
    }

    public void clearInFlightMessageNumByGroupName(String group) {
        Set<String> topicGroupKey = this.topicInFlightMessageNum.keySet();
        for (String key : topicGroupKey) {
            if (!key.contains(group)) {
                continue;
            }

            Pair<String, String> topicAndGroup = splitKey(key);
            if (topicAndGroup != null && topicAndGroup.getObject2().equals(group)) {
                this.topicInFlightMessageNum.remove(key);
                log.info("PopInflightMessageCounter#clearInFlightMessageNumByGroupName: clean by group, topic={}, group={}",
                    topicAndGroup.getObject1(), topicAndGroup.getObject2());
            }
        }
    }

    public void clearInFlightMessageNumByTopicName(String topic) {
        Set<String> topicGroupKey = this.topicInFlightMessageNum.keySet();
        for (String key : topicGroupKey) {
            if (!key.contains(topic)) {
                continue;
            }

            Pair<String, String> topicAndGroup = splitKey(key);
            if (topicAndGroup != null && topicAndGroup.getObject1().equals(topic)) {
                this.topicInFlightMessageNum.remove(key);
                log.info("PopInflightMessageCounter#clearInFlightMessageNumByTopicName: clean by topic, topic={}, group={}",
                    topicAndGroup.getObject1(), topicAndGroup.getObject2());
            }
        }
    }

    public void clearInFlightMessageNum(String topic, String group, int queueId) {
        topicInFlightMessageNum.computeIfPresent(buildKey(topic, group), (key, queueNum) -> {
            queueNum.computeIfPresent(queueId, (queueIdKey, counter) -> null);
            if (queueNum.isEmpty()) {
                return null;
            }
            return queueNum;
        });
    }

    public long getGroupPopInFlightMessageNum(String topic, String group, int queueId) {
        Map<Integer /* queueId */, AtomicLong> queueCounter = topicInFlightMessageNum.get(buildKey(topic, group));
        if (queueCounter == null) {
            return 0;
        }
        AtomicLong counter = queueCounter.get(queueId);
        if (counter == null) {
            return 0;
        }
        return Math.max(0, counter.get());
    }

    private static Pair<String /* topic */, String /* group */> splitKey(String key) {
        String[] strings = key.split(TOPIC_GROUP_SEPARATOR);
        if (strings.length != 2) {
            return null;
        }
        return new Pair<>(strings[0], strings[1]);
    }

    private static String buildKey(String topic, String group) {
        return topic + TOPIC_GROUP_SEPARATOR + group;
    }
}
