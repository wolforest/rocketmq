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
package org.apache.rocketmq.broker.domain.queue.offset;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.common.app.config.BrokerPathConfigHelper;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class LmqConsumerOffsetManager extends ConsumerOffsetManager {
    private ConcurrentHashMap<String, Long> lmqOffsetTable = new ConcurrentHashMap<>(512);

    public LmqConsumerOffsetManager() {

    }

    public LmqConsumerOffsetManager(Broker broker) {
        super(broker);
    }

    @Override
    public long queryOffset(final String group, final String topic, final int queueId) {
        if (!MQConstants.isLmq(group)) {
            return super.queryOffset(group, topic, queueId);
        }
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        Long offset = lmqOffsetTable.get(key);
        if (offset != null) {
            return offset;
        }
        return -1;
    }

    @Override
    public Map<Integer, Long> queryOffset(final String group, final String topic) {
        if (!MQConstants.isLmq(group)) {
            return super.queryOffset(group, topic);
        }
        Map<Integer, Long> map = new HashMap<>();
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        Long offset = lmqOffsetTable.get(key);
        if (offset != null) {
            map.put(0, offset);
        }
        return map;
    }

    @Override
    public void commitOffset(final String clientHost, final String group, final String topic, final int queueId,
        final long offset) {
        if (!MQConstants.isLmq(group)) {
            super.commitOffset(clientHost, group, topic, queueId, offset);
            return;
        }
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        lmqOffsetTable.put(key, offset);
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getLmqConsumerOffsetPath(broker.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString == null) {
            return;
        }

        LmqConsumerOffsetManager obj = RemotingSerializable.fromJson(jsonString, LmqConsumerOffsetManager.class);
        if (obj == null) {
            return;
        }

        super.setOffsetTable(obj.getOffsetTable());
        this.lmqOffsetTable = obj.lmqOffsetTable;
    }

    @Override
    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public ConcurrentHashMap<String, Long> getLmqOffsetTable() {
        return lmqOffsetTable;
    }

    public void setLmqOffsetTable(ConcurrentHashMap<String, Long> lmqOffsetTable) {
        this.lmqOffsetTable = lmqOffsetTable;
    }
}
