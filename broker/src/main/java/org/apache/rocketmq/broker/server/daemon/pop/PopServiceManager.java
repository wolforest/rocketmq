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
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.connection.longpolling.PopLongPollingThread;
import org.apache.rocketmq.broker.server.connection.longpolling.PopRequest;
import org.apache.rocketmq.broker.api.controller.NotificationProcessor;
import org.apache.rocketmq.broker.api.controller.PopMessageProcessor;
import org.apache.rocketmq.common.domain.topic.KeyBuilder;
import org.apache.rocketmq.common.domain.constant.PopConstants;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.api.broker.pop.PopCheckPoint;
import org.apache.rocketmq.store.api.broker.pop.PopKeyBuilder;

public class PopServiceManager {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final Broker broker;

    private final String reviveTopic;
    private final QueueLockManager queueLockManager;

    private final PopReviveThread[] popReviveThreads;
    private final PopBufferMergeThread popBufferMergeThread;

    private final PopLongPollingThread popPollingService;
    private final PopLongPollingThread notificationPollingService;


    public PopServiceManager(final Broker broker, PopMessageProcessor popMessageProcessor, NotificationProcessor notificationProcessor) {
        this.broker = broker;
        this.reviveTopic = KeyBuilder.buildClusterReviveTopic(this.broker.getBrokerConfig().getBrokerClusterName());

        this.popPollingService = new PopLongPollingThread(broker, popMessageProcessor);
        this.notificationPollingService = new PopLongPollingThread(broker, notificationProcessor);

        this.queueLockManager = new QueueLockManager(broker);
        this.popBufferMergeThread = new PopBufferMergeThread(this.broker);

        this.popReviveThreads = new PopReviveThread[this.broker.getBrokerConfig().getReviveQueueNum()];
        for (int i = 0; i < this.broker.getBrokerConfig().getReviveQueueNum(); i++) {
            this.popReviveThreads[i] = new PopReviveThread(broker, reviveTopic, i);
            this.popReviveThreads[i].setShouldRunPopRevive(broker.getBrokerConfig().getBrokerId() == 0);
        }
    }

    public void start() {
        this.popPollingService.start();
        this.notificationPollingService.start();
        this.popBufferMergeThread.start();
        this.queueLockManager.start();

        for (PopReviveThread popReviveThread : popReviveThreads) {
            popReviveThread.start();
        }
    }

    public void shutdown() {
        this.popPollingService.shutdown();
        this.notificationPollingService.shutdown();
        this.popBufferMergeThread.shutdown();
        this.queueLockManager.shutdown();

        for (PopReviveThread popReviveThread : popReviveThreads) {
            popReviveThread.shutdown();
        }
    }

    public final MessageExtBrokerInner buildCkMsg(final PopCheckPoint ck, final int reviveQid) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();

        msgInner.setTopic(reviveTopic);
        msgInner.setBody(JSON.toJSONString(ck).getBytes(DataConverter.CHARSET_UTF8));
        msgInner.setQueueId(reviveQid);
        msgInner.setTags(PopConstants.CK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.broker.getStoreHost());
        msgInner.setStoreHost(this.broker.getStoreHost());
        msgInner.setDeliverTimeMs(ck.getReviveTime() - PopConstants.ackTimeInterval);
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopKeyBuilder.genCkUniqueId(ck));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        return msgInner;
    }

    public void setReviveStatus(boolean shouldStart) {
        for (PopReviveThread popReviveThread : popReviveThreads) {
            popReviveThread.setShouldRunPopRevive(shouldStart);
        }
    }

    public boolean isReviveRunning() {
        for (PopReviveThread popReviveThread : popReviveThreads) {
            if (popReviveThread.isShouldRunPopRevive()) {
                return true;
            }
        }

        return false;
    }

    public ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>> getPopPollingMap() {
        return popPollingService.getPollingMap();
    }

    public void notifyLongPollingRequestIfNeed(String topic, String group, int queueId) {
        long popBufferOffset = this.getPopBufferMergeService().getLatestOffset(topic, group, queueId);
        long consumerOffset = this.broker.getConsumerOffsetManager().queryOffset(group, topic, queueId);
        long maxOffset = this.broker.getMessageStore().getMaxOffsetInQueue(topic, queueId);
        long offset = Math.max(popBufferOffset, consumerOffset);
        if (maxOffset <= offset) {
            return;
        }

        boolean notifySuccess = popPollingService.notifyMessageArriving(topic, group, -1);
        if (!notifySuccess) {
            // notify pop queue
            notifySuccess = popPollingService.notifyMessageArriving(topic, group, queueId);
        }

        notificationPollingService.notifyMessageArriving(topic, queueId);
        if (this.broker.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("notify long polling request. topic:{}, group:{}, queueId:{}, success:{}",
                topic, group, queueId, notifySuccess);
        }
    }

    public void notificationArriving(final String topic, final int queueId) {
        notificationPollingService.notifyMessageArrivingWithRetryTopic(topic, queueId);
    }

    public void popArriving(final String topic, final int queueId) {
        popPollingService.notifyMessageArrivingWithRetryTopic(topic, queueId);
    }

    public boolean popArriving(final String topic, final String cid, final int queueId) {
        return popPollingService.notifyMessageArriving(topic, cid, queueId);
    }

    public String getReviveTopic() {
        return reviveTopic;
    }

    public PopReviveThread[] getPopReviveServices() {
        return popReviveThreads;
    }

    public PopLongPollingThread getPopPollingService() {
        return popPollingService;
    }

    public PopBufferMergeThread getPopBufferMergeService() {
        return this.popBufferMergeThread;
    }

    public QueueLockManager getQueueLockManager() {
        return queueLockManager;
    }

    public PopLongPollingThread getNotificationPollingService() {
        return notificationPollingService;
    }
}
