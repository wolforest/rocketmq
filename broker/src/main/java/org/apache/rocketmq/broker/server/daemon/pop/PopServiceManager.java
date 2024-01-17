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
import org.apache.rocketmq.broker.server.longpolling.PopLongPollingService;
import org.apache.rocketmq.broker.server.longpolling.PopRequest;
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

    private final PopReviveService[] popReviveServices;
    private final PopBufferMergeService popBufferMergeService;

    private final PopLongPollingService popPollingService;
    private final PopLongPollingService notificationPollingService;


    public PopServiceManager(final Broker broker, PopMessageProcessor popMessageProcessor, NotificationProcessor notificationProcessor) {
        this.broker = broker;
        this.reviveTopic = KeyBuilder.buildClusterReviveTopic(this.broker.getBrokerConfig().getBrokerClusterName());

        this.popPollingService = new PopLongPollingService(broker, popMessageProcessor);
        this.notificationPollingService = new PopLongPollingService(broker, notificationProcessor);

        this.queueLockManager = new QueueLockManager(broker);
        this.popBufferMergeService = new PopBufferMergeService(this.broker);

        this.popReviveServices = new PopReviveService[this.broker.getBrokerConfig().getReviveQueueNum()];
        for (int i = 0; i < this.broker.getBrokerConfig().getReviveQueueNum(); i++) {
            this.popReviveServices[i] = new PopReviveService(broker, reviveTopic, i);
            this.popReviveServices[i].setShouldRunPopRevive(broker.getBrokerConfig().getBrokerId() == 0);
        }
    }

    public void start() {
        this.popPollingService.start();
        this.notificationPollingService.start();
        this.popBufferMergeService.start();
        this.queueLockManager.start();

        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.start();
        }
    }

    public void shutdown() {
        this.popPollingService.shutdown();
        this.notificationPollingService.shutdown();
        this.popBufferMergeService.shutdown();
        this.queueLockManager.shutdown();

        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.shutdown();
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
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.setShouldRunPopRevive(shouldStart);
        }
    }

    public boolean isReviveRunning() {
        for (PopReviveService popReviveService : popReviveServices) {
            if (popReviveService.isShouldRunPopRevive()) {
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

    public PopReviveService[] getPopReviveServices() {
        return popReviveServices;
    }

    public PopLongPollingService getPopPollingService() {
        return popPollingService;
    }

    public PopBufferMergeService getPopBufferMergeService() {
        return this.popBufferMergeService;
    }

    public QueueLockManager getQueueLockManager() {
        return queueLockManager;
    }

    public PopLongPollingService getNotificationPollingService() {
        return notificationPollingService;
    }
}
