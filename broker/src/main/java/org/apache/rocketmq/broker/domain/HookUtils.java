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
package org.apache.rocketmq.broker.domain;

import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.daemon.schedule.ScheduleMessageService;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageAccessor;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.domain.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.domain.topic.TopicValidator;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;
import org.apache.rocketmq.store.server.config.BrokerRole;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import static org.apache.rocketmq.store.domain.timer.model.TimerState.TIMER_TOPIC;

public class HookUtils {

    protected static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final AtomicLong PRINT_TIMES = new AtomicLong(0);

    /**
     * On Linux: The maximum length for a file name is 255 bytes.
     * The maximum combined length of both the file name and path name is 4096 bytes.
     * This length matches the PATH_MAX that is supported by the operating system.
     * The Unicode representation of a character can occupy several bytes,
     * so the maximum number of characters that comprises a path and file name can vary.
     * The actual limitation is the number of bytes in the path and file components,
     * which might correspond to an equal number of characters.
     */
    private static final Integer MAX_TOPIC_LENGTH = 255;

    public static PutMessageResult checkBeforePutMessage(Broker broker, final MessageExt msg) {
        if (broker.getMessageStore().isShutdown()) {
            LOG.warn("message store has shutdown, so putMessage is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (!broker.getMessageStoreConfig().isDuplicationEnable() && BrokerRole.SLAVE == broker.getMessageStoreConfig().getBrokerRole()) {
            long value = PRINT_TIMES.getAndIncrement();
            if ((value % 50000) == 0) {
                LOG.warn("message store is in slave mode, so putMessage is forbidden ");
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (!broker.getMessageStore().getRunningFlags().isWriteable()) {
            long value = PRINT_TIMES.getAndIncrement();
            if ((value % 50000) == 0) {
                LOG.warn("message store is not writeable, so putMessage is forbidden " + broker.getMessageStore().getRunningFlags().getFlagBits());
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        } else {
            PRINT_TIMES.set(0);
        }

        final byte[] topicData = msg.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
        boolean retryTopic = msg.getTopic() != null && msg.getTopic().startsWith(MQConstants.RETRY_GROUP_TOPIC_PREFIX);
        if (!retryTopic && topicData.length > Byte.MAX_VALUE) {
            LOG.warn("putMessage message topic[{}] length too long {}, but it is not supported by broker", msg.getTopic(), topicData.length);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (topicData.length > MAX_TOPIC_LENGTH) {
            LOG.warn("putMessage message topic[{}] length too long {}, but it is not supported by broker", msg.getTopic(), topicData.length);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (msg.getBody() == null) {
            LOG.warn("putMessage message topic[{}], but message body is null", msg.getTopic());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (broker.getMessageStore().isOSPageCacheBusy()) {
            return new PutMessageResult(PutMessageStatus.OS_PAGE_CACHE_BUSY, null);
        }
        return null;
    }

    public static PutMessageResult checkInnerBatch(Broker broker, final MessageExt msg) {
        if (msg.getProperties().containsKey(MessageConst.PROPERTY_INNER_NUM)
            && !MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)) {
            LOG.warn("[BUG]The message had property {} but is not an inner batch", MessageConst.PROPERTY_INNER_NUM);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)) {
            Optional<TopicConfig> topicConfig = Optional.ofNullable(broker.getTopicConfigManager().getTopicConfigTable().get(msg.getTopic()));
            if (!QueueTypeUtils.isBatchCq(topicConfig)) {
                LOG.error("[BUG]The message is an inner batch but cq type is not batch cq");
                return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
            }
        }

        return null;
    }

    public static PutMessageResult handleScheduleMessage(Broker broker, final MessageExtBrokerInner msg) {
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE && tranType != MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            return null;

        }
        // Delay Delivery
        if (msg.getDelayTimeLevel() > 0) {
            transformDelayLevelMessage(broker, msg);
        }

        if (isRolledTimerMessage(msg)) {
            return null;
        }

        if (!checkIfTimerMessage(msg)) {
            return null;
        }

        if (!broker.getMessageStoreConfig().isTimerWheelEnable()) {
            //wheel timer is not enabled, reject the message
            return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_NOT_ENABLE, null);
        }

        return transformTimerMessage(broker, msg);
    }

    private static boolean isRolledTimerMessage(MessageExtBrokerInner msg) {
        return TIMER_TOPIC.equals(msg.getTopic());
    }

    public static boolean checkIfTimerMessage(MessageExtBrokerInner msg) {
        if (msg.getDelayTimeLevel() > 0) {
            if (null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS)) {
                MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_TIMER_DELIVER_MS);
            }
            if (null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC)) {
                MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_TIMER_DELAY_SEC);
            }
            if (null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS)) {
                MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_TIMER_DELAY_MS);
            }
            return false;
            //return this.defaultMessageStore.getMessageStoreConfig().isTimerInterceptDelayLevel();
        }
        //double check
        if (TIMER_TOPIC.equals(msg.getTopic()) || null != msg.getProperty(MessageConst.PROPERTY_TIMER_OUT_MS)) {
            return false;
        }
        return null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS) || null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS) || null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC);
    }

    private static Long formatDelayTime(MessageExtBrokerInner msg) {
        Long deliverMs;
        try {
            if (msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null) {
                deliverMs = System.currentTimeMillis() + Long.parseLong(msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC)) * 1000;
            } else if (msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS) != null) {
                deliverMs = System.currentTimeMillis() + Long.parseLong(msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS));
            } else {
                deliverMs = Long.parseLong(msg.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS));
            }
        } catch (Exception e) {
            return null;
        }

        return deliverMs;
    }

    private static PutMessageResult transformTimerMessage(Broker broker, MessageExtBrokerInner msg) {
        //do transform
        int delayLevel = msg.getDelayTimeLevel();
        Long deliverMs = formatDelayTime(msg);
        if (deliverMs == null || deliverMs > System.currentTimeMillis()) {
            return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, null);
        }

        if (delayLevel <= 0 && deliverMs - System.currentTimeMillis() > broker.getMessageStoreConfig().getTimerMaxDelaySec() * 1000L) {
            return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, null);
        }

        int timerPrecisionMs = broker.getMessageStoreConfig().getTimerPrecisionMs();
        if (deliverMs % timerPrecisionMs == 0) {
            deliverMs -= timerPrecisionMs;
        } else {
            deliverMs = deliverMs / timerPrecisionMs * timerPrecisionMs;
        }

        if (broker.getTimerMessageStore().isReject(deliverMs)) {
            return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_FLOW_CONTROL, null);
        }

        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TIMER_OUT_MS, deliverMs + "");
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        msg.setTopic(TIMER_TOPIC);
        msg.setQueueId(0);

        return null;
    }

    public static void transformDelayLevelMessage(Broker broker, MessageExtBrokerInner msg) {

        if (msg.getDelayTimeLevel() > broker.getScheduleMessageService().getMaxDelayLevel()) {
            msg.setDelayTimeLevel(broker.getScheduleMessageService().getMaxDelayLevel());
        }

        // Backup real topic, queueId
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

        msg.setTopic(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC);
        msg.setQueueId(ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel()));
    }

    public static boolean sendMessageBack(Broker broker, List<MessageExt> msgList, String brokerName, String brokerAddr) {
        try {
            Iterator<MessageExt> it = msgList.iterator();
            while (it.hasNext()) {
                MessageExt msg = it.next();
                msg.setWaitStoreMsgOK(false);
                broker.getClusterClient().sendMessageToSpecificBroker(brokerAddr, brokerName, msg, "InnerSendMessageBackGroup", 3000);
                it.remove();
            }
        } catch (Exception e) {
            LOG.error("send message back to broker {} addr {} failed", brokerName, brokerAddr, e);
            return false;
        }
        return true;
    }
}
