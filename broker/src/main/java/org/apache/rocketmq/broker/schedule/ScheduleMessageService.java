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
package org.apache.rocketmq.broker.schedule;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.topic.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;

public class ScheduleMessageService extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long WAIT_FOR_SHUTDOWN = 5000L;
    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
        new ConcurrentHashMap<>(32);

    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
        new ConcurrentHashMap<>(32);
    private final AtomicBoolean started = new AtomicBoolean(false);

    private ScheduledExecutorService deliverExecutorService;
    private int maxDelayLevel;
    private DataVersion dataVersion = new DataVersion();
    private boolean enableAsyncDeliver = false;


    private ScheduledExecutorService handleExecutorService;
    private final ScheduledExecutorService scheduledPersistService;

    private final Map<Integer /* level */, LinkedBlockingQueue<PutResultProcess>> deliverPendingTable =
        new ConcurrentHashMap<>(32);

    private final BrokerController brokerController;
    private final transient AtomicLong versionChangeCounter = new AtomicLong(0);

    public ScheduleMessageService(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.enableAsyncDeliver = brokerController.getMessageStoreConfig().isEnableScheduleAsyncDeliver();
        scheduledPersistService = ThreadUtils.newScheduledThreadPool(1,
            new ThreadFactoryImpl("ScheduleMessageServicePersistThread", true, brokerController.getBrokerConfig()));
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        for (Map.Entry<Integer, Long> next : this.offsetTable.entrySet()) {
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    public void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
        if (versionChangeCounter.incrementAndGet() % brokerController.getBrokerConfig().getDelayOffsetUpdateVersionStep() == 0) {
            long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
            dataVersion.nextVersion(stateMachineVersion);
        }
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }

        this.load();
        this.initExecutorService();
        this.startSchedule();
        this.startPersistentService();
    }

    private void initExecutorService() {
        this.deliverExecutorService = ThreadUtils.newScheduledThreadPool(this.maxDelayLevel, new ThreadFactoryImpl("ScheduleMessageTimerThread_"));
        if (this.enableAsyncDeliver) {
            this.handleExecutorService = ThreadUtils.newScheduledThreadPool(this.maxDelayLevel, new ThreadFactoryImpl("ScheduleMessageExecutorHandleThread_"));
        }
    }

    private void startSchedule() {
        for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
            Integer level = entry.getKey();
            Long timeDelay = entry.getValue();
            Long offset = this.offsetTable.get(level);
            if (null == offset) {
                offset = 0L;
            }

            if (timeDelay == null) {
                continue;
            }

            if (this.enableAsyncDeliver) {
                this.handleExecutorService.schedule(new HandlePutResultTask(this, level), FIRST_DELAY_TIME, TimeUnit.MILLISECONDS);
            }
            this.deliverExecutorService.schedule(new DeliverDelayedMessageTimerTask(this, level, offset), FIRST_DELAY_TIME, TimeUnit.MILLISECONDS);
        }
    }

    private void startPersistentService() {
        scheduledPersistService.scheduleAtFixedRate(() -> {
            try {
                ScheduleMessageService.this.persist();
            } catch (Throwable e) {
                log.error("scheduleAtFixedRate flush exception", e);
            }
        }, 10000, this.brokerController.getMessageStoreConfig().getFlushDelayOffsetInterval(), TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        stop();
        ThreadUtils.shutdown(scheduledPersistService);
    }

    public boolean stop() {
        if (!this.started.compareAndSet(true, false) || null == this.deliverExecutorService) {
            return true;
        }

        stopDeliverExecutorService();
        stopHandleExecutorService();
        logDeliverPendingTable();

        this.persist();
        return true;
    }

    private void stopDeliverExecutorService() {
        this.deliverExecutorService.shutdown();
        try {
            this.deliverExecutorService.awaitTermination(WAIT_FOR_SHUTDOWN, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("deliverExecutorService awaitTermination error", e);
        }
    }

    private void stopHandleExecutorService() {
        if (this.handleExecutorService == null) {
            return;
        }

        this.handleExecutorService.shutdown();
        try {
            this.handleExecutorService.awaitTermination(WAIT_FOR_SHUTDOWN, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("handleExecutorService awaitTermination error", e);
        }
    }

    private void logDeliverPendingTable() {
        for (int i = 1; i <= this.deliverPendingTable.size(); i++) {
            log.warn("deliverPendingTable level: {}, size: {}", i, this.deliverPendingTable.get(i).size());
        }
    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public boolean load() {
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        result = result && this.correctDelayOffset();
        return result;
    }
    
    public boolean loadWhenSyncDelayOffset() {
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        return result;
    }

    public boolean correctDelayOffset() {
        try {
            for (int delayLevel : delayLevelTable.keySet()) {
                ConsumeQueueInterface cq =
                    brokerController.getMessageStore().getConsumeQueueStore().findOrCreateConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                        delayLevel2QueueId(delayLevel));
                Long currentDelayOffset = offsetTable.get(delayLevel);
                if (currentDelayOffset == null || cq == null) {
                    continue;
                }
                long correctDelayOffset = currentDelayOffset;
                long cqMinOffset = cq.getMinOffsetInQueue();
                long cqMaxOffset = cq.getMaxOffsetInQueue();
                if (currentDelayOffset < cqMinOffset) {
                    correctDelayOffset = cqMinOffset;
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                        currentDelayOffset, cqMinOffset, cqMaxOffset, cq.getQueueId());
                }

                if (currentDelayOffset > cqMaxOffset) {
                    correctDelayOffset = cqMaxOffset;
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                        currentDelayOffset, cqMinOffset, cqMaxOffset, cq.getQueueId());
                }
                if (correctDelayOffset != currentDelayOffset) {
                    log.error("correct delay offset [ delayLevel {} ] from {} to {}", delayLevel, currentDelayOffset, correctDelayOffset);
                    offsetTable.put(delayLevel, correctDelayOffset);
                }
            }
        } catch (Exception e) {
            log.error("correctDelayOffset exception", e);
            return false;
        }
        return true;
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.brokerController.getMessageStore().getMessageStoreConfig()
            .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString == null) {
            return;
        }

        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
        if (delayOffsetSerializeWrapper == null) {
            return;
        }

        this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
        // For compatible
        if (delayOffsetSerializeWrapper.getDataVersion() != null) {
            this.dataVersion.assignNewOne(delayOffsetSerializeWrapper.getDataVersion());
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        delayOffsetSerializeWrapper.setDataVersion(this.dataVersion);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        String levelString = this.brokerController.getMessageStoreConfig().getMessageDelayLevel();
        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                long delayTimeMillis = tu * num;
                this.delayLevelTable.put(level, delayTimeMillis);
                if (this.enableAsyncDeliver) {
                    this.deliverPendingTable.put(level, new LinkedBlockingQueue<>());
                }
            }
        } catch (Exception e) {
            log.error("parse message delay level failed. messageDelayLevel = {}", levelString, e);
            return false;
        }

        return true;
    }

    public MessageExtBrokerInner messageTimeUp(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
            MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_TIMER_DELIVER_MS);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_TIMER_DELAY_SEC);

        msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

        String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
        int queueId = Integer.parseInt(queueIdStr);
        msgInner.setQueueId(queueId);

        return msgInner;
    }



    public ConcurrentMap<Integer, Long> getOffsetTable() {
        return offsetTable;
    }

    public ConcurrentMap<Integer, Long> getDelayLevelTable() {
        return delayLevelTable;
    }

    public boolean isEnableAsyncDeliver() {
        return enableAsyncDeliver;
    }

    public ScheduledExecutorService getDeliverExecutorService() {
        return deliverExecutorService;
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }

    public Map<Integer, LinkedBlockingQueue<PutResultProcess>> getDeliverPendingTable() {
        return deliverPendingTable;
    }

    public ScheduledExecutorService getHandleExecutorService() {
        return handleExecutorService;
    }
}
