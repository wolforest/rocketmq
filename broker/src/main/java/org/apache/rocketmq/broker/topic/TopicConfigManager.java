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
package org.apache.rocketmq.broker.topic;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.ImmutableMap;

import com.google.common.collect.Maps;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.app.config.ConfigManager;
import org.apache.rocketmq.common.app.KeyBuilder;
import org.apache.rocketmq.common.domain.topic.TopicAttributes;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.lang.attribute.Attribute;
import org.apache.rocketmq.common.lang.attribute.AttributeUtil;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.constant.PermName;
import org.apache.rocketmq.common.domain.sysflag.TopicSysFlag;
import org.apache.rocketmq.common.domain.topic.TopicValidator;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.utils.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.body.KVTable;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;

import static com.google.common.base.Preconditions.checkNotNull;

public class TopicConfigManager extends ConfigManager {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    private static final int SCHEDULE_TOPIC_QUEUE_NUM = 18;

    private transient final Lock topicConfigTableLock = new ReentrantLock();
    protected ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>(1024);
    private final DataVersion dataVersion = new DataVersion();
    protected transient BrokerController brokerController;

    public TopicConfigManager() {}

    public TopicConfigManager(BrokerController brokerController) {
        this(brokerController, true);
    }

    public TopicConfigManager(BrokerController brokerController, boolean init) {
        this.brokerController = brokerController;
        if (init) {
            init();
        }
    }

    protected void init() {
        addSystemTopic(TopicValidator.RMQ_SYS_SELF_TEST_TOPIC, 1, 1);

        if (this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
            addSystemTopic(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC,
                this.brokerController.getBrokerConfig().getDefaultTopicQueueNums(),
                this.brokerController.getBrokerConfig().getDefaultTopicQueueNums(),
                PermName.PERM_INHERIT | PermName.PERM_READ | PermName.PERM_WRITE);
        }

        addSystemTopic(TopicValidator.RMQ_SYS_BENCHMARK_TOPIC, 1024, 1024);

        addSystemTopic(this.brokerController.getBrokerConfig().getBrokerClusterName(), null, null,
            getPerm(this.brokerController.getBrokerConfig().isClusterTopicEnable()));

        addSystemTopic(this.brokerController.getBrokerConfig().getBrokerName(), 1, 1,
            getPerm(this.brokerController.getBrokerConfig().isBrokerTopicEnable()));

        addSystemTopic(TopicValidator.RMQ_SYS_OFFSET_MOVED_EVENT, 1, 1);

        addSystemTopic(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, SCHEDULE_TOPIC_QUEUE_NUM, SCHEDULE_TOPIC_QUEUE_NUM);

        if (this.brokerController.getBrokerConfig().isTraceTopicEnable()) {
            addSystemTopic(this.brokerController.getBrokerConfig().getMsgTraceTopicName(), 1, 1);
        }

        addSystemTopic(this.brokerController.getBrokerConfig().getBrokerClusterName() + "_" + MQConstants.REPLY_TOPIC_POSTFIX,
            1, 1);

        // PopAckConstants.REVIVE_TOPIC
        addSystemTopic(KeyBuilder.buildClusterReviveTopic(this.brokerController.getBrokerConfig().getBrokerClusterName()),
            this.brokerController.getBrokerConfig().getReviveQueueNum(),
            this.brokerController.getBrokerConfig().getReviveQueueNum());

        // sync broker member group topic
        addSystemTopic(TopicValidator.SYNC_BROKER_MEMBER_GROUP_PREFIX + this.brokerController.getBrokerConfig().getBrokerName(),
            1, 1, PermName.PERM_INHERIT);

        // TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC
        addSystemTopic(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC, 1,1);

        // TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC
        addSystemTopic(TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC, 1, 1);
    }

    private int getPerm(boolean readAndWrite) {
        int perm = PermName.PERM_INHERIT;
        if (readAndWrite) {
            perm |= PermName.PERM_READ | PermName.PERM_WRITE;
        }
        return perm;
    }

    private void addSystemTopic(String topic, Integer readQueueNums, Integer writeQueueNums) {
        addSystemTopic(topic, readQueueNums, writeQueueNums, null);
    }

    private void addSystemTopic(String topic, Integer readQueueNums, Integer writeQueueNums, Integer perm) {
        TopicConfig topicConfig = new TopicConfig(topic);
        TopicValidator.addSystemTopic(topic);

        if (readQueueNums != null) {
            topicConfig.setReadQueueNums(readQueueNums);
        }

        if (writeQueueNums != null) {
            topicConfig.setWriteQueueNums(writeQueueNums);
        }

        if (perm != null) {
            topicConfig.setPerm(perm);
        }

        putTopicConfig(topicConfig);
    }


    protected TopicConfig putTopicConfig(TopicConfig topicConfig) {
        return this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
    }

    protected TopicConfig getTopicConfig(String topicName) {
        return this.topicConfigTable.get(topicName);
    }

    protected TopicConfig removeTopicConfig(String topicName) {
        return this.topicConfigTable.remove(topicName);
    }

    public TopicConfig selectTopicConfig(final String topic) {
        return getTopicConfig(topic);
    }

    public TopicConfig createTopicInSendMessageMethod(final String topic, final String defaultTopic,
        final String remoteAddress, final int clientDefaultTopicQueueNums, final int topicSysFlag) {
        TopicConfig topicConfig = null;
        boolean createNew = false;

        try {
            if (this.topicConfigTableLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = getTopicConfig(topic);
                    if (topicConfig != null) {
                        return topicConfig;
                    }

                    TopicConfig defaultTopicConfig = getTopicConfig(defaultTopic);
                    if (defaultTopicConfig != null) {
                        if (defaultTopic.equals(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC)) {
                            if (!this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                                defaultTopicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
                            }
                        }

                        if (PermName.isInherited(defaultTopicConfig.getPerm())) {
                            topicConfig = new TopicConfig(topic);

                            int queueNums = Math.min(clientDefaultTopicQueueNums, defaultTopicConfig.getWriteQueueNums());

                            if (queueNums < 0) {
                                queueNums = 0;
                            }

                            topicConfig.setReadQueueNums(queueNums);
                            topicConfig.setWriteQueueNums(queueNums);
                            int perm = defaultTopicConfig.getPerm();
                            perm &= ~PermName.PERM_INHERIT;
                            topicConfig.setPerm(perm);
                            topicConfig.setTopicSysFlag(topicSysFlag);
                            topicConfig.setTopicFilterType(defaultTopicConfig.getTopicFilterType());
                        } else {
                            log.warn("Create new topic failed, because the default topic[{}] has no perm [{}] producer:[{}]",
                                defaultTopic, defaultTopicConfig.getPerm(), remoteAddress);
                        }
                    } else {
                        log.warn("Create new topic failed, because the default topic[{}] not exist. producer:[{}]",
                            defaultTopic, remoteAddress);
                    }

                    if (topicConfig != null) {
                        log.info("Create new topic by default topic:[{}] config:[{}] producer:[{}]",
                            defaultTopic, topicConfig, remoteAddress);

                        putTopicConfig(topicConfig);

                        long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
                        dataVersion.nextVersion(stateMachineVersion);

                        createNew = true;

                        this.persist();
                    }
                } finally {
                    this.topicConfigTableLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageMethod exception", e);
        }

        if (createNew) {
            registerBrokerData(topicConfig);
        }

        return topicConfig;
    }

    public TopicConfig createTopicIfAbsent(TopicConfig topicConfig) {
        return createTopicIfAbsent(topicConfig, true);
    }

    /**
     * useless method
     */
    public TopicConfig createTopicIfAbsent(TopicConfig topicConfig, boolean register) {
        boolean createNew = false;
        if (topicConfig == null) {
            throw new NullPointerException("TopicConfig");
        }
        if (StringUtils.isEmpty(topicConfig.getTopicName())) {
            throw new IllegalArgumentException("TopicName");
        }

        try {
            if (this.topicConfigTableLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    TopicConfig existedTopicConfig = getTopicConfig(topicConfig.getTopicName());
                    if (existedTopicConfig != null) {
                        return existedTopicConfig;
                    }
                    log.info("Create new topic [{}] config:[{}]", topicConfig.getTopicName(), topicConfig);
                    putTopicConfig(topicConfig);
                    long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
                    dataVersion.nextVersion(stateMachineVersion);
                    createNew = true;
                    this.persist();
                } finally {
                    this.topicConfigTableLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicIfAbsent ", e);
        }
        if (createNew && register) {
            registerBrokerData(topicConfig);
        }
        return getTopicConfig(topicConfig.getTopicName());
    }

    public TopicConfig createTopicInSendMessageBackMethod(String topic, int clientDefaultTopicQueueNums, int perm, int topicSysFlag) {
        return createTopicInSendMessageBackMethod(topic, clientDefaultTopicQueueNums, perm, false, topicSysFlag);
    }

    public TopicConfig createTopicInSendMessageBackMethod(String topic, int clientDefaultTopicQueueNums, int perm, boolean isOrder, int topicSysFlag) {
        TopicConfig topicConfig = getTopicConfig(topic);
        if (topicConfig != null) {
            if (isOrder != topicConfig.isOrder()) {
                topicConfig.setOrder(isOrder);
                this.updateTopicConfig(topicConfig);
            }
            return topicConfig;
        }

        boolean createNew = false;

        try {
            if (!this.topicConfigTableLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                return null;
            }

            try {
                topicConfig = getTopicConfig(topic);
                if (topicConfig != null) {
                    return topicConfig;
                }

                topicConfig = new TopicConfig(topic);
                topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                topicConfig.setPerm(perm);
                topicConfig.setTopicSysFlag(topicSysFlag);
                topicConfig.setOrder(isOrder);

                log.info("create new topic {}", topicConfig);
                putTopicConfig(topicConfig);
                createNew = true;

                long stateMachineVersion = brokerController.getMessageStore() != null
                    ? brokerController.getMessageStore().getStateMachineVersion()
                    : 0;
                dataVersion.nextVersion(stateMachineVersion);

                this.persist();
            } finally {
                this.topicConfigTableLock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageBackMethod exception", e);
        }

        if (createNew) {
            registerBrokerData(topicConfig);
        }

        return topicConfig;
    }

    public TopicConfig createTopicOfTranCheckMaxTime(final int clientDefaultTopicQueueNums, final int perm) {
        TopicConfig topicConfig = getTopicConfig(TopicValidator.RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
        if (topicConfig != null)
            return topicConfig;

        boolean createNew = false;

        try {
            if (this.topicConfigTableLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = getTopicConfig(TopicValidator.RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
                    if (topicConfig != null)
                        return topicConfig;

                    topicConfig = new TopicConfig(TopicValidator.RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
                    topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setPerm(perm);
                    topicConfig.setTopicSysFlag(0);

                    log.info("create new topic {}", topicConfig);
                    putTopicConfig(topicConfig);
                    createNew = true;
                    long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
                    dataVersion.nextVersion(stateMachineVersion);
                    this.persist();
                } finally {
                    this.topicConfigTableLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("create TRANS_CHECK_MAX_TIME_TOPIC exception", e);
        }

        if (createNew) {
            registerBrokerData(topicConfig);
        }

        return topicConfig;
    }

    public void updateTopicUnitFlag(final String topic, final boolean unit) {
        TopicConfig topicConfig = getTopicConfig(topic);
        if (topicConfig == null) {
            return;
        }

        int oldTopicSysFlag = topicConfig.getTopicSysFlag();
        if (unit) {
            topicConfig.setTopicSysFlag(TopicSysFlag.setUnitFlag(oldTopicSysFlag));
        } else {
            topicConfig.setTopicSysFlag(TopicSysFlag.clearUnitFlag(oldTopicSysFlag));
        }

        log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag={}", oldTopicSysFlag,
            topicConfig.getTopicSysFlag());

        putTopicConfig(topicConfig);

        long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
        dataVersion.nextVersion(stateMachineVersion);

        this.persist();

        registerBrokerData(topicConfig);
    }

    public void updateTopicUnitSubFlag(final String topic, final boolean hasUnitSub) {
        TopicConfig topicConfig = getTopicConfig(topic);
        if (topicConfig == null) {
            return;
        }

        int oldTopicSysFlag = topicConfig.getTopicSysFlag();
        if (hasUnitSub) {
            topicConfig.setTopicSysFlag(TopicSysFlag.setUnitSubFlag(oldTopicSysFlag));
        } else {
            topicConfig.setTopicSysFlag(TopicSysFlag.clearUnitSubFlag(oldTopicSysFlag));
        }

        log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag={}", oldTopicSysFlag,
            topicConfig.getTopicSysFlag());

        putTopicConfig(topicConfig);

        long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
        dataVersion.nextVersion(stateMachineVersion);

        this.persist();

        registerBrokerData(topicConfig);
    }

    public void updateTopicConfig(final TopicConfig topicConfig) {
        checkNotNull(topicConfig, "topicConfig shouldn't be null");

        Map<String, String> newAttributes = request(topicConfig);
        Map<String, String> currentAttributes = current(topicConfig.getTopicName());


        Map<String, String> finalAttributes = AttributeUtil.alterCurrentAttributes(
            this.topicConfigTable.get(topicConfig.getTopicName()) == null,
            TopicAttributes.ALL,
            ImmutableMap.copyOf(currentAttributes),
            ImmutableMap.copyOf(newAttributes));

        topicConfig.setAttributes(finalAttributes);

        TopicConfig old = putTopicConfig(topicConfig);
        if (old != null) {
            log.info("update topic config, old:[{}] new:[{}]", old, topicConfig);
        } else {
            log.info("create new topic [{}]", topicConfig);
        }

        long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
        dataVersion.nextVersion(stateMachineVersion);

        this.persist(topicConfig.getTopicName(), topicConfig);
    }

    public void updateOrderTopicConfig(final KVTable orderKVTableFromNs) {
        if (orderKVTableFromNs == null || orderKVTableFromNs.getTable() == null) {
            return;
        }

        boolean isChange = false;
        Set<String> orderTopics = orderKVTableFromNs.getTable().keySet();
        for (String topic : orderTopics) {
            TopicConfig topicConfig = getTopicConfig(topic);
            if (topicConfig == null || topicConfig.isOrder()) {
                continue;
            }

            topicConfig.setOrder(true);
            isChange = true;
            log.info("update order topic config, topic={}, order={}", topic, true);
        }

        // We don't have a mandatory rule to maintain the validity of order conf in NameServer,
        // so we may overwrite the order field mistakenly.
        // To avoid the above case, we comment the below codes, please use mqadmin API to update
        // the order filed.
        /*for (Map.Entry<String, TopicConfig> entry : this.topicConfigTable.entrySet()) {
            String topic = entry.getKey();
            if (!orderTopics.contains(topic)) {
                TopicConfig topicConfig = entry.getValue();
                if (topicConfig.isOrder()) {
                    topicConfig.setOrder(false);
                    isChange = true;
                    log.info("update order topic config, topic={}, order={}", topic, false);
                }
            }
        }*/

        if (isChange) {
            long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
            dataVersion.nextVersion(stateMachineVersion);
            this.persist();
        }
    }

    // make it testable
    public Map<String, Attribute> allAttributes() {
        return TopicAttributes.ALL;
    }

    public boolean isOrderTopic(final String topic) {
        TopicConfig topicConfig = getTopicConfig(topic);
        if (topicConfig == null) {
            return false;
        } else {
            return topicConfig.isOrder();
        }
    }

    public void deleteTopicConfig(final String topic) {
        TopicConfig old = removeTopicConfig(topic);
        if (old == null) {
            log.warn("delete topic config failed, topic: {} not exists", topic);
            return;
        }

        log.info("delete topic config OK, topic: {}", old);
        long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
        dataVersion.nextVersion(stateMachineVersion);
        this.persist();
    }

    public TopicConfigSerializeWrapper buildTopicConfigSerializeWrapper() {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        DataVersion dataVersionCopy = new DataVersion();
        dataVersionCopy.assignNewOne(this.dataVersion);
        topicConfigSerializeWrapper.setDataVersion(dataVersionCopy);
        return topicConfigSerializeWrapper;
    }

    public TopicConfigAndMappingSerializeWrapper buildSerializeWrapper(final ConcurrentMap<String, TopicConfig> topicConfigTable) {
        return buildSerializeWrapper(topicConfigTable, Maps.newHashMap());
    }

    public TopicConfigAndMappingSerializeWrapper buildSerializeWrapper(
        final ConcurrentMap<String, TopicConfig> topicConfigTable,
        final Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap
    ) {
        TopicConfigAndMappingSerializeWrapper topicConfigWrapper = new TopicConfigAndMappingSerializeWrapper();
        topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        topicConfigWrapper.setTopicQueueMappingInfoMap(topicQueueMappingInfoMap);
        topicConfigWrapper.setDataVersion(this.getDataVersion());
        if (this.brokerController.getBrokerConfig().isEnableSplitRegistration()) {
            this.getDataVersion().nextVersion();
        }
        return topicConfigWrapper;
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getTopicConfigPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString == null) {
            return;
        }

        TopicConfigSerializeWrapper topicConfigSerializeWrapper = TopicConfigSerializeWrapper.fromJson(jsonString, TopicConfigSerializeWrapper.class);
        if (topicConfigSerializeWrapper == null) {
            return;
        }

        this.topicConfigTable.putAll(topicConfigSerializeWrapper.getTopicConfigTable());
        this.dataVersion.assignNewOne(topicConfigSerializeWrapper.getDataVersion());
        this.printLoadDataWhenFirstBoot(topicConfigSerializeWrapper);
    }

    public String encode(final boolean prettyFormat) {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return topicConfigSerializeWrapper.toJson(prettyFormat);
    }

    private void printLoadDataWhenFirstBoot(final TopicConfigSerializeWrapper tcs) {
        for (Entry<String, TopicConfig> next : tcs.getTopicConfigTable().entrySet()) {
            log.info("load exist local topic, {}", next.getValue().toString());
        }
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setTopicConfigTable(
        ConcurrentMap<String, TopicConfig> topicConfigTable) {
        this.topicConfigTable = topicConfigTable;
    }

    public ConcurrentMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }

    private Map<String, String> request(TopicConfig topicConfig) {
        return topicConfig.getAttributes() == null ? new HashMap<>() : topicConfig.getAttributes();
    }

    private Map<String, String> current(String topic) {
        TopicConfig topicConfig = getTopicConfig(topic);
        if (topicConfig == null) {
            return new HashMap<>();
        }

        Map<String, String> attributes = topicConfig.getAttributes();
        if (attributes == null) {
            return new HashMap<>();
        } else {
            return attributes;
        }
    }

    private void registerBrokerData(TopicConfig topicConfig) {
        if (brokerController.getBrokerConfig().isEnableSingleTopicRegister()) {
            this.brokerController.getBrokerServiceRegistry().registerSingleTopicAll(topicConfig);
        } else {
            this.brokerController.getBrokerServiceRegistry().registerIncrementBrokerData(topicConfig, dataVersion);
        }
    }

    public boolean containsTopic(String topic) {
        return topicConfigTable.containsKey(topic);
    }
}
