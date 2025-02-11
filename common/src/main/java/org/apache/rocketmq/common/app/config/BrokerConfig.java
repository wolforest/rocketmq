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
package org.apache.rocketmq.common.app.config;

import org.apache.rocketmq.common.app.BrokerIdentity;
import org.apache.rocketmq.common.app.metrics.MetricsExporterType;
import org.apache.rocketmq.common.lang.annotation.ImportantField;
import org.apache.rocketmq.common.domain.constant.PermName;
import org.apache.rocketmq.common.domain.message.MessageRequestMode;
import org.apache.rocketmq.common.domain.topic.TopicValidator;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.utils.NameServerAddressUtils;
import org.apache.rocketmq.common.utils.NetworkUtils;

import java.util.concurrent.TimeUnit;

public class BrokerConfig extends BrokerIdentity {

    private String brokerConfigPath = null;

    private String rocketmqHome = System.getProperty(MQConstants.ROCKETMQ_HOME_PROPERTY, System.getenv(MQConstants.ROCKETMQ_HOME_ENV));

    /**
     * name server related setting
     * if namesrvAddr is not blank:
     *     fetchNamesrvAddrByAddressServer is useless
     * else if fetchNameSrvAddrByDnsLookup is true
     *     broker will load name server info by dns
     * else
     *     broker will load name server info by nameSrvAddr
     *
     */
    @ImportantField
    private String namesrvAddr = System.getProperty(NameServerAddressUtils.NAMESRV_ADDR_PROPERTY, System.getenv(NameServerAddressUtils.NAMESRV_ADDR_ENV));

    @ImportantField
    private boolean fetchNameSrvAddrByDnsLookup = false;

    @ImportantField
    private boolean fetchNamesrvAddrByAddressServer = false;

    /**
     * Listen port for single broker
     */
    @ImportantField
    private int listenPort = 6888;

    @ImportantField
    private String brokerIP1 = NetworkUtils.getLocalAddress();
    private String brokerIP2 = NetworkUtils.getLocalAddress();

    @ImportantField
    private boolean recoverConcurrently = false;

    /**
     * broker permission
     * default: Readable and writable
     */
    private int brokerPermission = PermName.PERM_READ | PermName.PERM_WRITE;

    /**
     * default consume queue nums per topic
     * default: 8
     *
     * @renamed from defaultTopicQueueNums to queueNumPerTopic
     */
    private int defaultTopicQueueNums = 8;

    /**
     * should be false in production env
     */
    @ImportantField
    private boolean autoCreateTopicEnable = true;

    /**
     * can cluster name be used as topic name
     * default: true
     */
    private boolean clusterTopicEnable = true;

    /**
     * can broker name be used as topic name
     */
    private boolean brokerTopicEnable = true;

    /**
     * should be true in production env
     */
    @ImportantField
    private boolean autoCreateSubscriptionGroup = true;

    /**
     * message store plugin
     * default: blank string - no plugin
     */
    private String messageStorePlugIn = "";

    /**
     * @TODO move it out of Broker Config
     */
    private static final int PROCESSOR_NUMBER = Runtime.getRuntime().availableProcessors();

    /**
     * on-off switch for message tracing
     */
    private boolean traceOn = true;
    /**
     *
     */
    @ImportantField
    private String msgTraceTopicName = TopicValidator.RMQ_SYS_TRACE_TOPIC;
    /**
     * on-off switch whether special tracing topic was used
     * if you want to enable message tracing
     * you need set traceOn and traceTopicEnable both true
     *
     * @renamed from traceTopicEnable to useTraceTopic
     */
    @ImportantField
    private boolean traceTopicEnable = false;

    /**
     * thread numbers for send message thread pool.
     */
    private int sendMessageThreadPoolNums = Math.min(PROCESSOR_NUMBER, 4);
    private int putMessageFutureThreadPoolNums = Math.min(PROCESSOR_NUMBER, 4);
    private int pullMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    private int litePullMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    private int ackMessageThreadPoolNums = 3;
    private int processReplyMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    private int queryMessageThreadPoolNums = 8 + PROCESSOR_NUMBER;

    private int adminBrokerThreadPoolNums = 16;
    private int clientManageThreadPoolNums = 32;
    private int consumerManageThreadPoolNums = 32;
    private int loadBalanceProcessorThreadPoolNums = 32;
    private int heartbeatThreadPoolNums = Math.min(32, PROCESSOR_NUMBER);
    private int recoverThreadPoolNums = 32;

    /**
     * Thread numbers for EndTransactionProcessor
     */
    private int endTransactionThreadPoolNums = Math.max(8 + PROCESSOR_NUMBER * 2, sendMessageThreadPoolNums * 4);

    /**
     * interval of consume offset storage
     */
    private int flushConsumerOffsetInterval = 1000 * 5;

    /**
     * useless
     */
    @Deprecated
    private int flushConsumerOffsetHistoryInterval = 1000 * 60;

    /**
     * whether reject transaction message
     * default: false
     */
    @ImportantField
    @Deprecated
    private boolean rejectTransactionMessage = false;

    private int sendThreadPoolQueueCapacity = 10000;
    private int putThreadPoolQueueCapacity = 10000;
    private int pullThreadPoolQueueCapacity = 100000;
    private int litePullThreadPoolQueueCapacity = 100000;
    private int ackThreadPoolQueueCapacity = 100000;
    private int replyThreadPoolQueueCapacity = 10000;
    private int queryThreadPoolQueueCapacity = 20000;
    private int clientManagerThreadPoolQueueCapacity = 1000000;
    private int consumerManagerThreadPoolQueueCapacity = 1000000;
    private int heartbeatThreadPoolQueueCapacity = 50000;
    private int endTransactionPoolQueueCapacity = 100000;
    private int adminBrokerThreadPoolQueueCapacity = 10000;
    private int loadBalanceThreadPoolQueueCapacity = 100000;

    private boolean longPollingEnable = true;

    private long shortPollingTimeMills = 1000;

    private boolean notifyConsumerIdsChangedEnable = true;

    /**
     * useless now
     */
    private boolean highSpeedMode = false;

    private int commercialBaseCount = 1;

    private int commercialSizePerMsg = 4 * 1024;

    private boolean accountStatsEnable = true;
    private boolean accountStatsPrintZeroValues = true;

    private boolean transferMsgByHeap = true;

    private String regionId = MQConstants.DEFAULT_TRACE_REGION_ID;
    private int registerBrokerTimeoutMills = 24000;

    private int sendHeartbeatTimeoutMillis = 1000;

    private boolean slaveReadEnable = false;

    /**
     * should be false in production env
     */
    private boolean disableConsumeIfConsumerReadSlowly = false;
    private long consumerFallbehindThreshold = 1024L * 1024 * 1024 * 16;

    /**
     * should be true,
     */
    private boolean brokerFastFailureEnable = true;
    private long waitTimeMillsInSendQueue = 200;
    private long waitTimeMillsInPullQueue = 5 * 1000;
    private long waitTimeMillsInLitePullQueue = 5 * 1000;
    private long waitTimeMillsInHeartbeatQueue = 31 * 1000;
    private long waitTimeMillsInTransactionQueue = 3 * 1000;
    private long waitTimeMillsInAckQueue = 3000;

    private long startAcceptSendRequestTimeStamp = 0L;



    // Switch of filter bit map calculation.
    // If switch on:
    // 1. Calculate filter bit map when construct queue.
    // 2. Filter bit map will be saved to consume queue extend file if allowed.
    private boolean enableCalcFilterBitMap = false;

    //Reject the pull consumer instance to pull messages from broker.
    //this property is used while receiving heartbeat request
    private boolean rejectPullConsumerEnable = false;

    // Expect num of consumers will use filter.
    // used by bloom filer with maxErrorRateOfBloomFilter
    private int expectConsumerNumUseFilter = 32;

    // Error rate of bloom filter, 1~100.
    private int maxErrorRateOfBloomFilter = 20;

    //how long to clean filter data after dead.Default: 24h
    private long filterDataCleanTimeSpan = 24 * 3600 * 1000;

    // whether to do filter while retry.
    private boolean filterSupportRetry = false;

    /**
     * whether property filter is supported
     * to use SQL92 filter, this must be set true
     */
    private boolean enablePropertyFilter = false;

    private boolean compressedRegister = false;

    private boolean forceRegister = true;

    /**
     * This configurable item defines interval of topics registration of broker to name server. Allowing values are
     * between 10,000 and 60,000 milliseconds.
     */
    private int registerNameServerPeriod = 1000 * 30;

    /**
     * the interval to send heartbeat to name server for liveness detection.
     */
    private int brokerHeartbeatInterval = 1000;

    /**
     * How long the broker will be considered as inactive by nameserver since last heartbeat. Effective only if
     * enableSlaveActingMaster is true
     */
    private long brokerNotActiveTimeoutMillis = 10 * 1000;

    private boolean enableNetWorkFlowControl = false;

    private boolean enableBroadcastOffsetStore = true;

    private long broadcastOffsetExpireSecond = 2 * 60;

    private long broadcastOffsetExpireMaxSecond = 5 * 60;

    private int popPollingSize = 1024;
    private int popPollingMapSize = 100000;
    // 20w cost 200M heap memory.
    private long maxPopPollingSize = 100000;
    private int reviveQueueNum = 8;
    private long reviveInterval = 1000;
    private long reviveMaxSlow = 3;
    private long reviveScanTime = 10000;

    /**
     * if true: will mock checkPoint for ackMsg
     */
    private boolean enableSkipLongAwaitingAck = false;
    private long reviveAckWaitMs = TimeUnit.MINUTES.toMillis(3);
    private boolean enablePopLog = false;

    /**
     * switch of pop buffer merge feature
     * default value should be true
     */
    private boolean enablePopBufferMerge = false;
    private int popCkStayBufferTime = 10 * 1000;
    private int popCkStayBufferTimeOut = 3 * 1000;
    /**
     * max check point number store in memory
     */
    private int popCkMaxBufferSize = 200000;
    private int popCkOffsetMaxQueueSize = 20000;
    private boolean enablePopBatchAck = false;
    private boolean enableNotifyAfterPopOrderLockRelease = true;
    private boolean initPopOffsetByCheckMsgInMem = true;

    // read message from pop retry topic v1, for the compatibility, will be removed in the future version
    private boolean retrieveMessageFromPopRetryTopicV1 = true;
    private boolean enableRetryTopicV2 = false;

    private boolean realTimeNotifyConsumerChange = true;

    private boolean litePullMessageEnable = true;

    // The period to sync broker member group from namesrv, default value is 1 second
    private int syncBrokerMemberGroupPeriod = 1000;

    /**
     * the interval of pulling topic information from the named server
     */
    private long loadBalancePollNameServerInterval = 1000 * 30;

    /**
     * the interval of cleaning
     */
    private int cleanOfflineBrokerInterval = 1000 * 30;

    private boolean serverLoadBalancerEnable = true;

    private MessageRequestMode defaultMessageRequestMode = MessageRequestMode.PULL;

    private int defaultPopShareQueueNum = -1;

    /**
     * The minimum time of the transactional message  to be checked firstly, one message only exceed this time interval
     * that can be checked.
     */
    @ImportantField
    private long transactionTimeOut = 6 * 1000;

    /**
     * The maximum number of times the message was checked, if exceed this value, this message will be discarded.
     */
    @ImportantField
    private int transactionCheckMax = 15;

    /**
     * Transaction message check interval.
     */
    @ImportantField
    private long transactionCheckInterval = 30 * 1000;

    private long transactionMetricFlushInterval = 3 * 1000;

    /**
     * transaction batch op message
     */
    private int transactionOpMsgMaxSize = 4096;

    private int transactionOpBatchInterval = 3000;

    /**
     * Acl feature switch
     */
    @ImportantField
    private boolean aclEnable = false;

    private boolean storeReplyMessageEnable = true;

    private boolean enableDetailStat = true;

    private boolean autoDeleteUnusedStats = false;

    /**
     * Whether to distinguish log paths when multiple brokers are deployed on the same machine
     */
    private boolean isolateLogEnable = false;

    private long forwardTimeout = 3 * 1000;

    /**
     * Slave will act master when failover. For example, if master down, timer or transaction message which is expired in slave will
     * put to master (master of the same process in broker container mode or other masters in cluster when enableFailoverRemotingActing is true)
     * when enableSlaveActingMaster is true
     */
    private boolean enableSlaveActingMaster = false;

    private boolean enableRemoteEscape = false;

    private boolean skipPreOnline = false;

    private boolean asyncSendEnable = true;

    private boolean useServerSideResetOffset = true;

    private long consumerOffsetUpdateVersionStep = 500;

    private long delayOffsetUpdateVersionStep = 200;

    /**
     * Whether to lock quorum replicas.
     *
     * True: need to lock quorum replicas succeed. False: only need to lock one replica succeed.
     */
    private boolean lockInStrictMode = false;

    private boolean compatibleWithOldNameSrv = true;

    /**
     * Is startup controller mode, which support auto switch broker's role.
     */
    private boolean enableControllerMode = false;

    private String controllerAddr = "";

    private boolean fetchControllerAddrByDnsLookup = false;

    private long syncBrokerMetadataPeriod = 5 * 1000;

    private long checkSyncStateSetPeriod = 5 * 1000;

    private long syncControllerMetadataPeriod = 10 * 1000;

    private long controllerHeartBeatTimeoutMills = 10 * 1000;

    private boolean validateSystemTopicWhenUpdateTopic = true;

    /**
     * It is an important basis for the controller to choose the broker master.
     * The lower the value of brokerElectionPriority, the higher the priority of the broker being selected as the master.
     * You can set a lower priority for the broker with better machine conditions.
     */
    private int brokerElectionPriority = Integer.MAX_VALUE;

    private boolean useStaticSubscription = false;

    private MetricsExporterType metricsExporterType = MetricsExporterType.DISABLE;

    private int metricsOtelCardinalityLimit = 50 * 1000;
    private String metricsGrpcExporterTarget = "";
    private String metricsGrpcExporterHeader = "";
    private long metricGrpcExporterTimeOutInMills = 3 * 1000;
    private long metricGrpcExporterIntervalInMills = 60 * 1000;
    private long metricLoggingExporterIntervalInMills = 10 * 1000;

    private int metricsPromExporterPort = 5557;
    private String metricsPromExporterHost = "";

    // Label pairs in CSV. Each label follows pattern of Key:Value. eg: instance_id:xxx,uid:xxx
    private String metricsLabel = "";

    private boolean metricsInDelta = false;

    private long channelExpiredTimeout = 1000 * 120;
    private long subscriptionExpiredTimeout = 1000 * 60 * 10;

    /**
     * Estimate accumulation or not when subscription filter type is tag and is not SUB_ALL.
     */
    private boolean estimateAccumulation = true;

    private boolean coldCtrStrategyEnable = false;
    private boolean usePIDColdCtrStrategy = true;
    private long cgColdReadThreshold = 3 * 1024 * 1024;
    private long globalColdReadThreshold = 100 * 1024 * 1024;
    
    /**
     * The interval to fetch namesrv addr, default value is 10 second
     */
    private long fetchNamesrvAddrInterval = 10 * 1000;

    /**
     * Pop response returns the actual retry topic rather than tampering with the original topic
     */
    private boolean popResponseReturnActualRetryTopic = false;

    /**
     * If both the deleteTopicWithBrokerRegistration flag in the NameServer configuration and this flag are set to true,
     * it guarantees the ultimate consistency of data between the broker and the nameserver during topic deletion.
     */
    private boolean enableSingleTopicRegister = false;

    private boolean enableMixedMessageType = false;

    /**
     * This flag and deleteTopicWithBrokerRegistration flag in the NameServer cannot be set to true at the same time,
     * otherwise there will be a loss of routing
     */
    private boolean enableSplitRegistration = false;

    private long popInflightMessageThreshold = 10000;
    private boolean enablePopMessageThreshold = false;

    private int splitRegistrationSize = 800;

    /**
     * Config in this black list will be not allowed to update by command.
     * Try to update this config black list by restart process.
     * Try to update configures in black list by restart process.
     */
    private String configBlackList = "configBlackList;brokerConfigPath";

    public String getConfigBlackList() {
        return configBlackList;
    }

    public void setConfigBlackList(String configBlackList) {
        this.configBlackList = configBlackList;
    }

    public void setEnablePopBufferMerge(boolean enablePopBufferMerge) {
        this.enablePopBufferMerge = enablePopBufferMerge;
    }

    public void setEnableSkipLongAwaitingAck(boolean enableSkipLongAwaitingAck) {
        this.enableSkipLongAwaitingAck = enableSkipLongAwaitingAck;
    }

    public void setReviveAckWaitMs(long reviveAckWaitMs) {
        this.reviveAckWaitMs = reviveAckWaitMs;
    }

    public void setSlaveReadEnable(final boolean slaveReadEnable) {
        this.slaveReadEnable = slaveReadEnable;
    }

    public void setRegionId(final String regionId) {
        this.regionId = regionId;
    }

    public void setBrokerPermission(int brokerPermission) {
        this.brokerPermission = brokerPermission;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public void setAutoCreateTopicEnable(boolean autoCreateTopic) {
        this.autoCreateTopicEnable = autoCreateTopic;
    }

    public void setBrokerIP2(String brokerIP2) {
        this.brokerIP2 = brokerIP2;
    }
    
    public long getMaxPopPollingSize() {
        return maxPopPollingSize;
    }

    public int getReviveQueueNum() {
        return reviveQueueNum;
    }

    public long getReviveInterval() {
        return reviveInterval;
    }

    public int getPopCkStayBufferTime() {
        return popCkStayBufferTime;
    }

    public int getPopCkStayBufferTimeOut() {
        return popCkStayBufferTimeOut;
    }

    public int getPopPollingMapSize() {
        return popPollingMapSize;
    }

    public long getReviveScanTime() {
        return reviveScanTime;
    }

    public long getReviveMaxSlow() {
        return reviveMaxSlow;
    }

    public int getPopPollingSize() {
        return popPollingSize;
    }

    public boolean isEnablePopBufferMerge() {
        return enablePopBufferMerge;
    }

    public int getPopCkMaxBufferSize() {
        return popCkMaxBufferSize;
    }

    public int getPopCkOffsetMaxQueueSize() {
        return popCkOffsetMaxQueueSize;
    }

    public boolean isEnablePopBatchAck() {
        return enablePopBatchAck;
    }

    public boolean isEnableSkipLongAwaitingAck() {
        return enableSkipLongAwaitingAck;
    }

    public long getReviveAckWaitMs() {
        return reviveAckWaitMs;
    }

    public boolean isEnablePopLog() {
        return enablePopLog;
    }

    public boolean isTraceOn() {
        return traceOn;
    }

    public long getStartAcceptSendRequestTimeStamp() {
        return startAcceptSendRequestTimeStamp;
    }

    public long getWaitTimeMillsInSendQueue() {
        return waitTimeMillsInSendQueue;
    }

    public long getConsumerFallbehindThreshold() {
        return consumerFallbehindThreshold;
    }

    public boolean isBrokerFastFailureEnable() {
        return brokerFastFailureEnable;
    }

    public long getWaitTimeMillsInPullQueue() {
        return waitTimeMillsInPullQueue;
    }

    public boolean isDisableConsumeIfConsumerReadSlowly() {
        return disableConsumeIfConsumerReadSlowly;
    }

    public boolean isSlaveReadEnable() {
        return slaveReadEnable;
    }

    public int getRegisterBrokerTimeoutMills() {
        return registerBrokerTimeoutMills;
    }

    public String getRegionId() {
        return regionId;
    }

    public boolean isTransferMsgByHeap() {
        return transferMsgByHeap;
    }

    public String getMessageStorePlugIn() {
        return messageStorePlugIn;
    }

    public boolean isHighSpeedMode() {
        return highSpeedMode;
    }

    public int getBrokerPermission() {
        return brokerPermission;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }


    public boolean isAutoCreateTopicEnable() {
        return autoCreateTopicEnable;
    }


    public String getBrokerIP1() {
        return brokerIP1;
    }

    public void setBrokerIP1(String brokerIP1) {
        this.brokerIP1 = brokerIP1;
    }

    public String getBrokerIP2() {
        return brokerIP2;
    }


    public int getSendMessageThreadPoolNums() {
        return sendMessageThreadPoolNums;
    }

    public int getPutMessageFutureThreadPoolNums() {
        return putMessageFutureThreadPoolNums;
    }

    public int getPullMessageThreadPoolNums() {
        return pullMessageThreadPoolNums;
    }

    public int getAckMessageThreadPoolNums() {
        return ackMessageThreadPoolNums;
    }

    public int getProcessReplyMessageThreadPoolNums() {
        return processReplyMessageThreadPoolNums;
    }

    public int getQueryMessageThreadPoolNums() {
        return queryMessageThreadPoolNums;
    }

    public int getAdminBrokerThreadPoolNums() {
        return adminBrokerThreadPoolNums;
    }

    public int getFlushConsumerOffsetInterval() {
        return flushConsumerOffsetInterval;
    }

    public boolean isClusterTopicEnable() {
        return clusterTopicEnable;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public boolean isAutoCreateSubscriptionGroup() {
        return autoCreateSubscriptionGroup;
    }

    public String getBrokerConfigPath() {
        return brokerConfigPath;
    }

    public void setBrokerConfigPath(String brokerConfigPath) {
        this.brokerConfigPath = brokerConfigPath;
    }

    public String getRocketmqHome() {
        return rocketmqHome;
    }

    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getLitePullMessageThreadPoolNums() {
        return litePullMessageThreadPoolNums;
    }

    public int getLitePullThreadPoolQueueCapacity() {
        return litePullThreadPoolQueueCapacity;
    }

    public int getAdminBrokerThreadPoolQueueCapacity() {
        return adminBrokerThreadPoolQueueCapacity;
    }

    public int getLoadBalanceThreadPoolQueueCapacity() {
        return loadBalanceThreadPoolQueueCapacity;
    }

    public int getSendHeartbeatTimeoutMillis() {
        return sendHeartbeatTimeoutMillis;
    }

    public long getWaitTimeMillsInLitePullQueue() {
        return waitTimeMillsInLitePullQueue;
    }

    public boolean isLitePullMessageEnable() {
        return litePullMessageEnable;
    }

    public void setLitePullMessageEnable(boolean litePullMessageEnable) {
        this.litePullMessageEnable = litePullMessageEnable;
    }

    public int getSyncBrokerMemberGroupPeriod() {
        return syncBrokerMemberGroupPeriod;
    }

    public boolean isRejectTransactionMessage() {
        return rejectTransactionMessage;
    }

    public boolean isFetchNamesrvAddrByAddressServer() {
        return fetchNamesrvAddrByAddressServer;
    }

    public int getSendThreadPoolQueueCapacity() {
        return sendThreadPoolQueueCapacity;
    }

    public int getPutThreadPoolQueueCapacity() {
        return putThreadPoolQueueCapacity;
    }

    public int getPullThreadPoolQueueCapacity() {
        return pullThreadPoolQueueCapacity;
    }

    public int getAckThreadPoolQueueCapacity() {
        return ackThreadPoolQueueCapacity;
    }

    public int getReplyThreadPoolQueueCapacity() {
        return replyThreadPoolQueueCapacity;
    }

    public int getQueryThreadPoolQueueCapacity() {
        return queryThreadPoolQueueCapacity;
    }

    public boolean isBrokerTopicEnable() {
        return brokerTopicEnable;
    }

    public void setBrokerTopicEnable(boolean brokerTopicEnable) {
        this.brokerTopicEnable = brokerTopicEnable;
    }

    public boolean isLongPollingEnable() {
        return longPollingEnable;
    }

    public void setLongPollingEnable(boolean longPollingEnable) {
        this.longPollingEnable = longPollingEnable;
    }

    public boolean isNotifyConsumerIdsChangedEnable() {
        return notifyConsumerIdsChangedEnable;
    }

    public long getShortPollingTimeMills() {
        return shortPollingTimeMills;
    }

    public void setShortPollingTimeMills(long shortPollingTimeMills) {
        this.shortPollingTimeMills = shortPollingTimeMills;
    }

    public int getClientManageThreadPoolNums() {
        return clientManageThreadPoolNums;
    }

    public int getClientManagerThreadPoolQueueCapacity() {
        return clientManagerThreadPoolQueueCapacity;
    }

    public int getConsumerManagerThreadPoolQueueCapacity() {
        return consumerManagerThreadPoolQueueCapacity;
    }

    public int getConsumerManageThreadPoolNums() {
        return consumerManageThreadPoolNums;
    }

    public int getCommercialBaseCount() {
        return commercialBaseCount;
    }

    public boolean isEnableCalcFilterBitMap() {
        return enableCalcFilterBitMap;
    }

    public void setEnableCalcFilterBitMap(boolean enableCalcFilterBitMap) {
        this.enableCalcFilterBitMap = enableCalcFilterBitMap;
    }

    public int getExpectConsumerNumUseFilter() {
        return expectConsumerNumUseFilter;
    }

    public void setExpectConsumerNumUseFilter(int expectConsumerNumUseFilter) {
        this.expectConsumerNumUseFilter = expectConsumerNumUseFilter;
    }

    public int getMaxErrorRateOfBloomFilter() {
        return maxErrorRateOfBloomFilter;
    }

    public void setMaxErrorRateOfBloomFilter(int maxErrorRateOfBloomFilter) {
        this.maxErrorRateOfBloomFilter = maxErrorRateOfBloomFilter;
    }

    public long getFilterDataCleanTimeSpan() {
        return filterDataCleanTimeSpan;
    }

    public boolean isFilterSupportRetry() {
        return filterSupportRetry;
    }

    public boolean isEnablePropertyFilter() {
        return enablePropertyFilter;
    }

    public void setEnablePropertyFilter(boolean enablePropertyFilter) {
        this.enablePropertyFilter = enablePropertyFilter;
    }

    public boolean isCompressedRegister() {
        return compressedRegister;
    }

    public boolean isForceRegister() {
        return forceRegister;
    }

    public int getHeartbeatThreadPoolQueueCapacity() {
        return heartbeatThreadPoolQueueCapacity;
    }

    public int getHeartbeatThreadPoolNums() {
        return heartbeatThreadPoolNums;
    }

    public long getWaitTimeMillsInHeartbeatQueue() {
        return waitTimeMillsInHeartbeatQueue;
    }

    public int getRegisterNameServerPeriod() {
        return registerNameServerPeriod;
    }

    public long getTransactionTimeOut() {
        return transactionTimeOut;
    }

    public void setTransactionTimeOut(long transactionTimeOut) {
        this.transactionTimeOut = transactionTimeOut;
    }

    public int getTransactionCheckMax() {
        return transactionCheckMax;
    }

    public void setTransactionCheckMax(int transactionCheckMax) {
        this.transactionCheckMax = transactionCheckMax;
    }

    public long getTransactionCheckInterval() {
        return transactionCheckInterval;
    }

    public void setTransactionCheckInterval(long transactionCheckInterval) {
        this.transactionCheckInterval = transactionCheckInterval;
    }

    public int getEndTransactionThreadPoolNums() {
        return endTransactionThreadPoolNums;
    }

    public int getEndTransactionPoolQueueCapacity() {
        return endTransactionPoolQueueCapacity;
    }

    public long getWaitTimeMillsInTransactionQueue() {
        return waitTimeMillsInTransactionQueue;
    }

    public String getMsgTraceTopicName() {
        return msgTraceTopicName;
    }

    public void setMsgTraceTopicName(String msgTraceTopicName) {
        this.msgTraceTopicName = msgTraceTopicName;
    }

    public boolean isTraceTopicEnable() {
        return traceTopicEnable;
    }

    public boolean isAclEnable() {
        return aclEnable;
    }

    public boolean isStoreReplyMessageEnable() {
        return storeReplyMessageEnable;
    }

    public boolean isEnableDetailStat() {
        return enableDetailStat;
    }

    public boolean isAutoDeleteUnusedStats() {
        return autoDeleteUnusedStats;
    }

    public void setAutoDeleteUnusedStats(boolean autoDeleteUnusedStats) {
        this.autoDeleteUnusedStats = autoDeleteUnusedStats;
    }

    public long getLoadBalancePollNameServerInterval() {
        return loadBalancePollNameServerInterval;
    }

    public void setLoadBalancePollNameServerInterval(long loadBalancePollNameServerInterval) {
        this.loadBalancePollNameServerInterval = loadBalancePollNameServerInterval;
    }

    public int getCleanOfflineBrokerInterval() {
        return cleanOfflineBrokerInterval;
    }

    public int getLoadBalanceProcessorThreadPoolNums() {
        return loadBalanceProcessorThreadPoolNums;
    }

    public boolean isServerLoadBalancerEnable() {
        return serverLoadBalancerEnable;
    }

    public MessageRequestMode getDefaultMessageRequestMode() {
        return defaultMessageRequestMode;
    }

    public int getDefaultPopShareQueueNum() {
        return defaultPopShareQueueNum;
    }

    public long getForwardTimeout() {
        return forwardTimeout;
    }

    public int getBrokerHeartbeatInterval() {
        return brokerHeartbeatInterval;
    }

    public long getBrokerNotActiveTimeoutMillis() {
        return brokerNotActiveTimeoutMillis;
    }

    public boolean isEnableNetWorkFlowControl() {
        return enableNetWorkFlowControl;
    }

    public boolean isEnableNotifyAfterPopOrderLockRelease() {
        return enableNotifyAfterPopOrderLockRelease;
    }

    public void setEnableNotifyAfterPopOrderLockRelease(boolean enableNotifyAfterPopOrderLockRelease) {
        this.enableNotifyAfterPopOrderLockRelease = enableNotifyAfterPopOrderLockRelease;
    }

    public boolean isInitPopOffsetByCheckMsgInMem() {
        return initPopOffsetByCheckMsgInMem;
    }

    public boolean isRealTimeNotifyConsumerChange() {
        return realTimeNotifyConsumerChange;
    }

    public boolean isEnableSlaveActingMaster() {
        return enableSlaveActingMaster;
    }

    public void setEnableSlaveActingMaster(boolean enableSlaveActingMaster) {
        this.enableSlaveActingMaster = enableSlaveActingMaster;
    }

    public boolean isEnableRemoteEscape() {
        return enableRemoteEscape;
    }

    public void setEnableRemoteEscape(boolean enableRemoteEscape) {
        this.enableRemoteEscape = enableRemoteEscape;
    }

    public boolean isSkipPreOnline() {
        return skipPreOnline;
    }

    public boolean isAsyncSendEnable() {
        return asyncSendEnable;
    }

    public long getConsumerOffsetUpdateVersionStep() {
        return consumerOffsetUpdateVersionStep;
    }

    public void setConsumerOffsetUpdateVersionStep(long consumerOffsetUpdateVersionStep) {
        this.consumerOffsetUpdateVersionStep = consumerOffsetUpdateVersionStep;
    }

    public long getDelayOffsetUpdateVersionStep() {
        return delayOffsetUpdateVersionStep;
    }

    public void setDelayOffsetUpdateVersionStep(long delayOffsetUpdateVersionStep) {
        this.delayOffsetUpdateVersionStep = delayOffsetUpdateVersionStep;
    }

    public int getCommercialSizePerMsg() {
        return commercialSizePerMsg;
    }

    public long getWaitTimeMillsInAckQueue() {
        return waitTimeMillsInAckQueue;
    }

    public boolean isRejectPullConsumerEnable() {
        return rejectPullConsumerEnable;
    }

    public boolean isAccountStatsEnable() {
        return accountStatsEnable;
    }

    public boolean isAccountStatsPrintZeroValues() {
        return accountStatsPrintZeroValues;
    }

    public boolean isLockInStrictMode() {
        return lockInStrictMode;
    }

    public void setLockInStrictMode(boolean lockInStrictMode) {
        this.lockInStrictMode = lockInStrictMode;
    }

    public boolean isIsolateLogEnable() {
        return isolateLogEnable;
    }

    public boolean isCompatibleWithOldNameSrv() {
        return compatibleWithOldNameSrv;
    }

    public void setCompatibleWithOldNameSrv(boolean compatibleWithOldNameSrv) {
        this.compatibleWithOldNameSrv = compatibleWithOldNameSrv;
    }

    public boolean isEnableControllerMode() {
        return enableControllerMode;
    }

    public void setEnableControllerMode(boolean enableControllerMode) {
        this.enableControllerMode = enableControllerMode;
    }

    public String getControllerAddr() {
        return controllerAddr;
    }

    public void setControllerAddr(String controllerAddr) {
        this.controllerAddr = controllerAddr;
    }

    public boolean isFetchControllerAddrByDnsLookup() {
        return fetchControllerAddrByDnsLookup;
    }

    public long getSyncBrokerMetadataPeriod() {
        return syncBrokerMetadataPeriod;
    }

    public void setSyncBrokerMetadataPeriod(long syncBrokerMetadataPeriod) {
        this.syncBrokerMetadataPeriod = syncBrokerMetadataPeriod;
    }

    public long getCheckSyncStateSetPeriod() {
        return checkSyncStateSetPeriod;
    }

    public void setCheckSyncStateSetPeriod(long checkSyncStateSetPeriod) {
        this.checkSyncStateSetPeriod = checkSyncStateSetPeriod;
    }

    public long getSyncControllerMetadataPeriod() {
        return syncControllerMetadataPeriod;
    }

    public void setSyncControllerMetadataPeriod(long syncControllerMetadataPeriod) {
        this.syncControllerMetadataPeriod = syncControllerMetadataPeriod;
    }

    public int getBrokerElectionPriority() {
        return brokerElectionPriority;
    }

    public long getControllerHeartBeatTimeoutMills() {
        return controllerHeartBeatTimeoutMills;
    }

    public boolean isRecoverConcurrently() {
        return recoverConcurrently;
    }

    public int getRecoverThreadPoolNums() {
        return recoverThreadPoolNums;
    }

    public boolean isFetchNameSrvAddrByDnsLookup() {
        return fetchNameSrvAddrByDnsLookup;
    }

    public boolean isUseServerSideResetOffset() {
        return useServerSideResetOffset;
    }

    public void setUseServerSideResetOffset(boolean useServerSideResetOffset) {
        this.useServerSideResetOffset = useServerSideResetOffset;
    }

    public boolean isEnableBroadcastOffsetStore() {
        return enableBroadcastOffsetStore;
    }

    public void setEnableBroadcastOffsetStore(boolean enableBroadcastOffsetStore) {
        this.enableBroadcastOffsetStore = enableBroadcastOffsetStore;
    }

    public long getBroadcastOffsetExpireSecond() {
        return broadcastOffsetExpireSecond;
    }

    public void setBroadcastOffsetExpireSecond(long broadcastOffsetExpireSecond) {
        this.broadcastOffsetExpireSecond = broadcastOffsetExpireSecond;
    }

    public long getBroadcastOffsetExpireMaxSecond() {
        return broadcastOffsetExpireMaxSecond;
    }

    public void setBroadcastOffsetExpireMaxSecond(long broadcastOffsetExpireMaxSecond) {
        this.broadcastOffsetExpireMaxSecond = broadcastOffsetExpireMaxSecond;
    }

    public MetricsExporterType getMetricsExporterType() {
        return metricsExporterType;
    }

    public int getMetricsOtelCardinalityLimit() {
        return metricsOtelCardinalityLimit;
    }

    public String getMetricsGrpcExporterTarget() {
        return metricsGrpcExporterTarget;
    }

    public String getMetricsGrpcExporterHeader() {
        return metricsGrpcExporterHeader;
    }

    public long getMetricGrpcExporterTimeOutInMills() {
        return metricGrpcExporterTimeOutInMills;
    }

    public long getMetricGrpcExporterIntervalInMills() {
        return metricGrpcExporterIntervalInMills;
    }

    public long getMetricLoggingExporterIntervalInMills() {
        return metricLoggingExporterIntervalInMills;
    }

    public String getMetricsLabel() {
        return metricsLabel;
    }

    public boolean isMetricsInDelta() {
        return metricsInDelta;
    }

    public int getMetricsPromExporterPort() {
        return metricsPromExporterPort;
    }

    public String getMetricsPromExporterHost() {
        return metricsPromExporterHost;
    }

    public int getTransactionOpMsgMaxSize() {
        return transactionOpMsgMaxSize;
    }

    public void setInitPopOffsetByCheckMsgInMem(boolean initPopOffsetByCheckMsgInMem) {
        this.initPopOffsetByCheckMsgInMem = initPopOffsetByCheckMsgInMem;
    }

    public boolean isRetrieveMessageFromPopRetryTopicV1() {
        return retrieveMessageFromPopRetryTopicV1;
    }

    public void setRetrieveMessageFromPopRetryTopicV1(boolean retrieveMessageFromPopRetryTopicV1) {
        this.retrieveMessageFromPopRetryTopicV1 = retrieveMessageFromPopRetryTopicV1;
    }

    public boolean isEnableRetryTopicV2() {
        return enableRetryTopicV2;
    }

    public void setEnableRetryTopicV2(boolean enableRetryTopicV2) {
        this.enableRetryTopicV2 = enableRetryTopicV2;
    }

    public void setTransactionOpMsgMaxSize(int transactionOpMsgMaxSize) {
        this.transactionOpMsgMaxSize = transactionOpMsgMaxSize;
    }

    public int getTransactionOpBatchInterval() {
        return transactionOpBatchInterval;
    }

    public void setTransactionOpBatchInterval(int transactionOpBatchInterval) {
        this.transactionOpBatchInterval = transactionOpBatchInterval;
    }

    public long getChannelExpiredTimeout() {
        return channelExpiredTimeout;
    }

    public long getSubscriptionExpiredTimeout() {
        return subscriptionExpiredTimeout;
    }

    public boolean isValidateSystemTopicWhenUpdateTopic() {
        return validateSystemTopicWhenUpdateTopic;
    }

    public boolean isEstimateAccumulation() {
        return estimateAccumulation;
    }

    public void setEstimateAccumulation(boolean estimateAccumulation) {
        this.estimateAccumulation = estimateAccumulation;
    }

    public boolean isColdCtrStrategyEnable() {
        return coldCtrStrategyEnable;
    }

    public boolean isUsePIDColdCtrStrategy() {
        return usePIDColdCtrStrategy;
    }

    public long getCgColdReadThreshold() {
        return cgColdReadThreshold;
    }

    public long getGlobalColdReadThreshold() {
        return globalColdReadThreshold;
    }

    public boolean isUseStaticSubscription() {
        return useStaticSubscription;
    }

    public long getFetchNamesrvAddrInterval() {
        return fetchNamesrvAddrInterval;
    }

    public boolean isPopResponseReturnActualRetryTopic() {
        return popResponseReturnActualRetryTopic;
    }

    public boolean isEnableSingleTopicRegister() {
        return enableSingleTopicRegister;
    }

    public void setEnableSingleTopicRegister(boolean enableSingleTopicRegister) {
        this.enableSingleTopicRegister = enableSingleTopicRegister;
    }

    public boolean isEnableMixedMessageType() {
        return enableMixedMessageType;
    }

    public boolean isEnableSplitRegistration() {
        return enableSplitRegistration;
    }

    public void setEnableSplitRegistration(boolean enableSplitRegistration) {
        this.enableSplitRegistration = enableSplitRegistration;
    }

    public int getSplitRegistrationSize() {
        return splitRegistrationSize;
    }

    public long getTransactionMetricFlushInterval() {
        return transactionMetricFlushInterval;
    }

    public void setTransactionMetricFlushInterval(long transactionMetricFlushInterval) {
        this.transactionMetricFlushInterval = transactionMetricFlushInterval;
    }

    public long getPopInflightMessageThreshold() {
        return popInflightMessageThreshold;
    }

    public void setPopInflightMessageThreshold(long popInflightMessageThreshold) {
        this.popInflightMessageThreshold = popInflightMessageThreshold;
    }

    public boolean isEnablePopMessageThreshold() {
        return enablePopMessageThreshold;
    }

    public void setEnablePopMessageThreshold(boolean enablePopMessageThreshold) {
        this.enablePopMessageThreshold = enablePopMessageThreshold;
    }

}
