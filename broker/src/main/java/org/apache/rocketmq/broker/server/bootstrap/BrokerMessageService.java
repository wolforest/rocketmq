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
package org.apache.rocketmq.broker.server.bootstrap;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.common.app.config.BrokerPathConfigHelper;
import org.apache.rocketmq.broker.infra.EscapeBridge;
import org.apache.rocketmq.broker.domain.metadata.filter.CommitLogDispatcherCalcBitMap;
import org.apache.rocketmq.broker.api.plugin.BrokerPlugin;
import org.apache.rocketmq.broker.api.controller.AckMessageProcessor;
import org.apache.rocketmq.broker.server.daemon.pop.PopServiceManager;
import org.apache.rocketmq.broker.server.daemon.schedule.ScheduleMessageService;
import org.apache.rocketmq.broker.domain.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.domain.transaction.TransactionalMessageCheckService;
import org.apache.rocketmq.broker.domain.transaction.TransactionalMessageService;
import org.apache.rocketmq.broker.domain.transaction.queue.DefaultTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.domain.transaction.queue.TransactionalMessageBridge;
import org.apache.rocketmq.broker.domain.transaction.queue.TransactionalMessageServiceImpl;
import org.apache.rocketmq.broker.domain.HookUtils;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.ServiceProvider;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.api.plugin.MessageArrivingListener;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.server.store.RocksDBMessageStore;
import org.apache.rocketmq.store.server.config.StoreType;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.domain.commitlog.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.api.plugin.PutMessageHook;
import org.apache.rocketmq.store.api.plugin.SendMessageBackHook;
import org.apache.rocketmq.store.api.plugin.MessagePluginFactory;
import org.apache.rocketmq.store.api.plugin.MessageStorePluginContext;
import org.apache.rocketmq.store.api.broker.stats.BrokerStatsManager;
import org.apache.rocketmq.store.domain.timer.TimerCheckpoint;
import org.apache.rocketmq.store.domain.timer.TimerMessageStore;
import org.apache.rocketmq.store.domain.timer.TimerMetrics;

public class BrokerMessageService {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private volatile boolean isTransactionCheckServiceStart = false;
    private volatile boolean isScheduleServiceStart = false;

    private final BrokerConfig brokerConfig;
    private final MessageStoreConfig messageStoreConfig;
    private final ConcurrentMap<String, TopicConfig> topicConfigTable;

    private final Broker broker;
    private final BrokerStatsManager brokerStatsManager;
    private final MessageArrivingListener messageArrivingListener;

    private final ScheduleMessageService scheduleMessageService;
    private TimerCheckpoint timerCheckpoint;
    private final EscapeBridge escapeBridge;

    protected TransactionalMessageCheckService transactionalMessageCheckService;
    protected TransactionalMessageService transactionalMessageService;
    protected AbstractTransactionalMessageCheckListener transactionalMessageCheckListener;

    private MessageStore messageStore;
    private TimerMessageStore timerMessageStore;


    public BrokerMessageService(Broker broker) {
        this.broker = broker;
        this.brokerConfig = broker.getBrokerConfig();
        this.brokerStatsManager = broker.getBrokerStatsManager();
        this.messageArrivingListener = broker.getBrokerNettyServer().getMessageArrivingListener();
        this.messageStoreConfig = broker.getMessageStoreConfig();
        this.topicConfigTable = broker.getTopicConfigManager().getTopicConfigTable();

        this.scheduleMessageService = new ScheduleMessageService(broker);
        this.escapeBridge = new EscapeBridge(broker);
    }

    public boolean init() {
        try {
            DefaultMessageStore defaultMessageStore;
            if (this.messageStoreConfig.isEnableRocksDBStore()) {
                defaultMessageStore = new RocksDBMessageStore(this.messageStoreConfig, this.brokerStatsManager, this.messageArrivingListener, this.brokerConfig, topicConfigTable);
            } else {
                defaultMessageStore = new DefaultMessageStore(this.messageStoreConfig, this.brokerStatsManager, this.messageArrivingListener, this.brokerConfig, topicConfigTable);
            }

            initDLedgerCommitLog(defaultMessageStore);
            initStorePlugins(defaultMessageStore);
            initTimerMessageStore();

            boolean result = loadMessageStore();
            initTransaction();

            return result;
        } catch (IOException e) {
            LOG.error("BrokerController#initialize: unexpected error occurs", e);
            return false;
        }
    }

    public void start() throws Exception {
        if (this.messageStore != null) {
            this.messageStore.start();
        }

        if (this.timerMessageStore != null) {
            this.timerMessageStore.start();
        }

        if (this.escapeBridge != null) {
            this.escapeBridge.start();
        }
    }

    public void shutdown() {
        //it is better to make sure the timerMessageStore shutdown firstly
        if (this.timerMessageStore != null) {
            this.timerMessageStore.shutdown();
        }

        if (this.messageStore != null) {
            this.messageStore.shutdown();
        }

        if (this.scheduleMessageService != null) {
            this.scheduleMessageService.persist();
            this.scheduleMessageService.shutdown();
        }

        if (this.escapeBridge != null) {
            escapeBridge.shutdown();
        }

        if (this.transactionalMessageService != null) {
            this.transactionalMessageService.close();
        }

        if (this.transactionalMessageCheckService != null) {
            this.transactionalMessageCheckService.shutdown(false);
        }

    }

    public MessageStore getMessageStoreByBrokerName(String brokerName) {
        if (this.brokerConfig.getBrokerName().equals(brokerName)) {
            return this.getMessageStore();
        }
        return null;
    }

    /**
     * running status of
     *      scheduled message,
     *      transaction check,
     *      message ack process
     *      and BrokerAttachedPlugins
     *
     * @return boolean
     */
    public boolean isSpecialServiceRunning() {
        if (isScheduleServiceStart() && isTransactionCheckServiceStart()) {
            return true;
        }

        PopServiceManager popServiceManager = broker.getBrokerNettyServer().getPopServiceManager();
        return popServiceManager != null && popServiceManager.isReviveRunning();
    }

    public void changeSpecialServiceStatus(boolean shouldStart) {
        for (BrokerPlugin brokerPlugin : broker.getBrokerServiceManager().getBrokerAttachedPlugins()) {
            if (brokerPlugin != null) {
                brokerPlugin.statusChanged(shouldStart);
            }
        }

        changeScheduleServiceStatus(shouldStart);
        changeTransactionCheckServiceStatus(shouldStart);

        AckMessageProcessor ackMessageProcessor = broker.getBrokerNettyServer().getAckMessageProcessor();
        if (ackMessageProcessor != null) {
            LOG.info("Set PopReviveService Status to {}", shouldStart);
            broker.getBrokerNettyServer().getPopServiceManager().setReviveStatus(shouldStart);
        }
    }

    public synchronized void changeTransactionCheckServiceStatus(boolean shouldStart) {
        if (isTransactionCheckServiceStart == shouldStart) {
            return;
        }

        LOG.info("TransactionCheckService status changed to {}", shouldStart);
        if (shouldStart) {
            this.transactionalMessageCheckService.start();
        } else {
            this.transactionalMessageCheckService.shutdown(true);
        }
        isTransactionCheckServiceStart = shouldStart;
    }

    public synchronized void changeScheduleServiceStatus(boolean shouldStart) {
        if (isScheduleServiceStart == shouldStart) {
            return;
        }

        LOG.info("ScheduleServiceStatus changed to {}", shouldStart);
        if (shouldStart) {
            this.scheduleMessageService.start();
        } else {
            this.scheduleMessageService.stop();
        }
        isScheduleServiceStart = shouldStart;

        if (timerMessageStore != null) {
            timerMessageStore.getTimerState().syncLastReadTimeMs();
            timerMessageStore.getTimerState().setShouldRunningDequeue(shouldStart);
        }
    }

    private void initDLedgerCommitLog(DefaultMessageStore defaultMessageStore) {
        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            return;
        }

        DLedgerRoleChangeHandler roleChangeHandler = new DLedgerRoleChangeHandler(broker, defaultMessageStore);
        DLedgerCommitLog dLedgerCommitLog =   (DLedgerCommitLog) defaultMessageStore.getCommitLog();

        dLedgerCommitLog.getdLedgerServer()
            .getDLedgerLeaderElector()
            .addRoleChangeHandler(roleChangeHandler);
    }

    private void initTimerMessageStore() throws IOException {
        if (!messageStoreConfig.isTimerWheelEnable()) {
            return ;
        }

        this.timerCheckpoint = new TimerCheckpoint(BrokerPathConfigHelper.getTimerCheckPath(messageStoreConfig.getStorePathRootDir()));
        TimerMetrics timerMetrics = new TimerMetrics(BrokerPathConfigHelper.getTimerMetricsPath(messageStoreConfig.getStorePathRootDir()));
        this.timerMessageStore = new TimerMessageStore(messageStore, messageStoreConfig, timerCheckpoint, timerMetrics, brokerStatsManager);
        this.timerMessageStore.registerEscapeBridgeHook(msg -> escapeBridge.putMessage(msg));
        this.messageStore.setTimerMessageStore(this.timerMessageStore);
    }

    private void initStorePlugins(DefaultMessageStore defaultMessageStore) throws IOException {
        // Load store plugin
        MessageStorePluginContext context = new MessageStorePluginContext(
            messageStoreConfig, brokerStatsManager, this.messageArrivingListener, brokerConfig, broker.getConfiguration());

        this.messageStore = MessagePluginFactory.build(context, defaultMessageStore);

        this.messageStore.getDispatcherList().addFirst(new CommitLogDispatcherCalcBitMap(this.brokerConfig, broker.getConsumerFilterManager()));
    }

    private boolean loadMessageStore() {
        boolean result = true;

        if (messageStore != null) {
            registerMessageStoreHook();
            result = this.messageStore.load();
        }

        if (messageStoreConfig.isTimerWheelEnable()) {
            result = result && this.timerMessageStore.load();
        }

        //scheduleMessageService load after messageStore load success
        result = result && this.scheduleMessageService.load();
        return result;
    }

    private void initTransaction() {
        TransactionalMessageBridge transactionalMessageBridge = new TransactionalMessageBridge(broker, this.getMessageStore());

        this.transactionalMessageService = ServiceProvider.loadClass(TransactionalMessageService.class);
        if (null == this.transactionalMessageService) {
            this.transactionalMessageService = new TransactionalMessageServiceImpl(transactionalMessageBridge);
            LOG.warn("Load default transaction message hook service: {}", TransactionalMessageServiceImpl.class.getSimpleName());
        }

        this.transactionalMessageCheckListener = ServiceProvider.loadClass(AbstractTransactionalMessageCheckListener.class);
        if (null == this.transactionalMessageCheckListener) {
            this.transactionalMessageCheckListener = new DefaultTransactionalMessageCheckListener();
            LOG.warn("Load default discard message hook service: {}", DefaultTransactionalMessageCheckListener.class.getSimpleName());
        }
        this.transactionalMessageCheckListener.setBrokerController(broker);
        this.transactionalMessageCheckService = new TransactionalMessageCheckService(broker, transactionalMessageBridge, this.transactionalMessageCheckListener);
    }

    private void addCheckBeforePutMessageHook() {
        messageStore.getPutMessageHookList().add(new PutMessageHook() {
            @Override
            public String hookName() {
                return "checkBeforePutMessage";
            }

            @Override
            public PutMessageResult executeBeforePutMessage(MessageExt msg) {
                return HookUtils.checkBeforePutMessage(broker, msg);
            }
        });
    }

    private void addInnerBatchCheckerHook() {
        messageStore.getPutMessageHookList().add(new PutMessageHook() {
            @Override
            public String hookName() {
                return "innerBatchChecker";
            }

            @Override
            public PutMessageResult executeBeforePutMessage(MessageExt msg) {
                if (msg instanceof MessageExtBrokerInner) {
                    return HookUtils.checkInnerBatch(broker, msg);
                }
                return null;
            }
        });
    }

    private void addHandleScheduleMessageHook() {
        messageStore.getPutMessageHookList().add(new PutMessageHook() {
            @Override
            public String hookName() {
                return "handleScheduleMessage";
            }

            @Override
            public PutMessageResult executeBeforePutMessage(MessageExt msg) {
                if (msg instanceof MessageExtBrokerInner) {
                    return HookUtils.handleScheduleMessage(broker, (MessageExtBrokerInner) msg);
                }
                return null;
            }
        });
    }

    private SendMessageBackHook createSendMessageBackHook() {
        return new SendMessageBackHook() {
            @Override
            public boolean executeSendMessageBack(List<MessageExt> msgList, String brokerName, String brokerAddr) {
                return HookUtils.sendMessageBack(broker, msgList, brokerName, brokerAddr);
            }
        };
    }

    private void registerMessageStoreHook() {
        addCheckBeforePutMessageHook();
        addInnerBatchCheckerHook();
        addHandleScheduleMessageHook();

        SendMessageBackHook sendMessageBackHook = createSendMessageBackHook();
        if (messageStore != null) {
            messageStore.setSendMessageBackHook(sendMessageBackHook);
        }
    }

    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }

    public TimerCheckpoint getTimerCheckpoint() {
        return timerCheckpoint;
    }

    public EscapeBridge getEscapeBridge() {
        return escapeBridge;
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public TimerMessageStore getTimerMessageStore() {
        return timerMessageStore;
    }

    public void setTransactionalMessageCheckService(
        TransactionalMessageCheckService transactionalMessageCheckService) {
        this.transactionalMessageCheckService = transactionalMessageCheckService;
    }

    public TransactionalMessageService getTransactionalMessageService() {
        return transactionalMessageService;
    }

    public void setTransactionalMessageService(TransactionalMessageService transactionalMessageService) {
        this.transactionalMessageService = transactionalMessageService;
    }

    public boolean isTransactionCheckServiceStart() {
        return isTransactionCheckServiceStart;
    }

    public boolean isScheduleServiceStart() {
        return isScheduleServiceStart;
    }

    public boolean isEnableRocksDBStore() {
        return StoreType.DEFAULT_ROCKSDB.getStoreType().equalsIgnoreCase(this.messageStoreConfig.getStoreType());
    }

}
