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

import io.openmessaging.storage.dledger.DLedgerLeaderElector;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.common.lang.thread.ThreadFactoryImpl;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.server.config.BrokerRole;
import org.apache.rocketmq.store.domain.commitlog.dledger.DLedgerCommitLog;

public class DLedgerRoleChangeHandler implements DLedgerLeaderElector.RoleChangeHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final ExecutorService executorService;
    private final Broker broker;
    private final DefaultMessageStore messageStore;
    private final DLedgerCommitLog dLedgerCommitLog;
    private final DLedgerServer dLegerServer;
    private Future<?> slaveSyncFuture;
    private long lastSyncTimeMs = System.currentTimeMillis();

    public DLedgerRoleChangeHandler(Broker broker, DefaultMessageStore messageStore) {
        this.broker = broker;
        this.messageStore = messageStore;
        this.dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
        this.dLegerServer = dLedgerCommitLog.getdLedgerServer();
        this.executorService = ThreadUtils.newSingleThreadExecutor(
            new ThreadFactoryImpl("DLegerRoleChangeHandler_", broker.getBrokerIdentity()));
    }

    @Override
    public void handle(long term, MemberState.Role role) {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                try {
                    boolean succ = true;
                    LOGGER.info("Begin handling broker role change term={} role={} currStoreRole={}", term, role, messageStore.getMessageStoreConfig().getBrokerRole());
                    switch (role) {
                        case CANDIDATE:
                            if (messageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE) {
                                changeToSlave(dLedgerCommitLog.getId());
                            }
                            break;
                        case FOLLOWER:
                            changeToSlave(dLedgerCommitLog.getId());
                            break;
                        case LEADER:
                            while (true) {
                                if (!dLegerServer.getMemberState().isLeader()) {
                                    succ = false;
                                    break;
                                }
                                if (dLegerServer.getDLedgerStore().getLedgerEndIndex() == -1) {
                                    break;
                                }
                                if (dLegerServer.getDLedgerStore().getLedgerEndIndex() == dLegerServer.getDLedgerStore().getCommittedIndex()
                                    && messageStore.dispatchBehindBytes() == 0) {
                                    break;
                                }
                                Thread.sleep(100);
                            }
                            if (succ) {
                                messageStore.recoverTopicQueueTable();
                                changeToMaster(BrokerRole.SYNC_MASTER);
                            }
                            break;
                        default:
                            break;
                    }
                    LOGGER.info("Finish handling broker role change succ={} term={} role={} currStoreRole={} cost={}", succ, term, role, messageStore.getMessageStoreConfig().getBrokerRole(), DLedgerUtils.elapsed(start));
                } catch (Throwable t) {
                    LOGGER.info("[MONITOR]Failed handling broker role change term={} role={} currStoreRole={} cost={}", term, role, messageStore.getMessageStoreConfig().getBrokerRole(), DLedgerUtils.elapsed(start), t);
                }
            }
        };
        executorService.submit(runnable);
    }

    private void handleSlaveSynchronize(BrokerRole role) {
        if (role == BrokerRole.SLAVE) {
            if (null != slaveSyncFuture) {
                slaveSyncFuture.cancel(false);
            }
            this.broker.getBrokerClusterService().getSlaveSynchronize().setMasterAddr(null);
            slaveSyncFuture = this.broker.getBrokerScheduleService().getScheduledExecutorService().scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (System.currentTimeMillis() - lastSyncTimeMs > 10 * 1000) {
                            broker.getBrokerClusterService().getSlaveSynchronize().syncAll();
                            lastSyncTimeMs = System.currentTimeMillis();
                        }
                        //timer checkpoint, latency-sensitive, so sync it more frequently
                        broker.getBrokerClusterService().getSlaveSynchronize().syncTimerCheckPoint();
                    } catch (Throwable e) {
                        LOGGER.error("ScheduledTask SlaveSynchronize syncAll error.", e);
                    }
                }
            }, 1000 * 3, 1000 * 3, TimeUnit.MILLISECONDS);
        } else {
            //handle the slave synchronise
            if (null != slaveSyncFuture) {
                slaveSyncFuture.cancel(false);
            }
            this.broker.getBrokerClusterService().getSlaveSynchronize().setMasterAddr(null);
        }
    }

    public void changeToSlave(int brokerId) {
        LOGGER.info("Begin to change to slave brokerName={} brokerId={}", this.broker.getBrokerConfig().getBrokerName(), brokerId);

        //change the role
        this.broker.getBrokerConfig().setBrokerId(brokerId == 0 ? 1 : brokerId); //TO DO check
        this.broker.getMessageStoreConfig().setBrokerRole(BrokerRole.SLAVE);

        this.broker.getBrokerMessageService().changeSpecialServiceStatus(false);

        //handle the slave synchronise
        handleSlaveSynchronize(BrokerRole.SLAVE);

        try {
            this.broker.getBrokerServiceRegistry().registerBrokerAll(true, true, this.broker.getBrokerConfig().isForceRegister());
        } catch (Throwable ignored) {

        }
        LOGGER.info("Finish to change to slave brokerName={} brokerId={}", this.broker.getBrokerConfig().getBrokerName(), brokerId);
    }

    public void changeToMaster(BrokerRole role) {
        if (role == BrokerRole.SLAVE) {
            return;
        }
        LOGGER.info("Begin to change to master brokerName={}", this.broker.getBrokerConfig().getBrokerName());

        //handle the slave synchronise
        handleSlaveSynchronize(role);

        this.broker.getBrokerMessageService().changeSpecialServiceStatus(true);

        //if the operations above are totally successful, we change to master
        this.broker.getBrokerConfig().setBrokerId(0); //TO DO check
        this.broker.getMessageStoreConfig().setBrokerRole(role);

        try {
            this.broker.getBrokerServiceRegistry().registerBrokerAll(true, true, this.broker.getBrokerConfig().isForceRegister());
        } catch (Throwable ignored) {

        }
        LOGGER.info("Finish to change to master brokerName={}", this.broker.getBrokerConfig().getBrokerName());
    }

    @Override
    public void startup() {

    }

    @Override
    public void shutdown() {
        executorService.shutdown();
    }
}
