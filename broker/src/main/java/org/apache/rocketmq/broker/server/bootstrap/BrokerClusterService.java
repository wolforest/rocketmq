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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.synchronizer.ReplicasManager;
import org.apache.rocketmq.broker.server.synchronizer.SlaveSynchronize;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.utils.NetworkUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.BrokerSyncInfo;

/**
 * related to :
 *      Master/Slave
 */
public class BrokerClusterService {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final Broker broker;
    private final BrokerConfig brokerConfig;

    private final Lock lock = new ReentrantLock();

    protected volatile long minBrokerIdInGroup = 0;
    protected volatile String minBrokerAddrInGroup = null;

    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();
    private final SlaveSynchronize slaveSynchronize;
    private ReplicasManager replicasManager;
    private boolean updateMasterHAServerAddrPeriodically = false;


    public BrokerClusterService(Broker broker) {
        this.broker = broker;
        this.brokerConfig = broker.getBrokerConfig();

        this.slaveSynchronize = new SlaveSynchronize(broker);
    }

    public void load() {
        if (this.brokerConfig.isEnableControllerMode()) {
            this.replicasManager = new ReplicasManager(broker);
            this.replicasManager.setFenced(true);
        }
    }

    public void start() {
        if (this.replicasManager != null) {
            this.replicasManager.start();
        }
    }

    public void shutdown() {
        if (this.replicasManager != null) {
            this.replicasManager.shutdown();
        }
    }


    public void updateMinBroker(long minBrokerId, String minBrokerAddr) {
        updateMinBroker(minBrokerId, minBrokerAddr, null, null);
    }

    public void updateMinBroker(long minBrokerId, String minBrokerAddr, String offlineBrokerAddr, String masterHaAddr) {
        if (!brokerConfig.isEnableSlaveActingMaster() || brokerConfig.getBrokerId() == MQConstants.MASTER_ID) {
            return;
        }

        if (minBrokerId == this.minBrokerIdInGroup) {
            return;
        }

        if (null == offlineBrokerAddr && minBrokerId > this.minBrokerIdInGroup) {
            offlineBrokerAddr = this.minBrokerAddrInGroup;
        }

        try {
            if (!lock.tryLock(3000, TimeUnit.MILLISECONDS)) {
                return;
            }
            onMinBrokerChange(minBrokerId, minBrokerAddr, offlineBrokerAddr, masterHaAddr);
        } catch (InterruptedException e) {
            LOG.error("Update min broker error, {}", e);
        } finally {
            lock.unlock();
        }
    }

    private void onMasterOffline() {
        // close channels with master broker
        String masterAddr = this.slaveSynchronize.getMasterAddr();
        if (masterAddr != null) {
            broker.getClusterClient().getRemotingClient().closeChannels(
                Arrays.asList(masterAddr, NetworkUtils.brokerVIPChannel(true, masterAddr)));
        }
        // master not available, stop sync
        this.slaveSynchronize.setMasterAddr(null);
        broker.getMessageStore().updateHaMasterAddress(null);
    }

    private void onMasterOnline(String masterAddr, String masterHaAddr) {
        boolean needSyncMasterFlushOffset = broker.getMessageStore().getMasterFlushedOffset() == 0
            && this.broker.getMessageStoreConfig().isSyncMasterFlushOffsetWhenStartup();
        if (masterHaAddr == null || needSyncMasterFlushOffset) {
            doSyncMasterFlushOffset(masterAddr, masterHaAddr, needSyncMasterFlushOffset);
        }

        // set master HA address.
        if (masterHaAddr != null) {
            broker.getMessageStore().updateHaMasterAddress(masterHaAddr);
        }

        // wakeup HAClient
        broker.getMessageStore().wakeupHAClient();
    }

    private void doSyncMasterFlushOffset(String masterAddr, String masterHaAddr, boolean needSyncMasterFlushOffset) {
        try {
            BrokerSyncInfo brokerSyncInfo = broker.getClusterClient().retrieveBrokerHaInfo(masterAddr);

            if (needSyncMasterFlushOffset) {
                LOG.info("Set master flush offset in slave to {}", brokerSyncInfo.getMasterFlushOffset());
                broker.getMessageStore().setMasterFlushedOffset(brokerSyncInfo.getMasterFlushOffset());
            }

            if (masterHaAddr == null) {
                broker.getMessageStore().updateHaMasterAddress(brokerSyncInfo.getMasterHaAddress());
                broker.getMessageStore().updateMasterAddress(brokerSyncInfo.getMasterAddress());
            }
        } catch (Exception e) {
            LOG.error("retrieve master ha info exception, {}", e);
        }
    }

    private void onMinBrokerChange(long minBrokerId, String minBrokerAddr, String offlineBrokerAddr,
        String masterHaAddr) {
        LOG.info("Min broker changed, old: {}-{}, new {}-{}",
            this.minBrokerIdInGroup, this.minBrokerAddrInGroup, minBrokerId, minBrokerAddr);

        this.minBrokerIdInGroup = minBrokerId;
        this.minBrokerAddrInGroup = minBrokerAddr;

        broker.getBrokerMessageService().changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == this.minBrokerIdInGroup);

        if (offlineBrokerAddr != null && offlineBrokerAddr.equals(this.slaveSynchronize.getMasterAddr())) {
            // master offline
            onMasterOffline();
        }

        if (minBrokerId == MQConstants.MASTER_ID && minBrokerAddr != null) {
            // master online
            onMasterOnline(minBrokerAddr, masterHaAddr);
        }

        // notify PullRequest on hold to pull from master.
        if (this.minBrokerIdInGroup == MQConstants.MASTER_ID) {
            broker.getBrokerNettyServer().getPullRequestHoldService().notifyMasterOnline();
        }
    }

    public long getMinBrokerIdInGroup() {
        return minBrokerIdInGroup;
    }

    public void setMinBrokerIdInGroup(long minBrokerIdInGroup) {
        this.minBrokerIdInGroup = minBrokerIdInGroup;
    }

    public String getMinBrokerAddrInGroup() {
        return minBrokerAddrInGroup;
    }

    public void setMinBrokerAddrInGroup(String minBrokerAddrInGroup) {
        this.minBrokerAddrInGroup = minBrokerAddrInGroup;
    }

    public RebalanceLockManager getRebalanceLockManager() {
        return rebalanceLockManager;
    }

    public SlaveSynchronize getSlaveSynchronize() {
        return slaveSynchronize;
    }

    public ReplicasManager getReplicasManager() {
        return replicasManager;
    }

    public void setReplicasManager(ReplicasManager replicasManager) {
        this.replicasManager = replicasManager;
    }

    public boolean isUpdateMasterHAServerAddrPeriodically() {
        return updateMasterHAServerAddrPeriodically;
    }

    public void setUpdateMasterHAServerAddrPeriodically(boolean updateMasterHAServerAddrPeriodically) {
        this.updateMasterHAServerAddrPeriodically = updateMasterHAServerAddrPeriodically;
    }
}
