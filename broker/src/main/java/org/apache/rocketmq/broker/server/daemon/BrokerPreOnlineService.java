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

package org.apache.rocketmq.broker.server.daemon;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.api.plugin.BrokerPlugin;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.daemon.schedule.DelayOffsetSerializeWrapper;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.lang.thread.ServiceThread;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.BrokerSyncInfo;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.store.server.config.StorePathConfigHelper;
import org.apache.rocketmq.store.server.ha.HAConnectionState;
import org.apache.rocketmq.store.server.ha.HAConnectionStateNotificationRequest;
import org.apache.rocketmq.store.domain.timer.TimerCheckpoint;

public class BrokerPreOnlineService extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final Broker broker;

    private int waitBrokerIndex = 0;

    public BrokerPreOnlineService(Broker broker) {
        this.broker = broker;
    }

    @Override
    public String getServiceName() {
        if (this.broker != null && this.broker.getBrokerConfig().isInBrokerContainer()) {
            return broker.getBrokerIdentity().getIdentifier() + BrokerPreOnlineService.class.getSimpleName();
        }
        return BrokerPreOnlineService.class.getSimpleName();
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            if (!this.broker.isIsolated()) {
                LOGGER.info("broker {} is online", this.broker.getBrokerConfig().getCanonicalName());
                break;
            }
            try {
                boolean isSuccess = this.prepareForBrokerOnline();
                if (!isSuccess) {
                    this.waitForRunning(1000);
                } else {
                    break;
                }
            } catch (Exception e) {
                LOGGER.error("Broker preOnline error, ", e);
            }
        }

        LOGGER.info(this.getServiceName() + " service end");
    }

    CompletableFuture<Boolean> waitForHaHandshakeComplete(String brokerAddr) {
        LOGGER.info("wait for handshake completion with {}", brokerAddr);
        HAConnectionStateNotificationRequest request =
            new HAConnectionStateNotificationRequest(HAConnectionState.TRANSFER, RemotingHelper.parseHostFromAddress(brokerAddr), true);
        if (this.broker.getMessageStore().getHaService() != null) {
            this.broker.getMessageStore().getHaService().putGroupConnectionStateRequest(request);
        } else {
            LOGGER.error("HAService is null, maybe broker config is wrong. For example, duplicationEnable is true");
            request.getRequestFuture().complete(false);
        }
        return request.getRequestFuture();
    }

    private boolean futureWaitAction(boolean result, BrokerMemberGroup brokerMemberGroup) {
        if (!result) {
            LOGGER.error("wait for handshake completion failed, HA connection lost");
            return false;
        }
        if (this.broker.getBrokerConfig().getBrokerId() != MQConstants.MASTER_ID) {
            LOGGER.info("slave preOnline complete, start service");
            long minBrokerId = getMinBrokerId(brokerMemberGroup.getBrokerAddrs());
            this.registerBroker(minBrokerId, brokerMemberGroup.getBrokerAddrs().get(minBrokerId));
        }
        return true;
    }

    private boolean prepareForMasterOnline(BrokerMemberGroup brokerMemberGroup) {
        List<Long> brokerIdList = new ArrayList<>(brokerMemberGroup.getBrokerAddrs().keySet());
        Collections.sort(brokerIdList);
        while (true) {
            if (waitBrokerIndex >= brokerIdList.size()) {
                LOGGER.info("master preOnline complete, start service");
                this.registerBroker(MQConstants.MASTER_ID, this.broker.getBrokerAddr());
                return true;
            }

            String brokerAddrToWait = brokerMemberGroup.getBrokerAddrs().get(brokerIdList.get(waitBrokerIndex));

            try {
                this.broker.getBrokerOuterAPI().
                    sendBrokerHaInfo(brokerAddrToWait, this.broker.getHAServerAddr(),
                        this.broker.getMessageStore().getBrokerInitMaxOffset(), this.broker.getBrokerAddr());
            } catch (Exception e) {
                LOGGER.error("send ha address to {} exception, {}", brokerAddrToWait, e);
                return false;
            }

            CompletableFuture<Boolean> haHandshakeFuture = waitForHaHandshakeComplete(brokerAddrToWait)
                .thenApply(result -> futureWaitAction(result, brokerMemberGroup));

            try {
                if (!haHandshakeFuture.get()) {
                    return false;
                }
            } catch (Exception e) {
                LOGGER.error("Wait handshake completion exception, {}", e);
                return false;
            }

            if (syncMetadataReverse(brokerAddrToWait)) {
                waitBrokerIndex++;
            } else {
                return false;
            }
        }
    }

    private boolean syncMetadataReverse(String brokerAddr) {
        try {
            LOGGER.info("Get metadata reverse from {}", brokerAddr);

            syncConsumerOffsetReverse(brokerAddr);
            syncDelayOffsetReverse(brokerAddr);
            syncTimerCheckPointReverse(brokerAddr);
            brokerAttachedPluginSyncMetadataReverse(brokerAddr);

        } catch (Exception e) {
            LOGGER.error("GetMetadataReverse Failed", e);
            return false;
        }

        return true;
    }

    private void syncConsumerOffsetReverse(String brokerAddr) throws RemotingSendRequestException, RemotingConnectException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        ConsumerOffsetSerializeWrapper consumerOffsetSerializeWrapper = this.broker.getBrokerOuterAPI().getAllConsumerOffset(brokerAddr);
        if (null != consumerOffsetSerializeWrapper && broker.getConsumerOffsetManager().getDataVersion().compare(consumerOffsetSerializeWrapper.getDataVersion()) <= 0) {
            LOGGER.info("{}'s consumerOffset data version is larger than master broker, {}'s consumerOffset will be used.", brokerAddr, brokerAddr);
            this.broker.getConsumerOffsetManager().getOffsetTable()
                .putAll(consumerOffsetSerializeWrapper.getOffsetTable());
            this.broker.getConsumerOffsetManager().getDataVersion().assignNewOne(consumerOffsetSerializeWrapper.getDataVersion());
            this.broker.getConsumerOffsetManager().persist();
        }
    }

    private void syncDelayOffsetReverse(String brokerAddr) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException, UnsupportedEncodingException {
        String delayOffset = this.broker.getBrokerOuterAPI().getAllDelayOffset(brokerAddr);
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
            DelayOffsetSerializeWrapper.fromJson(delayOffset, DelayOffsetSerializeWrapper.class);

        if (null != delayOffset && broker.getScheduleMessageService().getDataVersion().compare(delayOffsetSerializeWrapper.getDataVersion()) <= 0) {
            LOGGER.info("{}'s scheduleMessageService data version is larger than master broker, {}'s delayOffset will be used.", brokerAddr, brokerAddr);
            String fileName =
                StorePathConfigHelper.getDelayOffsetStorePath(this.broker
                    .getMessageStoreConfig().getStorePathRootDir());
            try {
                IOUtils.string2File(delayOffset, fileName);
                this.broker.getScheduleMessageService().load();
            } catch (IOException e) {
                LOGGER.error("Persist file Exception, {}", fileName, e);
            }
        }
    }

    private void syncTimerCheckPointReverse(String brokerAddr) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        TimerCheckpoint timerCheckpoint = this.broker.getBrokerOuterAPI().getTimerCheckPoint(brokerAddr);
        if (null != this.broker.getTimerCheckpoint() && this.broker.getTimerCheckpoint().getDataVersion().compare(timerCheckpoint.getDataVersion()) <= 0) {
            LOGGER.info("{}'s timerCheckpoint data version is larger than master broker, {}'s timerCheckpoint will be used.", brokerAddr, brokerAddr);
            this.broker.getTimerCheckpoint().setLastReadTimeMs(timerCheckpoint.getLastReadTimeMs());
            this.broker.getTimerCheckpoint().setMasterTimerQueueOffset(timerCheckpoint.getMasterTimerQueueOffset());
            this.broker.getTimerCheckpoint().getDataVersion().assignNewOne(timerCheckpoint.getDataVersion());
            this.broker.getTimerCheckpoint().flush();
        }
    }

    private void brokerAttachedPluginSyncMetadataReverse(String brokerAddr) throws Exception {
        for (BrokerPlugin brokerPlugin : broker.getBrokerServiceManager().getBrokerAttachedPlugins()) {
            if (brokerPlugin != null) {
                brokerPlugin.syncMetadataReverse(brokerAddr);
            }
        }
    }

    private boolean prepareForSlaveOnline(BrokerMemberGroup brokerMemberGroup) {
        BrokerSyncInfo brokerSyncInfo;
        try {
            brokerSyncInfo = this.broker.getBrokerOuterAPI()
                .retrieveBrokerHaInfo(brokerMemberGroup.getBrokerAddrs().get(MQConstants.MASTER_ID));
        } catch (Exception e) {
            LOGGER.error("retrieve master ha info exception, {}", e);
            return false;
        }

        if (this.broker.getMessageStore().getMasterFlushedOffset() == 0
            && this.broker.getMessageStoreConfig().isSyncMasterFlushOffsetWhenStartup()) {
            LOGGER.info("Set master flush offset in slave to {}", brokerSyncInfo.getMasterFlushOffset());
            this.broker.getMessageStore().setMasterFlushedOffset(brokerSyncInfo.getMasterFlushOffset());
        }

        if (brokerSyncInfo.getMasterHaAddress() != null) {
            this.broker.getMessageStore().updateHaMasterAddress(brokerSyncInfo.getMasterHaAddress());
            this.broker.getMessageStore().updateMasterAddress(brokerSyncInfo.getMasterAddress());
        } else {
            LOGGER.info("fetch master ha address return null, start service directly");
            long minBrokerId = getMinBrokerId(brokerMemberGroup.getBrokerAddrs());
            this.registerBroker(minBrokerId, brokerMemberGroup.getBrokerAddrs().get(minBrokerId));
            return true;
        }

        CompletableFuture<Boolean> haHandshakeFuture = waitForHaHandshakeComplete(brokerSyncInfo.getMasterHaAddress())
            .thenApply(result -> futureWaitAction(result, brokerMemberGroup));

        try {
            if (!haHandshakeFuture.get()) {
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("Wait handshake completion exception, {}", e);
            return false;
        }

        return true;
    }

    private boolean prepareForBrokerOnline() {
        BrokerMemberGroup brokerMemberGroup;
        try {
            brokerMemberGroup = this.broker.getBrokerOuterAPI().syncBrokerMemberGroup(
                this.broker.getBrokerConfig().getBrokerClusterName(),
                this.broker.getBrokerConfig().getBrokerName(),
                this.broker.getBrokerConfig().isCompatibleWithOldNameSrv());
        } catch (Exception e) {
            LOGGER.error("syncBrokerMemberGroup from namesrv error, start service failed, will try later, ", e);
            return false;
        }

        if (brokerMemberGroup != null && !brokerMemberGroup.getBrokerAddrs().isEmpty()) {
            long minBrokerId = getMinBrokerId(brokerMemberGroup.getBrokerAddrs());

            if (this.broker.getBrokerConfig().getBrokerId() == MQConstants.MASTER_ID) {
                return prepareForMasterOnline(brokerMemberGroup);
            } else if (minBrokerId == MQConstants.MASTER_ID) {
                return prepareForSlaveOnline(brokerMemberGroup);
            } else {
                LOGGER.info("no master online, start service directly");
                this.registerBroker(minBrokerId, brokerMemberGroup.getBrokerAddrs().get(minBrokerId));
            }
        } else {
            LOGGER.info("no other broker online, will start service directly");
            this.registerBroker(this.broker.getBrokerConfig().getBrokerId(), this.broker.getBrokerAddr());
        }

        return true;
    }

    /**
     * move from BrokerController.startService()
     * @param minBrokerId
     * @param minBrokerAddr
     */
    public void registerBroker(long minBrokerId, String minBrokerAddr) {
        BrokerConfig brokerConfig = broker.getBrokerConfig();
        LOGGER.info("{} start service, min broker id is {}, min broker addr: {}", brokerConfig.getCanonicalName(), minBrokerId, minBrokerAddr);
        broker.getBrokerClusterService().setMinBrokerIdInGroup(minBrokerId);
        broker.getBrokerClusterService().setMinBrokerAddrInGroup(minBrokerAddr);

        broker.getBrokerMessageService().changeSpecialServiceStatus(brokerConfig.getBrokerId() == minBrokerId);
        broker.getBrokerServiceRegistry().registerBrokerAll(true, false, brokerConfig.isForceRegister());
        broker.setIsolated(false);
    }

    private long getMinBrokerId(Map<Long, String> brokerAddrMap) {
        Map<Long, String> brokerAddrMapCopy = new HashMap<>(brokerAddrMap);
        brokerAddrMapCopy.remove(this.broker.getBrokerConfig().getBrokerId());
        if (!brokerAddrMapCopy.isEmpty()) {
            return Collections.min(brokerAddrMapCopy.keySet());
        }
        return this.broker.getBrokerConfig().getBrokerId();
    }
}
