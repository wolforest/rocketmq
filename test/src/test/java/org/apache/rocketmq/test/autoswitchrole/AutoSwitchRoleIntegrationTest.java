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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.autoswitchrole;

import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.synchronizer.ReplicasManager;
import org.apache.rocketmq.common.app.config.ControllerConfig;
import org.apache.rocketmq.common.domain.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.controller.ControllerManager;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.server.config.BrokerRole;
import org.apache.rocketmq.store.server.ha.HAClient;
import org.apache.rocketmq.store.server.ha.core.HAConnectionState;
import org.apache.rocketmq.store.server.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.infra.mappedfile.MappedFile;
import org.apache.rocketmq.store.infra.mappedfile.MappedFileQueue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore
public class AutoSwitchRoleIntegrationTest extends AutoSwitchRoleBase {

    private static final int DEFAULT_FILE_SIZE = 1024 * 1024;
    private static NamesrvController namesrvController;
    private static ControllerManager controllerManager;
    private static String nameserverAddress;
    private static String controllerAddress;

    private static ControllerConfig controllerConfig;

    private Broker broker1;
    private Broker broker2;
    private Random random = new Random();

    @BeforeClass
    public static void init() throws Exception {
        initialize();

        int controllerPort = nextPort();
        final String peers = String.format("n0-localhost:%d", controllerPort);

        final NettyServerConfig serverConfig = new NettyServerConfig();
        int namesrvPort = nextPort();
        serverConfig.setListenPort(namesrvPort);

        controllerConfig = buildControllerConfig("n0", peers);
        namesrvController = new NamesrvController(new NamesrvConfig(), serverConfig, new NettyClientConfig());
        assertTrue(namesrvController.initialize());
        namesrvController.start();

        initAndStartControllerManager();

        nameserverAddress = "127.0.0.1:" + namesrvPort + ";";
        controllerAddress = "127.0.0.1:" + controllerPort + ";";
    }

    private static void initAndStartControllerManager() {
        controllerManager = new ControllerManager(controllerConfig, new NettyServerConfig(), new NettyClientConfig());
        assertTrue(controllerManager.initialize());
        controllerManager.start();
    }

    public void initBroker(int mappedFileSize, String brokerName) throws Exception {

        this.broker1 = startBroker(nameserverAddress, controllerAddress, brokerName, 1, nextPort(), nextPort(), nextPort(), BrokerRole.SYNC_MASTER, mappedFileSize);
        this.broker2 = startBroker(nameserverAddress, controllerAddress, brokerName, 2, nextPort(), nextPort(), nextPort(), BrokerRole.SLAVE, mappedFileSize);
        // Wait slave connecting to master
        assertTrue(waitSlaveReady(this.broker2.getMessageStore()));
        Thread.sleep(1000);
    }

    public void mockData(String topic) throws Exception {
        final MessageStore messageStore = broker1.getMessageStore();
        putMessage(messageStore, topic);
        // Check slave message
        checkMessage(broker2.getMessageStore(), topic, 10, 0);
    }

    public boolean waitSlaveReady(MessageStore messageStore) throws InterruptedException {
        int tryTimes = 0;
        while (tryTimes < 100) {
            final HAClient haClient = messageStore.getHaService().getHAClient();
            if (haClient != null && haClient.getCurrentState().equals(HAConnectionState.TRANSFER)) {
                return true;
            } else {
                Thread.sleep(2000);
                tryTimes++;
            }
        }
        return false;
    }

    @Test
    public void testCheckSyncStateSet() throws Exception {
        String topic = "Topic-" + AutoSwitchRoleIntegrationTest.class.getSimpleName() + random.nextInt(65535);
        String brokerName = "Broker-" + AutoSwitchRoleIntegrationTest.class.getSimpleName() + random.nextInt(65535);
        initBroker(DEFAULT_FILE_SIZE, brokerName);

        mockData(topic);

        // Check SyncStateSet
        final ReplicasManager replicasManager = broker1.getBrokerClusterService().getReplicasManager();
        SyncStateSet syncStateSet = replicasManager.getSyncStateSet();
        assertEquals(2, syncStateSet.getSyncStateSet().size());

        // Shutdown controller2
        ScheduledExecutorService singleThread = Executors.newSingleThreadScheduledExecutor();
        while (!singleThread.awaitTermination(6 * 1000, TimeUnit.MILLISECONDS)) {
            this.broker2.shutdown();
            singleThread.shutdown();
        }

        syncStateSet = replicasManager.getSyncStateSet();
        shutdownAndClearBroker();
        assertEquals(1, syncStateSet.getSyncStateSet().size());
    }

    @Test
    public void testChangeMaster() throws Exception {
        String topic = "Topic-" + AutoSwitchRoleIntegrationTest.class.getSimpleName() + random.nextInt(65535);
        String brokerName = "Broker-" + AutoSwitchRoleIntegrationTest.class.getSimpleName() + random.nextInt(65535);
        initBroker(DEFAULT_FILE_SIZE, brokerName);
        int listenPort = broker1.getBrokerConfig().getListenPort();
        int nettyPort = broker1.getNettyServerConfig().getListenPort();
        mockData(topic);

        // Let master shutdown
        broker1.shutdown();
        brokerList.remove(this.broker1);
        Thread.sleep(6000);

        // The slave should change to master
        assertTrue(broker2.getBrokerClusterService().getReplicasManager().isMasterState());
        assertEquals(broker2.getBrokerClusterService().getReplicasManager().getMasterEpoch(), 2);

        // Restart old master, it should be slave
        broker1 = startBroker(nameserverAddress, controllerAddress, brokerName, 1, nextPort(), listenPort, nettyPort, BrokerRole.SLAVE, DEFAULT_FILE_SIZE);
        waitSlaveReady(broker1.getMessageStore());

        assertFalse(broker1.getBrokerClusterService().getReplicasManager().isMasterState());
        assertEquals(broker1.getBrokerClusterService().getReplicasManager().getMasterAddress(), broker2.getBrokerClusterService().getReplicasManager().getBrokerAddress());

        // Put another batch messages
        final MessageStore messageStore = broker2.getMessageStore();
        putMessage(messageStore, topic);

        // Check slave message
        checkMessage(broker1.getMessageStore(), topic, 20, 0);
        shutdownAndClearBroker();
    }


    @Test
    public void testRestartWithChangedAddress() throws Exception {
        String topic = "Topic-" + AutoSwitchRoleIntegrationTest.class.getSimpleName() + random.nextInt(65535);
        String brokerName = "Broker-" + AutoSwitchRoleIntegrationTest.class.getSimpleName() + random.nextInt(65535);
        int oldPort = nextPort();
        this.broker1 = startBroker(nameserverAddress, controllerAddress, brokerName, 1, nextPort(), oldPort, oldPort, BrokerRole.SYNC_MASTER, DEFAULT_FILE_SIZE);
        Thread.sleep(1000);
        assertTrue(broker1.getBrokerClusterService().getReplicasManager().isMasterState());
        assertEquals(broker1.getBrokerClusterService().getReplicasManager().getMasterEpoch(), 1);

        // Let master shutdown
        broker1.shutdown();
        brokerList.remove(this.broker1);
        Thread.sleep(6000);

        // Restart with changed address
        int newPort = nextPort();
        this.broker1 = startBroker(nameserverAddress, controllerAddress, brokerName, 1, nextPort(), newPort, newPort, BrokerRole.SYNC_MASTER, DEFAULT_FILE_SIZE);
        Thread.sleep(1000);

        // Check broker id
        assertEquals(1, broker1.getBrokerClusterService().getReplicasManager().getBrokerControllerId().longValue());
        // Check role
        assertTrue(broker1.getBrokerClusterService().getReplicasManager().isMasterState());

        // check ip address
        RemotingCommand remotingCommand = controllerManager.getController().getReplicaInfo(new GetReplicaInfoRequestHeader(brokerName)).get(500, TimeUnit.MILLISECONDS);
        GetReplicaInfoResponseHeader resp = (GetReplicaInfoResponseHeader) remotingCommand.readCustomHeader();
        assertEquals(1, resp.getMasterBrokerId().longValue());
        assertTrue(resp.getMasterAddress().contains(String.valueOf(newPort)));
        shutdownAndClearBroker();
    }

    @Test
    public void testBasicWorkWhenControllerShutdown() throws Exception {
        String topic = "Foobar";
        String brokerName = "Broker-" + AutoSwitchRoleIntegrationTest.class.getSimpleName() + random.nextInt();
        initBroker(DEFAULT_FILE_SIZE, brokerName);
        // Put message from 0 to 9
        putMessage(this.broker1.getMessageStore(), topic);
        checkMessage(this.broker2.getMessageStore(), topic, 10, 0);

        // Shutdown Controller
        controllerManager.shutdown();

        // Put message from 10 to 19
        putMessage(this.broker1.getMessageStore(), topic);
        checkMessage(this.broker2.getMessageStore(), topic, 20, 0);

        initAndStartControllerManager();
    }

    @Test
    public void testAddBroker() throws Exception {
        String topic = "Topic-" + AutoSwitchRoleIntegrationTest.class.getSimpleName() + random.nextInt(65535);
        String brokerName = "Broker-" + AutoSwitchRoleIntegrationTest.class.getSimpleName() + random.nextInt(65535);
        initBroker(DEFAULT_FILE_SIZE, brokerName);
        mockData(topic);

        Broker broker3 = startBroker(nameserverAddress, controllerAddress, brokerName, 3, nextPort(), nextPort(), nextPort(), BrokerRole.SLAVE, DEFAULT_FILE_SIZE);
        waitSlaveReady(broker3.getMessageStore());
        checkMessage(broker3.getMessageStore(), topic, 10, 0);
        putMessage(this.broker1.getMessageStore(), topic);
        checkMessage(broker3.getMessageStore(), topic, 20, 0);
        shutdownAndClearBroker();
    }

    @Test
    public void testTruncateEpochLogAndChangeMaster() throws Exception {
        shutdownAndClearBroker();
        String topic = "FooBar";
        String brokerName = "Broker-" + AutoSwitchRoleIntegrationTest.class.getSimpleName() + random.nextInt(65535);
        // Noted that 10 msg 's total size = 1570, and if init the mappedFileSize = 1700, one file only be used to store 10 msg.
        initBroker(1700, brokerName);
        // Step1: Put message
        putMessage(this.broker1.getMessageStore(), topic);
        checkMessage(this.broker2.getMessageStore(), topic, 10, 0);

        // Step2: shutdown broker1, broker2 as master
        broker1.shutdown();
        brokerList.remove(broker1);
        Thread.sleep(5000);

        assertTrue(broker2.getBrokerClusterService().getReplicasManager().isMasterState());
        assertEquals(broker2.getBrokerClusterService().getReplicasManager().getMasterEpoch(), 2);

        // Step3: add broker3
        Broker broker3 = startBroker(nameserverAddress, controllerAddress, brokerName, 3, nextPort(), nextPort(), nextPort(), BrokerRole.SLAVE, 1700);
        waitSlaveReady(broker3.getMessageStore());
        checkMessage(broker3.getMessageStore(), topic, 10, 0);

        // Step4: put another batch message
        // Master: <Epoch1, 0, 1570> <Epoch2, 1570, 3270>
        putMessage(this.broker2.getMessageStore(), topic);
        checkMessage(broker3.getMessageStore(), topic, 20, 0);

        // Step5: Check file position, each epoch will be stored on one file(Because fileSize = 1700, which equal to 10 msg size);
        // So epoch1 was stored in firstFile, epoch2 was stored in second file, the lastFile was empty.
        final MessageStore broker2MessageStore = this.broker2.getMessageStore();
        final MappedFileQueue fileQueue = broker2MessageStore.getCommitLog().getMappedFileQueue();
        assertEquals(2, fileQueue.getTotalFileSize() / 1700);

        // Truncate epoch1's log (truncateEndOffset = 1570), which means we should delete the first file directly.
        final MappedFile firstFile = broker2MessageStore.getCommitLog().getMappedFileQueue().getFirstMappedFile();
        firstFile.shutdown(1000);
        fileQueue.retryDeleteFirstFile(1000);
        assertEquals(broker2MessageStore.getCommitLog().getMinOffset(), 1700);

        final AutoSwitchHAService haService = (AutoSwitchHAService) this.broker2.getMessageStore().getHaService();
        haService.truncateEpochFilePrefix(1570);
        checkMessage(broker2MessageStore, topic, 10, 10);

        // Step6, start broker4, link to broker2, it should sync msg from epoch2(offset = 1700).
        Broker broker4 = startBroker(nameserverAddress, controllerAddress, brokerName, 4, nextPort(), nextPort(), nextPort(), BrokerRole.SLAVE, 1700);
        waitSlaveReady(broker4.getMessageStore());
        checkMessage(broker4.getMessageStore(), topic, 10, 10);
        shutdownAndClearBroker();
    }

    public void shutdownAndClearBroker() throws InterruptedException {
        for (Broker controller : brokerList) {
            controller.shutdown();
            IOUtils.deleteFile(new File(controller.getMessageStoreConfig().getStorePathRootDir()));
        }
        brokerList.clear();
    }

    @AfterClass
    public static void destroy() {
        if (namesrvController != null) {
            namesrvController.shutdown();
        }
        if (controllerManager != null) {
            controllerManager.shutdown();
        }
        File file = new File(STORE_PATH_ROOT_PARENT_DIR);
        IOUtils.deleteFile(file);
    }

}
