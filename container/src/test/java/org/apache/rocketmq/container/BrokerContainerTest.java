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

package org.apache.rocketmq.container;

import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.infra.ClusterClient;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.app.BrokerIdentity;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.domain.constant.PermName;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.common.utils.NetworkUtils;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.store.server.config.BrokerRole;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BrokerContainerTest {
    private static final List<File> TMP_FILE_LIST = new ArrayList<>();
    private static final Random RANDOM = new Random();
    private static final Set<Integer> PORTS_IN_USE = new HashSet<>();

    /**
     * Tests if the controller can be properly stopped and started.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testBrokerContainerRestart() throws Exception {
        BrokerContainer brokerController = new BrokerContainer(
                new BrokerContainerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig());
        assertThat(brokerController.initialize()).isTrue();
        brokerController.start();
        brokerController.shutdown();
    }

    @Test
    public void testRegisterIncrementBrokerData() throws Exception {

        Broker broker = new Broker(
                new BrokerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig(),
                new MessageStoreConfig());
        broker.getBrokerConfig().setEnableSlaveActingMaster(true);

        ClusterClient clusterClient = mock(ClusterClient.class);
        Method method = Broker.class.getDeclaredMethod("setBrokerOuterAPI", ClusterClient.class);
        method.setAccessible(true);
        method.invoke(broker, clusterClient);
        List<TopicConfig> topicConfigList = new ArrayList<>(2);
        for (int i = 0; i < 2; i++) {
            topicConfigList.add(new TopicConfig("topic-" + i));
        }
        DataVersion dataVersion = new DataVersion();

        // Check normal condition.
        testRegisterIncrementBrokerDataWithPerm(broker, clusterClient,
                topicConfigList, dataVersion, PermName.PERM_READ | PermName.PERM_WRITE, 1);
        // Check unwritable broker.
        testRegisterIncrementBrokerDataWithPerm(broker, clusterClient,
                topicConfigList, dataVersion, PermName.PERM_READ, 2);
        // Check unreadable broker.
        testRegisterIncrementBrokerDataWithPerm(broker, clusterClient,
                topicConfigList, dataVersion, PermName.PERM_WRITE, 3);
    }

    @Test
    public void testRegisterIncrementBrokerDataPerm() throws Exception {
        Broker broker = new Broker(
                new BrokerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig(),
                new MessageStoreConfig());
        broker.getBrokerConfig().setEnableSlaveActingMaster(true);

        ClusterClient clusterClient = mock(ClusterClient.class);
        Method method = Broker.class.getDeclaredMethod("setBrokerOuterAPI", ClusterClient.class);
        method.setAccessible(true);
        method.invoke(broker, clusterClient);

        List<TopicConfig> topicConfigList = new ArrayList<>(2);
        for (int i = 0; i < 2; i++) {
            topicConfigList.add(new TopicConfig("topic-" + i));
        }
        DataVersion dataVersion = new DataVersion();

        broker.getBrokerConfig().setBrokerPermission(4);

        broker.getBrokerServiceRegistry().registerIncrementBrokerData(topicConfigList, dataVersion);
        // Get topicConfigSerializeWrapper created by registerIncrementBrokerData() from brokerOuterAPI.registerBrokerAll()
        ArgumentCaptor<TopicConfigSerializeWrapper> captor = ArgumentCaptor.forClass(TopicConfigSerializeWrapper.class);
        ArgumentCaptor<BrokerIdentity> brokerIdentityCaptor = ArgumentCaptor.forClass(BrokerIdentity.class);
        verify(clusterClient).registerBrokerAll(anyString(), anyString(), anyString(), anyLong(), anyString(),
                captor.capture(), ArgumentMatchers.anyList(), anyBoolean(), anyInt(), anyBoolean(), anyBoolean(), anyLong(), brokerIdentityCaptor.capture());
        TopicConfigSerializeWrapper wrapper = captor.getValue();
        for (Map.Entry<String, TopicConfig> entry : wrapper.getTopicConfigTable().entrySet()) {
            assertThat(entry.getValue().getPerm()).isEqualTo(broker.getBrokerConfig().getBrokerPermission());
        }

    }

    @Test
    public void testMasterScaleOut() throws Exception {
        BrokerContainer brokerContainer = new BrokerContainer(
                new BrokerContainerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig());
        assertThat(brokerContainer.initialize()).isTrue();
        brokerContainer.getBrokerContainerConfig().setNamesrvAddr("127.0.0.1:9876");
        brokerContainer.start();

        BrokerConfig masterBrokerConfig = new BrokerConfig();

        String baseDir = createBaseDir("unnittest-master").getAbsolutePath();
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(baseDir);
        messageStoreConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");
        InnerBrokerController brokerController = brokerContainer.addBroker(masterBrokerConfig, messageStoreConfig);
        assertThat(brokerController.isIsolated()).isFalse();

        brokerContainer.shutdown();
        brokerController.getMessageStore().destroy();
    }

    @Test
    public void testAddMasterFailed() throws Exception {
        BrokerContainer brokerContainer = new BrokerContainer(
                new BrokerContainerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig());
        assertThat(brokerContainer.initialize()).isTrue();
        brokerContainer.start();

        BrokerConfig masterBrokerConfig = new BrokerConfig();
        masterBrokerConfig.setListenPort(brokerContainer.getNettyServerConfig().getListenPort());
        boolean exceptionCaught = false;
        try {
            String baseDir = createBaseDir("unnittest-master").getAbsolutePath();
            MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
            messageStoreConfig.setStorePathRootDir(baseDir);
            messageStoreConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");
            brokerContainer.addBroker(masterBrokerConfig, messageStoreConfig);
        } catch (Exception e) {
            exceptionCaught = true;
        } finally {
            brokerContainer.shutdown();

        }

        assertThat(exceptionCaught).isTrue();
    }

    @Test
    public void testAddSlaveFailed() throws Exception {
        BrokerContainer sharedBrokerController = new BrokerContainer(
                new BrokerContainerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig());
        assertThat(sharedBrokerController.initialize()).isTrue();
        sharedBrokerController.start();

        BrokerConfig slaveBrokerConfig = new BrokerConfig();
        slaveBrokerConfig.setBrokerId(1);
        slaveBrokerConfig.setListenPort(sharedBrokerController.getNettyServerConfig().getListenPort());
        MessageStoreConfig slaveMessageStoreConfig = new MessageStoreConfig();
        slaveMessageStoreConfig.setBrokerRole(BrokerRole.SLAVE);
        String baseDir = createBaseDir("unnittest-slave").getAbsolutePath();
        slaveMessageStoreConfig.setStorePathRootDir(baseDir);
        slaveMessageStoreConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");
        boolean exceptionCaught = false;
        try {
            sharedBrokerController.addBroker(slaveBrokerConfig, slaveMessageStoreConfig);
        } catch (Exception e) {
            exceptionCaught = true;
        } finally {
            sharedBrokerController.shutdown();
        }

        assertThat(exceptionCaught).isTrue();
    }

    //@Test
    public void testAddAndRemoveMaster() throws Exception {
        BrokerContainer brokerContainer = new BrokerContainer(
                new BrokerContainerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig());
        assertThat(brokerContainer.initialize()).isTrue();
        brokerContainer.start();
        BrokerConfig masterBrokerConfig = new BrokerConfig();
        String baseDir = createBaseDir("unnittest-master").getAbsolutePath();
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(baseDir);
        messageStoreConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");

        InnerBrokerController master = brokerContainer.addBroker(masterBrokerConfig, messageStoreConfig);

        assertThat(master).isNotNull();
        master.start();
        assertThat(master.isIsolated()).isFalse();

        brokerContainer.removeBroker(new BrokerIdentity(masterBrokerConfig.getBrokerClusterName(), masterBrokerConfig.getBrokerName(), masterBrokerConfig.getBrokerId()));
        assertThat(brokerContainer.getMasterBrokers().size()).isEqualTo(0);

        brokerContainer.shutdown();
        master.getMessageStore().destroy();
    }

    @Test
    public void testAddAndRemoveDLedgerBroker() throws Exception {
        BrokerContainer brokerContainer = new BrokerContainer(
                new BrokerContainerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig());
        assertThat(brokerContainer.initialize()).isTrue();
        brokerContainer.start();

        BrokerConfig dLedgerBrokerConfig = new BrokerConfig();
        String baseDir = createBaseDir("unnittest-dLedger").getAbsolutePath();
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(baseDir);
        messageStoreConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");
        messageStoreConfig.setEnableDLegerCommitLog(true);
        messageStoreConfig.setdLegerSelfId("n0");
        messageStoreConfig.setdLegerGroup("group");
        messageStoreConfig.setHaListenPort(generatePort());
        dLedgerBrokerConfig.setListenPort(generatePort());
        messageStoreConfig.setdLegerPeers(String.format("n0-localhost:%d", generatePort()));
        InnerBrokerController dLedger = brokerContainer.addBroker(dLedgerBrokerConfig, messageStoreConfig);
        assertThat(dLedger).isNotNull();
        dLedger.start();
        assertThat(dLedger.isIsolated()).isFalse();

        brokerContainer.removeBroker(new BrokerIdentity(dLedgerBrokerConfig.getBrokerClusterName(), dLedgerBrokerConfig.getBrokerName(), Integer.parseInt(messageStoreConfig.getdLegerSelfId().substring(1))));
        assertThat(brokerContainer.getMasterBrokers().size()).isEqualTo(0);

        brokerContainer.shutdown();
        dLedger.getMessageStore().destroy();
    }

    @Test
    public void testAddAndRemoveSlaveSuccess() throws Exception {
        BrokerContainer brokerContainer = new BrokerContainer(
                new BrokerContainerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig());
        assertThat(brokerContainer.initialize()).isTrue();
        brokerContainer.start();

        BrokerConfig masterBrokerConfig = new BrokerConfig();
        masterBrokerConfig.setListenPort(NetworkUtils.nextPort());
        String baseDir = createBaseDir("unnittest-master").getAbsolutePath();
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(baseDir);
        messageStoreConfig.setHaListenPort(NetworkUtils.nextPort());
        messageStoreConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");
        InnerBrokerController master = brokerContainer.addBroker(masterBrokerConfig, messageStoreConfig);
        assertThat(master).isNotNull();
        master.start();
        assertThat(master.isIsolated()).isFalse();

        BrokerConfig slaveBrokerConfig = new BrokerConfig();
        slaveBrokerConfig.setListenPort(generatePort());
        slaveBrokerConfig.setBrokerId(1);
        MessageStoreConfig slaveMessageStoreConfig = new MessageStoreConfig();
        slaveMessageStoreConfig.setBrokerRole(BrokerRole.SLAVE);
        slaveMessageStoreConfig.setHaListenPort(generatePort());
        baseDir = createBaseDir("unnittest-slave").getAbsolutePath();
        slaveMessageStoreConfig.setStorePathRootDir(baseDir);
        slaveMessageStoreConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");
        InnerBrokerController slave = brokerContainer.addBroker(slaveBrokerConfig, slaveMessageStoreConfig);
        assertThat(slave).isNotNull();
        slave.start();
        assertThat(slave.isIsolated()).isFalse();

        brokerContainer.removeBroker(new BrokerIdentity(slaveBrokerConfig.getBrokerClusterName(), slaveBrokerConfig.getBrokerName(), slaveBrokerConfig.getBrokerId()));
        assertThat(brokerContainer.getSlaveBrokers().size()).isEqualTo(0);

        brokerContainer.removeBroker(new BrokerIdentity(masterBrokerConfig.getBrokerClusterName(), masterBrokerConfig.getBrokerName(), masterBrokerConfig.getBrokerId()));
        assertThat(brokerContainer.getMasterBrokers().size()).isEqualTo(0);

        brokerContainer.shutdown();
        slave.getMessageStore().destroy();
        master.getMessageStore().destroy();
    }

    private static File createBaseDir(String prefix) {
        final File file;
        try {
            file = Files.createTempDirectory(prefix).toFile();
            TMP_FILE_LIST.add(file);
            return file;
        } catch (IOException e) {
            throw new RuntimeException("Couldn't create tmp folder", e);
        }
    }

    public static int generatePort() {
        int result = NetworkUtils.nextPort();
        while (PORTS_IN_USE.contains(result) || PORTS_IN_USE.contains(result - 2)) {
            result = NetworkUtils.nextPort();
        }
        PORTS_IN_USE.add(result);
        PORTS_IN_USE.add(result - 2);
        return result;
    }

    @After
    public void destroy() {
        for (File file : TMP_FILE_LIST) {
            IOUtils.deleteFile(file);
        }
    }

    private void testRegisterIncrementBrokerDataWithPerm(Broker broker,
                                                         ClusterClient clusterClient,
                                                         List<TopicConfig> topicConfigList, DataVersion dataVersion, int perm, int times) {
        broker.getBrokerConfig().setBrokerPermission(perm);

        broker.getBrokerServiceRegistry().registerIncrementBrokerData(topicConfigList, dataVersion);
        // Get topicConfigSerializeWrapper created by registerIncrementBrokerData() from brokerOuterAPI.registerBrokerAll()
        ArgumentCaptor<TopicConfigSerializeWrapper> captor = ArgumentCaptor.forClass(TopicConfigSerializeWrapper.class);
        ArgumentCaptor<BrokerIdentity> brokerIdentityCaptor = ArgumentCaptor.forClass(BrokerIdentity.class);
        verify(clusterClient, times(times)).registerBrokerAll(anyString(), anyString(), anyString(), anyLong(),
                anyString(), captor.capture(), ArgumentMatchers.anyList(), anyBoolean(), anyInt(), anyBoolean(), anyBoolean(), anyLong(), brokerIdentityCaptor.capture());
        TopicConfigSerializeWrapper wrapper = captor.getValue();

        for (TopicConfig topicConfig : topicConfigList) {
            topicConfig.setPerm(perm);
        }
        assertThat(wrapper.getDataVersion()).isEqualTo(dataVersion);
        assertThat(wrapper.getTopicConfigTable()).containsExactly(
                entry("topic-0", topicConfigList.get(0)),
                entry("topic-1", topicConfigList.get(1)));
        for (TopicConfig topicConfig : topicConfigList) {
            topicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
        }
    }
}
