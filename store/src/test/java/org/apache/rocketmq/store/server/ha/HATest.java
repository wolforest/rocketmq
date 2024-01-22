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

package org.apache.rocketmq.store.server.ha;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.common.utils.SystemUtils;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.api.broker.stats.BrokerStatsManager;
import org.apache.rocketmq.store.api.dto.GetMessageResult;
import org.apache.rocketmq.store.api.dto.GetMessageStatus;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;
import org.apache.rocketmq.store.server.ha.core.HAConnectionState;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.server.config.BrokerRole;
import org.apache.rocketmq.store.server.config.FlushDiskType;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.server.daemon.ReputMessageService;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class HATest {
    private final String storeMessage = "Once, there was a chance for me!";
    private int queueTotal = 100;
    private final AtomicInteger queueId = new AtomicInteger(0);
    private SocketAddress bornHost;
    private SocketAddress storeHost;
    private byte[] messageBody;

    private MessageStore masterStore;
    private MessageStore slaveStore;
    private MessageStoreConfig masterStoreConfig;

    private final BrokerStatsManager brokerStatsManager = new BrokerStatsManager("simpleTest", true);
    private final String storePathRootParentDir = System.getProperty("java.io.tmpdir") + File.separator + "rocketmq-test" + File.separator + UUID.randomUUID();
    private final String storePathRootDir = storePathRootParentDir + File.separator + "store";

    @Before
    public void init() throws Exception {
        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);

        buildMasterStoreConfig();
        MessageStoreConfig slaveStoreConfig = buildSlaveStoreConfig();

        masterStore = buildMessageStore(masterStoreConfig, 0L);
        slaveStore = buildMessageStore(slaveStoreConfig, 1L);

        boolean masterLoad = masterStore.load();
        boolean slaveLoad = slaveStore.load();
        assertTrue(masterLoad);
        assertTrue(slaveLoad);

        masterStore.start();
        slaveStore.updateHaMasterAddress("127.0.0.1:" + masterStoreConfig.getHaListenPort());

        slaveStore.start();
        slaveStore.updateHaMasterAddress("127.0.0.1:" + masterStoreConfig.getHaListenPort());

        await().atMost(6, SECONDS).until(
            () -> slaveStore.getHaService().getHAClient().getCurrentState() == HAConnectionState.TRANSFER
        );
    }

    @Test
    public void testHandleHA() {
        long totalMsgs = 10;
        queueTotal = 1;
        messageBody = storeMessage.getBytes();
        for (long i = 0; i < totalMsgs; i++) {
            masterStore.putMessage(buildMessage());
        }
        for (long i = 0; i < totalMsgs; i++) {
            final long index = i;
            Boolean exist = await().atMost(Duration.ofSeconds(5)).until(() -> {
                GetMessageResult result = slaveStore.getMessage("GROUP_A", "FooBar", 0, index, 1024 * 1024, null);
                if (result == null) {
                    return false;
                }
                boolean flag = GetMessageStatus.FOUND == result.getStatus();
                result.release();
                return flag;

            }, item -> item);
            assertTrue(exist);
        }
    }

    @Test
    public void testSemiSyncReplica() throws Exception {
        long totalMsgs = 5;
        queueTotal = 1;
        messageBody = storeMessage.getBytes();
        for (long i = 0; i < totalMsgs; i++) {
            MessageExtBrokerInner msg = buildMessage();
            CompletableFuture<PutMessageResult> putResultFuture = masterStore.asyncPutMessage(msg);
            PutMessageResult result = putResultFuture.get();
            assertEquals(PutMessageStatus.PUT_OK, result.getPutMessageStatus());
            //message has been replicated to slave's commitLog, but maybe not dispatch to ConsumeQueue yet
            //so direct read from commitLog by physical offset
            MessageExt slaveMsg = slaveStore.lookMessageByOffset(result.getAppendMessageResult().getWroteOffset());
            assertNotNull(slaveMsg);
            assertArrayEquals(msg.getBody(), slaveMsg.getBody());
            assertEquals(msg.getTopic(), slaveMsg.getTopic());
            assertEquals(msg.getTags(), slaveMsg.getTags());
            assertEquals(msg.getKeys(), slaveMsg.getKeys());
        }
        //shutdown slave, putMessage should return FLUSH_SLAVE_TIMEOUT
        slaveStore.shutdown();

        //wait to let master clean the slave's connection
        await().atMost(Duration.ofSeconds(3)).until(() -> masterStore.getHaService().getConnectionCount().get() == 0);
        for (long i = 0; i < totalMsgs; i++) {
            CompletableFuture<PutMessageResult> putResultFuture = masterStore.asyncPutMessage(buildMessage());
            PutMessageResult result = putResultFuture.get();
            assertEquals(PutMessageStatus.FLUSH_SLAVE_TIMEOUT, result.getPutMessageStatus());
        }
    }

    @Test
    public void testSemiSyncReplicaWhenSlaveActingMaster() throws Exception {
        // SKip Mac
        Assume.assumeFalse(SystemUtils.isMac());

        long totalMsgs = 5;
        queueTotal = 1;
        messageBody = storeMessage.getBytes();
        ((DefaultMessageStore) masterStore).getBrokerConfig().setEnableSlaveActingMaster(true);
        for (long i = 0; i < totalMsgs; i++) {
            MessageExtBrokerInner msg = buildMessage();
            CompletableFuture<PutMessageResult> putResultFuture = masterStore.asyncPutMessage(msg);
            PutMessageResult result = putResultFuture.get();
            assertEquals(PutMessageStatus.PUT_OK, result.getPutMessageStatus());
            //message has been replicated to slave's commitLog, but maybe not dispatch to ConsumeQueue yet
            //so direct read from commitLog by physical offset
            MessageExt slaveMsg = slaveStore.lookMessageByOffset(result.getAppendMessageResult().getWroteOffset());
            assertNotNull(slaveMsg);
            assertArrayEquals(msg.getBody(), slaveMsg.getBody());
            assertEquals(msg.getTopic(), slaveMsg.getTopic());
            assertEquals(msg.getTags(), slaveMsg.getTags());
            assertEquals(msg.getKeys(), slaveMsg.getKeys());
        }

        //shutdown slave, putMessage should return IN_SYNC_REPLICAS_NOT_ENOUGH
        slaveStore.shutdown();
        masterStore.setAliveReplicaNumInGroup(1);

        //wait to let master clean the slave's connection
        Thread.sleep(masterStoreConfig.getHaHousekeepingInterval() + 500);
        for (long i = 0; i < totalMsgs; i++) {
            CompletableFuture<PutMessageResult> putResultFuture = masterStore.asyncPutMessage(buildMessage());
            PutMessageResult result = putResultFuture.get();
            assertEquals(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, result.getPutMessageStatus());
        }

        ((DefaultMessageStore) masterStore).getBrokerConfig().setEnableSlaveActingMaster(false);
    }

    @Test
    public void testSemiSyncReplicaWhenAdaptiveDegradation() throws Exception {
        long totalMsgs = 5;
        queueTotal = 1;
        messageBody = storeMessage.getBytes();
        ((DefaultMessageStore) masterStore).getBrokerConfig().setEnableSlaveActingMaster(true);
        masterStore.getMessageStoreConfig().setEnableAutoInSyncReplicas(true);
        for (long i = 0; i < totalMsgs; i++) {
            MessageExtBrokerInner msg = buildMessage();
            CompletableFuture<PutMessageResult> putResultFuture = masterStore.asyncPutMessage(msg);
            PutMessageResult result = putResultFuture.get();
            assertEquals(PutMessageStatus.PUT_OK, result.getPutMessageStatus());
            //message has been replicated to slave's commitLog, but maybe not dispatch to ConsumeQueue yet
            //so direct read from commitLog by physical offset
            final MessageExt[] slaveMsg = {null};
            await().atMost(Duration.ofSeconds(3)).until(() -> {
                slaveMsg[0] = slaveStore.lookMessageByOffset(result.getAppendMessageResult().getWroteOffset());
                return slaveMsg[0] != null;
            });
            assertArrayEquals(msg.getBody(), slaveMsg[0].getBody());
            assertEquals(msg.getTopic(), slaveMsg[0].getTopic());
            assertEquals(msg.getTags(), slaveMsg[0].getTags());
            assertEquals(msg.getKeys(), slaveMsg[0].getKeys());
        }

        //shutdown slave, putMessage should return IN_SYNC_REPLICAS_NOT_ENOUGH
        slaveStore.shutdown();
        masterStore.setAliveReplicaNumInGroup(1);

        //wait to let master clean the slave's connection
        await().atMost(Duration.ofSeconds(3)).until(() -> masterStore.getHaService().getConnectionCount().get() == 0);
        for (long i = 0; i < totalMsgs; i++) {
            CompletableFuture<PutMessageResult> putResultFuture = masterStore.asyncPutMessage(buildMessage());
            PutMessageResult result = putResultFuture.get();
            assertEquals(PutMessageStatus.PUT_OK, result.getPutMessageStatus());
        }

        ((DefaultMessageStore) masterStore).getBrokerConfig().setEnableSlaveActingMaster(false);
        masterStore.getMessageStoreConfig().setEnableAutoInSyncReplicas(false);
    }

    @After
    public void destroy() throws Exception {

        slaveStore.shutdown();
        slaveStore.destroy();
        masterStore.shutdown();
        masterStore.destroy();
        File file = new File(storePathRootParentDir);
        IOUtils.deleteFile(file);
    }

    private MessageStore buildMessageStore(MessageStoreConfig messageStoreConfig, long brokerId) throws Exception {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerId(brokerId);
        return new DefaultMessageStore(messageStoreConfig, brokerStatsManager, null, brokerConfig, new ConcurrentHashMap<>());
    }

    private void buildMasterStoreConfig() {
        masterStoreConfig = new MessageStoreConfig();
        masterStoreConfig.setBrokerRole(BrokerRole.SYNC_MASTER);
        masterStoreConfig.setStorePathRootDir(storePathRootDir + File.separator + "master");
        masterStoreConfig.setStorePathCommitLog(storePathRootDir + File.separator + "master" + File.separator + "commitLog");
        masterStoreConfig.setHaListenPort(0);
        masterStoreConfig.setTotalReplicas(2);
        masterStoreConfig.setInSyncReplicas(2);
        masterStoreConfig.setHaHousekeepingInterval(2 * 1000);
        masterStoreConfig.setHaSendHeartbeatInterval(1000);
        buildMessageStoreConfig(masterStoreConfig);
    }

    private MessageStoreConfig buildSlaveStoreConfig() {
        MessageStoreConfig slaveStoreConfig = new MessageStoreConfig();
        slaveStoreConfig.setBrokerRole(BrokerRole.SLAVE);
        slaveStoreConfig.setStorePathRootDir(storePathRootDir + File.separator + "slave");
        slaveStoreConfig.setStorePathCommitLog(storePathRootDir + File.separator + "slave" + File.separator + "commitLog");
        slaveStoreConfig.setHaListenPort(0);
        slaveStoreConfig.setTotalReplicas(2);
        slaveStoreConfig.setInSyncReplicas(2);
        slaveStoreConfig.setHaHousekeepingInterval(2 * 1000);
        slaveStoreConfig.setHaSendHeartbeatInterval(1000);
        buildMessageStoreConfig(slaveStoreConfig);

        return slaveStoreConfig;
    }

    private void buildMessageStoreConfig(MessageStoreConfig messageStoreConfig) {
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
    }

    private MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("FooBar");
        msg.setTags("TAG1");
        msg.setBody(messageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(queueId.getAndIncrement()) % queueTotal);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        return msg;
    }

    private boolean isCommitLogAvailable(DefaultMessageStore store) {
        try {
            Field serviceField = store.getClass().getDeclaredField("reputMessageService");
            serviceField.setAccessible(true);
            ReputMessageService reputService =
                (ReputMessageService) serviceField.get(store);

            Method method = ReputMessageService.class.getDeclaredMethod("isCommitLogAvailable");
            method.setAccessible(true);
            return (boolean) method.invoke(reputService);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

}
