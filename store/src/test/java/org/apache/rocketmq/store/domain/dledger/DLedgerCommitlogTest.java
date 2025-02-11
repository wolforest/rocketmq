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
package org.apache.rocketmq.store.domain.dledger;

import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.store.file.MmapFileList;

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageExtBatch;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.SystemUtils;
import org.apache.rocketmq.store.domain.commitlog.MessageStoreTestBase;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.api.dto.GetMessageResult;
import org.apache.rocketmq.store.api.dto.GetMessageStatus;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;
import org.apache.rocketmq.store.domain.commitlog.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.server.store.StoreCheckpoint;
import org.apache.rocketmq.store.server.config.StorePathConfigHelper;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.rocketmq.store.server.StoreTestUtil.releaseMmapFilesOnWindows;
import static org.awaitility.Awaitility.await;

public class DLedgerCommitlogTest extends MessageStoreTestBase {

    @Test
    public void testTruncateCQ() throws Exception {
        String base = createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();
        {
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
            DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
            DLedgerServer dLedgerServer = dLedgerCommitLog.getdLedgerServer();
            DLedgerMmapFileStore dLedgerMmapFileStore = (DLedgerMmapFileStore) dLedgerServer.getdLedgerStore();
            MmapFileList mmapFileList = dLedgerMmapFileStore.getDataFileList();
            Boolean success = await().atMost(Duration.ofSeconds(4)).until(() -> dLedgerCommitLog.getdLedgerServer().getMemberState().isLeader(), item -> item);
            Assert.assertTrue(success);
            doPutMessages(messageStore, topic, 0, 2000, 0);
            await().atMost(Duration.ofSeconds(10)).until(() -> 2000 == messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(24, mmapFileList.getMappedFiles().size());
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(2000, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            doGetMessages(messageStore, topic, 0, 2000, 0);
            messageStore.shutdown();
            releaseMmapFilesOnWindows(dLedgerMmapFileStore.getDataFileList().getMappedFiles());
        }

        {
            //Abnormal recover, left some commitlogs
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, true, 4);
            DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
            DLedgerServer dLedgerServer = dLedgerCommitLog.getdLedgerServer();
            DLedgerMmapFileStore dLedgerMmapFileStore = (DLedgerMmapFileStore) dLedgerServer.getdLedgerStore();
            MmapFileList mmapFileList = dLedgerMmapFileStore.getDataFileList();
            Boolean success = await().atMost(Duration.ofSeconds(4)).until(() -> dLedgerCommitLog.getdLedgerServer().getMemberState().isLeader(), item -> item);
            Assert.assertTrue(success);
            Assert.assertEquals(20, mmapFileList.getMappedFiles().size());
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1700, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            doGetMessages(messageStore, topic, 0, 1700, 0);
            messageStore.shutdown();
            releaseMmapFilesOnWindows(dLedgerMmapFileStore.getDataFileList().getMappedFiles());
        }
        {
            //Abnormal recover, left none commitlogs
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, true, 20);
            DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
            DLedgerServer dLedgerServer = dLedgerCommitLog.getdLedgerServer();
            DLedgerMmapFileStore dLedgerMmapFileStore = (DLedgerMmapFileStore) dLedgerServer.getdLedgerStore();
            MmapFileList mmapFileList = dLedgerMmapFileStore.getDataFileList();
            Boolean success = await().atMost(Duration.ofSeconds(4)).until(() -> dLedgerCommitLog.getdLedgerServer().getMemberState().isLeader(), item -> item);
            Assert.assertTrue(success);
            Assert.assertEquals(0, mmapFileList.getMappedFiles().size());
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            messageStore.shutdown();
        }
    }

    @Test
    public void testRecover() throws Exception {
        String base = createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();
        {
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
            DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
            Boolean success = await().atMost(Duration.ofSeconds(4)).until(() -> dLedgerCommitLog.getdLedgerServer().getMemberState().isLeader(), item -> item);
            Assert.assertTrue(success);
            doPutMessages(messageStore, topic, 0, 1000, 0);
            await().atMost(Duration.ofSeconds(10)).until(() -> 1000 == messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1000, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            doGetMessages(messageStore, topic, 0, 1000, 0);
            messageStore.shutdown();
        }

        {
            //normal recover
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1000, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            doGetMessages(messageStore, topic, 0, 1000, 0);
            messageStore.shutdown();
        }

        {
            //abnormal recover
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, true, 0);
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1000, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            doGetMessages(messageStore, topic, 0, 1000, 0);
            messageStore.shutdown();
        }
    }
    @Test
    public void testDLedgerAbnormallyRecover() throws Exception {
        String base = createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();

        int messageNumPerQueue = 100;

        DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
        Thread.sleep(1000);
        doPutMessages(messageStore, topic, 0, messageNumPerQueue, 0);
        doPutMessages(messageStore, topic, 1, messageNumPerQueue, 0);
        Thread.sleep(1000);
        Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
        Assert.assertEquals(messageNumPerQueue, messageStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(0, messageStore.dispatchBehindBytes());
        doGetMessages(messageStore, topic, 0, messageNumPerQueue, 0);
        StoreCheckpoint storeCheckpoint = messageStore.getStoreCheckpoint();
        storeCheckpoint.setPhysicMsgTimestamp(0);
        storeCheckpoint.setLogicsMsgTimestamp(0);
        messageStore.shutdown();

        String fileName = StorePathConfigHelper.getAbortFile(base);
        makeSureFileExists(fileName);

        File file = new File(base + File.separator + "consumequeue" + File.separator + topic + File.separator + "0" + File.separator + "00000000000000001040");
        file.delete();
//        truncateAllConsumeQueue(base + File.separator + "consumequeue" + File.separator + topic + File.separator);
        messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
        Thread.sleep(1000);
        doGetMessages(messageStore, topic, 0, messageNumPerQueue, 0);
        doGetMessages(messageStore, topic, 1, messageNumPerQueue, 0);
        messageStore.shutdown();

    }

    @Test
    public void testPutAndGetMessage() throws Exception {
        String base = createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
        DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
        Boolean success = await().atMost(Duration.ofSeconds(4)).until(() -> dLedgerCommitLog.getdLedgerServer().getMemberState().isLeader(), item -> item);
        Assert.assertTrue(success);
        String topic = UUID.randomUUID().toString();

        List<PutMessageResult> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            MessageExtBrokerInner msgInner =
                i < 5 ? buildMessage() : buildIPv6HostMessage();
            msgInner.setTopic(topic);
            msgInner.setQueueId(0);
            PutMessageResult putMessageResult = messageStore.putMessage(msgInner);
            results.add(putMessageResult);
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            Assert.assertEquals(i, putMessageResult.getAppendMessageResult().getLogicsOffset());
        }
        await().atMost(Duration.ofSeconds(10)).until(() -> 10 == messageStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
        Assert.assertEquals(10, messageStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(0, messageStore.dispatchBehindBytes());
        GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 0, 32, null);
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());

        Assert.assertEquals(10, getMessageResult.getMessageBufferList().size());
        Assert.assertEquals(10, getMessageResult.getMessageMapedList().size());

        for (int i = 0; i < results.size(); i++) {
            ByteBuffer buffer = getMessageResult.getMessageBufferList().get(i);
            MessageExt messageExt = MessageDecoder.decode(buffer);
            Assert.assertEquals(i, messageExt.getQueueOffset());
            Assert.assertEquals(results.get(i).getAppendMessageResult().getMsgId(), messageExt.getMsgId());
            Assert.assertEquals(results.get(i).getAppendMessageResult().getWroteOffset(), messageExt.getCommitLogOffset());
        }
        messageStore.destroy();
        messageStore.shutdown();
    }

    @Test
    public void testBatchPutAndGetMessage() throws Exception {
        String base = createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
        DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
        Boolean success = await().atMost(Duration.ofSeconds(4)).until(() -> dLedgerCommitLog.getdLedgerServer().getMemberState().isLeader(), item -> item);
        Assert.assertTrue(success);
        String topic = UUID.randomUUID().toString();
        // should be less than 4
        int batchMessageSize = 2;
        int repeat = 10;
        List<PutMessageResult> results = new ArrayList<>();
        for (int i = 0; i < repeat; i++) {
            MessageExtBatch messageExtBatch =
                i < repeat / 10 ? buildBatchMessage(batchMessageSize) : buildIPv6HostBatchMessage(batchMessageSize);
            messageExtBatch.setTopic(topic);
            messageExtBatch.setQueueId(0);
            PutMessageResult putMessageResult = messageStore.putMessages(messageExtBatch);
            results.add(putMessageResult);
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            Assert.assertEquals(i * batchMessageSize, putMessageResult.getAppendMessageResult().getLogicsOffset());
        }
        await().atMost(Duration.ofSeconds(10)).until(() -> repeat * batchMessageSize == messageStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
        Assert.assertEquals(repeat * batchMessageSize, messageStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(0, messageStore.dispatchBehindBytes());
        GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 0, 100, null);
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());

        Assert.assertEquals(repeat * batchMessageSize > 32 ? 32 : repeat * batchMessageSize, getMessageResult.getMessageBufferList().size());
        Assert.assertEquals(repeat * batchMessageSize > 32 ? 32 : repeat * batchMessageSize, getMessageResult.getMessageMapedList().size());
        Assert.assertEquals(repeat * batchMessageSize, getMessageResult.getMaxOffset());

        for (int i = 0; i < results.size(); i++) {
            ByteBuffer buffer = getMessageResult.getMessageBufferList().get(i * batchMessageSize);
            MessageExt messageExt = MessageDecoder.decode(buffer);
            Assert.assertEquals(i * batchMessageSize, messageExt.getQueueOffset());
            Assert.assertEquals(results.get(i).getAppendMessageResult().getMsgId().split(",").length, batchMessageSize);
            Assert.assertEquals(results.get(i).getAppendMessageResult().getWroteOffset(), messageExt.getCommitLogOffset());
        }
        messageStore.destroy();
        messageStore.shutdown();
    }

    @Test
    public void testAsyncPutAndGetMessage() throws Exception {
        Assume.assumeFalse(SystemUtils.isWindows());
        String base = createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
        DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
        Boolean success = await().atMost(Duration.ofSeconds(4)).until(() -> dLedgerCommitLog.getdLedgerServer().getMemberState().isLeader(), item -> item);
        Assert.assertTrue(success);
        String topic = UUID.randomUUID().toString();

        List<PutMessageResult> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            MessageExtBrokerInner msgInner =
                i < 5 ? buildMessage() : buildIPv6HostMessage();
            msgInner.setTopic(topic);
            msgInner.setQueueId(0);
            CompletableFuture<PutMessageResult> futureResult = messageStore.asyncPutMessage(msgInner);
            PutMessageResult putMessageResult = futureResult.get(3000, TimeUnit.MILLISECONDS);
            results.add(putMessageResult);
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            Assert.assertEquals(i, putMessageResult.getAppendMessageResult().getLogicsOffset());
        }
        await().atMost(Duration.ofSeconds(10)).until(() -> 10 == messageStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
        Assert.assertEquals(10, messageStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(0, messageStore.dispatchBehindBytes());
        GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 0, 32, null);
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());

        Assert.assertEquals(10, getMessageResult.getMessageBufferList().size());
        Assert.assertEquals(10, getMessageResult.getMessageMapedList().size());

        for (int i = 0; i < results.size(); i++) {
            ByteBuffer buffer = getMessageResult.getMessageBufferList().get(i);
            MessageExt messageExt = MessageDecoder.decode(buffer);
            Assert.assertEquals(i, messageExt.getQueueOffset());
            Assert.assertEquals(results.get(i).getAppendMessageResult().getMsgId(), messageExt.getMsgId());
            Assert.assertEquals(results.get(i).getAppendMessageResult().getWroteOffset(), messageExt.getCommitLogOffset());
        }
        messageStore.destroy();
        messageStore.shutdown();
    }

    @Test
    public void testAsyncBatchPutAndGetMessage() throws Exception {
        String base = createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
        DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
        Boolean success = await().atMost(Duration.ofSeconds(4)).until(() -> dLedgerCommitLog.getdLedgerServer().getMemberState().isLeader(), item -> item);
        Assert.assertTrue(success);
        String topic = UUID.randomUUID().toString();
        // should be less than 4
        int batchMessageSize = 2;
        int repeat = 10;

        List<PutMessageResult> results = new ArrayList<>();
        for (int i = 0; i < repeat; i++) {
            MessageExtBatch messageExtBatch =
                i < 5 ? buildBatchMessage(batchMessageSize) : buildIPv6HostBatchMessage(batchMessageSize);
            messageExtBatch.setTopic(topic);
            messageExtBatch.setQueueId(0);
            CompletableFuture<PutMessageResult> futureResult = messageStore.asyncPutMessages(messageExtBatch);
            PutMessageResult putMessageResult = futureResult.get(3000, TimeUnit.MILLISECONDS);
            results.add(putMessageResult);
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            Assert.assertEquals(i * batchMessageSize, putMessageResult.getAppendMessageResult().getLogicsOffset());
        }
        await().atMost(Duration.ofSeconds(10)).until(() -> repeat * batchMessageSize == messageStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
        Assert.assertEquals(repeat * batchMessageSize, messageStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(0, messageStore.dispatchBehindBytes());
        GetMessageResult getMessageResult = messageStore.getMessage("group", topic, 0, 0, 32, null);
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());

        Assert.assertEquals(repeat * batchMessageSize > 32 ? 32 : repeat * batchMessageSize, getMessageResult.getMessageBufferList().size());
        Assert.assertEquals(repeat * batchMessageSize > 32 ? 32 : repeat * batchMessageSize, getMessageResult.getMessageMapedList().size());
        Assert.assertEquals(repeat * batchMessageSize, getMessageResult.getMaxOffset());

        for (int i = 0; i < results.size(); i++) {
            ByteBuffer buffer = getMessageResult.getMessageBufferList().get(i * batchMessageSize);
            MessageExt messageExt = MessageDecoder.decode(buffer);
            Assert.assertEquals(i * batchMessageSize, messageExt.getQueueOffset());
            Assert.assertEquals(results.get(i).getAppendMessageResult().getMsgId().split(",").length, batchMessageSize);
            Assert.assertEquals(results.get(i).getAppendMessageResult().getWroteOffset(), messageExt.getCommitLogOffset());
        }
        messageStore.destroy();
        messageStore.shutdown();
    }

    @Test
    public void testCommittedPos() throws Exception {
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());
        String group = UUID.randomUUID().toString();
        DefaultMessageStore leaderStore = createDledgerMessageStore(createBaseDir(), group, "n0", peers, "n0", false, 0);

        String topic = UUID.randomUUID().toString();
        MessageExtBrokerInner msgInner = buildMessage();
        msgInner.setTopic(topic);
        msgInner.setQueueId(0);
        PutMessageResult putMessageResult = leaderStore.putMessage(msgInner);
        Assert.assertEquals(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, putMessageResult.getPutMessageStatus());

        Assert.assertEquals(0, leaderStore.getCommitLog().getMaxOffset());
        Assert.assertEquals(0, leaderStore.getMaxOffsetInQueue(topic, 0));

        DefaultMessageStore followerStore = createDledgerMessageStore(createBaseDir(), group, "n1", peers, "n0", false, 0);
        await().atMost(10, SECONDS).until(followerCatchesUp(followerStore, topic));

        Assert.assertEquals(1, leaderStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertTrue(leaderStore.getCommitLog().getMaxOffset() > 0);

        leaderStore.destroy();
        followerStore.destroy();

        leaderStore.shutdown();
        followerStore.shutdown();
    }

    @Test
    public void testIPv6HostMsgCommittedPos() throws Exception {
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());
        String group = UUID.randomUUID().toString();
        DefaultMessageStore leaderStore = createDledgerMessageStore(createBaseDir(), group, "n0", peers, "n0", false, 0);

        String topic = UUID.randomUUID().toString();
        MessageExtBrokerInner msgInner = buildIPv6HostMessage();
        msgInner.setTopic(topic);
        msgInner.setQueueId(0);
        PutMessageResult putMessageResult = leaderStore.putMessage(msgInner);
        Assert.assertEquals(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, putMessageResult.getPutMessageStatus());

        //Thread.sleep(1000);

        Assert.assertEquals(0, leaderStore.getCommitLog().getMaxOffset());
        Assert.assertEquals(0, leaderStore.getMaxOffsetInQueue(topic, 0));

        DefaultMessageStore followerStore = createDledgerMessageStore(createBaseDir(), group, "n1", peers, "n0", false, 0);
        await().atMost(10, SECONDS).until(followerCatchesUp(followerStore, topic));

        Assert.assertEquals(1, leaderStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertTrue(leaderStore.getCommitLog().getMaxOffset() > 0);

        leaderStore.destroy();
        followerStore.destroy();

        leaderStore.shutdown();
        followerStore.shutdown();
    }

    private Callable<Boolean> followerCatchesUp(DefaultMessageStore followerStore, String topic) {
        return () -> followerStore.getMaxOffsetInQueue(topic, 0) == 1;
    }
}
