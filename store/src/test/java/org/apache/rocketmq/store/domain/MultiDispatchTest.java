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

package org.apache.rocketmq.store.domain;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.store.domain.queue.MultiDispatchUtils;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.domain.dispatcher.MultiDispatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiDispatchTest {

    private MultiDispatch multiDispatch;

    private DefaultMessageStore messageStore;

    @Before
    public void init() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 8);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 4);
        messageStoreConfig.setMaxHashSlotNum(100);
        messageStoreConfig.setMaxIndexNum(100 * 10);
        messageStoreConfig.setStorePathRootDir(System.getProperty("java.io.tmpdir") + File.separator + "unitteststore1");
        messageStoreConfig.setStorePathCommitLog(
            System.getProperty("java.io.tmpdir") + File.separator + "unitteststore1" + File.separator + "commitlog");

        messageStoreConfig.setEnableLmq(true);
        messageStoreConfig.setEnableMultiDispatch(true);
        BrokerConfig brokerConfig = new BrokerConfig();
        //too much reference
        messageStore = new DefaultMessageStore(messageStoreConfig, null, null, brokerConfig, new ConcurrentHashMap<>());
        multiDispatch = new MultiDispatch(messageStore);
    }

    @After
    public void destroy() {
        IOUtils.deleteFile(new File(System.getProperty("java.io.tmpdir") + File.separator + "unitteststore1"));
    }

    @Test
    public void lmqQueueKey() {
        MessageExtBrokerInner messageExtBrokerInner = mock(MessageExtBrokerInner.class);
        when(messageExtBrokerInner.getQueueId()).thenReturn(2);
        String ret = MultiDispatchUtils.lmqQueueKey("%LMQ%lmq123");
        assertEquals(ret, "%LMQ%lmq123-0");
    }

    @Test
    public void wrapMultiDispatch() throws RocksDBException {
        MessageExtBrokerInner messageExtBrokerInner = buildMessageMultiQueue();
        multiDispatch.wrapMultiDispatch(messageExtBrokerInner);
        assertEquals(messageExtBrokerInner.getProperty(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET), "0,0");
    }

    private MessageExtBrokerInner buildMessageMultiQueue() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("test");
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody("aaa".getBytes(Charset.forName("UTF-8")));
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(0);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(new InetSocketAddress("127.0.0.1", 54270));
        msg.setBornHost(new InetSocketAddress("127.0.0.1", 10911));
        for (int i = 0; i < 1; i++) {
            msg.putUserProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH, "%LMQ%123,%LMQ%456");
        }
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

        return msg;
    }
}