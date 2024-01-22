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

package org.apache.rocketmq.store.server.ha.core;

import java.time.Duration;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Test;

import static org.awaitility.Awaitility.await;

public class FlowMonitorThreadTest {

    @Test
    public void testLimit() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setHaFlowControlEnable(true);
        messageStoreConfig.setMaxHaTransferByteInSecond(10);

        FlowMonitorThread flowMonitorThread = new FlowMonitorThread(messageStoreConfig);
        flowMonitorThread.start();

        flowMonitorThread.addByteCountTransferred(3);
        Boolean flag = await().atMost(Duration.ofSeconds(2)).until(() -> 7 == flowMonitorThread.canTransferMaxByteNum(), item -> item);
        flag &= await().atMost(Duration.ofSeconds(2)).until(() -> 10 == flowMonitorThread.canTransferMaxByteNum(), item -> item);
        Assert.assertTrue(flag);

        flowMonitorThread.shutdown();
    }

    @Test
    public void testSpeed() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setHaFlowControlEnable(true);
        messageStoreConfig.setMaxHaTransferByteInSecond(10);

        FlowMonitorThread flowMonitorThread = new FlowMonitorThread(messageStoreConfig);

        flowMonitorThread.addByteCountTransferred(3);
        flowMonitorThread.calculateSpeed();
        Assert.assertEquals(3, flowMonitorThread.getTransferredByteInSecond());

        flowMonitorThread.addByteCountTransferred(5);
        flowMonitorThread.calculateSpeed();
        Assert.assertEquals(5, flowMonitorThread.getTransferredByteInSecond());
    }
}
