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
package org.apache.rocketmq.broker.util;

import java.util.Objects;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.utils.StringUtils;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;
import org.apache.rocketmq.store.server.RunningFlags;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class HookUtilsTest {

    @Test
    public void testCheckBeforePutMessage() {
        BrokerController brokerController = Mockito.mock(BrokerController.class);
        MessageStore messageStore = Mockito.mock(MessageStore.class);
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        RunningFlags runningFlags = Mockito.mock(RunningFlags.class);

        Mockito.when(brokerController.getMessageStore()).thenReturn(messageStore);
        Mockito.when(brokerController.getMessageStore().isShutdown()).thenReturn(false);
        Mockito.when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        Mockito.when(messageStore.getRunningFlags()).thenReturn(runningFlags);
        Mockito.when(messageStore.getRunningFlags().isWriteable()).thenReturn(true);

        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(StringUtils.randomAlphabetic(Byte.MAX_VALUE).toUpperCase());
        messageExt.setBody(StringUtils.randomAlphabetic(Byte.MAX_VALUE).toUpperCase().getBytes());
        Assert.assertNull(HookUtils.checkBeforePutMessage(brokerController, messageExt));

        messageExt.setTopic(StringUtils.randomAlphabetic(Byte.MAX_VALUE + 1).toUpperCase());
        Assert.assertEquals(PutMessageStatus.MESSAGE_ILLEGAL, Objects.requireNonNull(
            HookUtils.checkBeforePutMessage(brokerController, messageExt)).getPutMessageStatus());

        messageExt.setTopic(MQConstants.RETRY_GROUP_TOPIC_PREFIX +
            StringUtils.randomAlphabetic(Byte.MAX_VALUE + 1).toUpperCase());
        Assert.assertNull(HookUtils.checkBeforePutMessage(brokerController, messageExt));

        messageExt.setTopic(MQConstants.RETRY_GROUP_TOPIC_PREFIX +
            StringUtils.randomAlphabetic(255 - MQConstants.RETRY_GROUP_TOPIC_PREFIX.length()).toUpperCase());
        Assert.assertNull(HookUtils.checkBeforePutMessage(brokerController, messageExt));

        messageExt.setTopic(MQConstants.RETRY_GROUP_TOPIC_PREFIX +
            StringUtils.randomAlphabetic(256 - MQConstants.RETRY_GROUP_TOPIC_PREFIX.length()).toUpperCase());
        Assert.assertEquals(PutMessageStatus.MESSAGE_ILLEGAL, Objects.requireNonNull(
            HookUtils.checkBeforePutMessage(brokerController, messageExt)).getPutMessageStatus());
    }
}