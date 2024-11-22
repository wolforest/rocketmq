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
package org.apache.rocketmq.broker.domain.transaction.queue;

import org.apache.rocketmq.broker.domain.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.domain.transaction.OperationResult;
import org.apache.rocketmq.broker.domain.transaction.TransactionalMessageService;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.message.Message;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.domain.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.store.api.dto.AppendMessageResult;
import org.apache.rocketmq.store.api.dto.AppendMessageStatus;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TransactionalMessageServiceImplTest {

    private TransactionalMessageService queueTransactionMsgService;

    @Mock
    private TransactionalMessageBridge bridge;

    @Spy
    private Broker broker = new Broker(new BrokerConfig(), new NettyServerConfig(),
        new NettyClientConfig(), new MessageStoreConfig());

    @Mock
    private AbstractTransactionalMessageCheckListener listener;

    @Before
    public void init() {
        when(bridge.getBrokerController()).thenReturn(broker);
        listener.setBrokerController(broker);
        queueTransactionMsgService = new TransactionalMessageServiceImpl(bridge);
    }

    @Test
    public void testPrepareMessage() {
        MessageExtBrokerInner inner = createMessageBrokerInner();
        when(bridge.putHalfMessage(any(MessageExtBrokerInner.class)))
                .thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        PutMessageResult result = queueTransactionMsgService.prepareMessage(inner);
        assert result.isOk();
    }

    @Test
    public void testCommitMessage() {
        when(bridge.lookMessageByOffset(anyLong())).thenReturn(createMessageBrokerInner());
        OperationResult result = queueTransactionMsgService.commitMessage(createEndTransactionRequestHeader(MessageSysFlag.TRANSACTION_COMMIT_TYPE));
        assertThat(result.getResponseCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testRollbackMessage() {
        when(bridge.lookMessageByOffset(anyLong())).thenReturn(createMessageBrokerInner());
        OperationResult result = queueTransactionMsgService.commitMessage(createEndTransactionRequestHeader(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE));
        assertThat(result.getResponseCode()).isEqualTo(ResponseCode.SUCCESS);
    }


    @Test
    public void testDeletePrepareMessage_queueFull() throws InterruptedException {
        ((TransactionalMessageServiceImpl)queueTransactionMsgService).getDeleteContext().put(0, new MessageQueueOpContext(0, 1));
        boolean res = queueTransactionMsgService.deletePrepareMessage(createMessageBrokerInner());
        assertThat(res).isTrue();
        when(bridge.writeOp(any(Integer.class), any(Message.class))).thenReturn(false);
        res = queueTransactionMsgService.deletePrepareMessage(createMessageBrokerInner());
        assertThat(res).isFalse();
    }

    @Test
    public void testDeletePrepareMessage_maxSize() throws InterruptedException {
        broker.getBrokerConfig().setTransactionOpMsgMaxSize(1);
        broker.getBrokerConfig().setTransactionOpBatchInterval(3000);
        queueTransactionMsgService.open();
        boolean res = queueTransactionMsgService.deletePrepareMessage(createMessageBrokerInner(1000, "test", "testHello"));
        assertThat(res).isTrue();
        verify(bridge, timeout(50)).writeOp(any(Integer.class), any(Message.class));
        queueTransactionMsgService.close();
    }

    @Test
    public void testOpen() {
        boolean isOpen = queueTransactionMsgService.open();
        assertThat(isOpen).isTrue();
    }

    private EndTransactionRequestHeader createEndTransactionRequestHeader(int status) {
        EndTransactionRequestHeader header = new EndTransactionRequestHeader();
        header.setTopic("topic");
        header.setCommitLogOffset(123456789L);
        header.setCommitOrRollback(status);
        header.setMsgId("12345678");
        header.setTransactionId("123");
        header.setProducerGroup("testTransactionGroup");
        header.setTranStateTableOffset(1234L);
        return header;
    }

    private MessageExtBrokerInner createMessageBrokerInner(long queueOffset, String topic, String body) {
        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        inner.setBornTimestamp(System.currentTimeMillis() - 80000);
        inner.setTransactionId("123456123");
        inner.setTopic(topic);
        inner.setQueueOffset(queueOffset);
        inner.setBody(body.getBytes());
        inner.setMsgId("123456123");
        inner.setQueueId(0);
        inner.setTopic("hello");
        return inner;
    }

    private MessageExtBrokerInner createMessageBrokerInner() {
        return createMessageBrokerInner(1, "testTopic", "hello world");
    }
}
