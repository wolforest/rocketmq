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
package org.apache.rocketmq.store.server.ha.ms;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.store.server.ha.HAConnection;
import org.apache.rocketmq.store.server.ha.core.FlowMonitorThread;
import org.apache.rocketmq.store.server.ha.core.HAConnectionState;

public class DefaultHAConnection implements HAConnection {

    /**
     * Transfer Header buffer size. Schema: physic offset and body size. Format:
     *
     * <pre>
     * ┌───────────────────────────────────────────────┬───────────────────────┐
     * │                  physicOffset                 │         bodySize      │
     * │                    (8bytes)                   │         (4bytes)      │
     * ├───────────────────────────────────────────────┴───────────────────────┤
     * │                                                                       │
     * │                           Transfer Header                             │
     * </pre>
     * <p>
     */
    public static final int TRANSFER_HEADER_SIZE = 8 + 4;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final DefaultHAService haService;
    private final SocketChannel socketChannel;
    private final String clientAddress;

    private final WriteSocketThread writeSocketThread;
    private final ReadSocketThread readSocketThread;
    private volatile HAConnectionState currentState = HAConnectionState.TRANSFER;
    private volatile long slaveRequestOffset = -1;

    private volatile long slaveAckOffset = -1;
    private final FlowMonitorThread flowMonitorThread;

    public DefaultHAConnection(final DefaultHAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.configureSocketChannel();
        this.clientAddress = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.writeSocketThread = new WriteSocketThread(this.socketChannel, this);
        this.readSocketThread = new ReadSocketThread(this.socketChannel, this);
        this.haService.getConnectionCount().incrementAndGet();
        this.flowMonitorThread = new FlowMonitorThread(haService.getDefaultMessageStore().getMessageStoreConfig());
    }

    private void configureSocketChannel() throws IOException {
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        if (NettySystemConfig.socketSndbufSize > 0) {
            this.socketChannel.socket().setReceiveBufferSize(NettySystemConfig.socketSndbufSize);
        }
        if (NettySystemConfig.socketRcvbufSize > 0) {
            this.socketChannel.socket().setSendBufferSize(NettySystemConfig.socketRcvbufSize);
        }
    }

    public void start() {
        changeCurrentState(HAConnectionState.TRANSFER);
        this.flowMonitorThread.start();
        this.readSocketThread.start();
        this.writeSocketThread.start();
    }

    public void shutdown() {
        changeCurrentState(HAConnectionState.SHUTDOWN);
        this.writeSocketThread.shutdown(true);
        this.readSocketThread.shutdown(true);
        this.flowMonitorThread.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel == null) {
            return;
        }

        try {
            this.socketChannel.close();
        } catch (IOException e) {
            log.error("", e);
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void changeCurrentState(HAConnectionState currentState) {
        log.info("change state to {}", currentState);
        this.currentState = currentState;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return currentState;
    }

    @Override
    public String getClientAddress() {
        return this.clientAddress;
    }

    @Override
    public long getSlaveAckOffset() {
        return slaveAckOffset;
    }

    public long getTransferredByteInSecond() {
        return this.flowMonitorThread.getTransferredByteInSecond();
    }

    public long getTransferFromWhere() {
        return writeSocketThread.getNextTransferFromWhere();
    }

    public WriteSocketThread getWriteSocketService() {
        return writeSocketThread;
    }

    public ReadSocketThread getReadSocketService() {
        return readSocketThread;
    }

    public long getSlaveRequestOffset() {
        return slaveRequestOffset;
    }

    public FlowMonitorThread getFlowMonitor() {
        return flowMonitorThread;
    }

    public void setCurrentState(HAConnectionState currentState) {
        this.currentState = currentState;
    }

    public void setSlaveRequestOffset(long slaveRequestOffset) {
        this.slaveRequestOffset = slaveRequestOffset;
    }

    public void setSlaveAckOffset(long slaveAckOffset) {
        this.slaveAckOffset = slaveAckOffset;
    }

    public DefaultHAService getHaService() {
        return haService;
    }

}
