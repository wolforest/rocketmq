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

package org.apache.rocketmq.store.server.ha.autoswitch;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.store.server.ha.HAConnection;
import org.apache.rocketmq.store.server.ha.core.FlowMonitorThread;
import org.apache.rocketmq.store.server.ha.core.HAConnectionState;

public class AutoSwitchHAConnection implements HAConnection {

    /**
     * Handshake data protocol in syncing msg from master. Format:
     * <pre>
     * ┌─────────────────┬───────────────┬───────────┬───────────┬────────────────────────────────────┐
     * │  current state  │   body size   │   offset  │   epoch   │   EpochEntrySize * EpochEntryNums  │
     * │     (4bytes)    │   (4bytes)    │  (8bytes) │  (4bytes) │      (12bytes * EpochEntryNums)    │
     * ├─────────────────┴───────────────┴───────────┴───────────┼────────────────────────────────────┤
     * │                       Header                            │             Body                   │
     * │                                                         │                                    │
     * </pre>
     * Handshake Header protocol Format:
     * current state + body size + offset + epoch
     */
    public static final int HANDSHAKE_HEADER_SIZE = 4 + 4 + 8 + 4;

    /**
     * Transfer data protocol in syncing msg from master. Format:
     * <pre>
     * ┌─────────────────┬───────────────┬───────────┬───────────┬─────────────────────┬──────────────────┬──────────────────┐
     * │  current state  │   body size   │   offset  │   epoch   │   epochStartOffset  │   confirmOffset  │    log data      │
     * │     (4bytes)    │   (4bytes)    │  (8bytes) │  (4bytes) │      (8bytes)       │      (8bytes)    │   (data size)    │
     * ├─────────────────┴───────────────┴───────────┴───────────┴─────────────────────┴──────────────────┼──────────────────┤
     * │                                               Header                                             │       Body       │
     * │                                                                                                  │                  │
     * </pre>
     * Transfer Header protocol Format:
     * current state + body size + offset + epoch  + epochStartOffset + additionalInfo(confirmOffset)
     */
    public static final int TRANSFER_HEADER_SIZE = HANDSHAKE_HEADER_SIZE + 8 + 8;
    public static final int EPOCH_ENTRY_SIZE = 12;
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AutoSwitchHAService haService;
    private final SocketChannel socketChannel;
    private final String clientAddress;
    private final EpochFileCache epochCache;
    private final AbstractWriteSocketThread writeSocketService;
    private final ReadSocketThread readSocketThread;
    private final FlowMonitorThread flowMonitorThread;

    private volatile HAConnectionState currentState = HAConnectionState.HANDSHAKE;
    private volatile long slaveRequestOffset = -1;
    private volatile long slaveAckOffset = -1;

    /**
     * Whether the slave have already sent a handshake message
     */
    private volatile boolean isSlaveSendHandshake = false;
    private volatile int currentTransferEpoch = -1;
    private volatile long currentTransferEpochEndOffset = 0;
    private volatile boolean isSyncFromLastFile = false;
    private volatile boolean isAsyncLearner = false;
    private volatile long slaveId = -1;

    /**
     * Last endOffset when master transfer data to slave
     */
    private volatile long lastMasterMaxOffset = -1;
    /**
     * Last time ms when transfer data to slave.
     */
    private volatile long lastTransferTimeMs = 0;

    public AutoSwitchHAConnection(AutoSwitchHAService haService, SocketChannel socketChannel, EpochFileCache epochCache) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.configureSocketChannel();
        this.clientAddress = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.epochCache = epochCache;
        this.writeSocketService = new WriteSocketThread(this.socketChannel, this);
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

    @Override
    public void start() {
        changeCurrentState(HAConnectionState.HANDSHAKE);
        this.flowMonitorThread.start();
        this.readSocketThread.start();
        this.writeSocketService.start();
    }

    @Override
    public void shutdown() {
        changeCurrentState(HAConnectionState.SHUTDOWN);
        this.flowMonitorThread.shutdown(true);
        this.writeSocketService.shutdown(true);
        this.readSocketThread.shutdown(true);
        this.close();
    }

    @Override
    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (final IOException e) {
                LOGGER.error("", e);
            }
        }
    }

    public void changeCurrentState(HAConnectionState connectionState) {
        LOGGER.info("change state to {}", connectionState);
        this.currentState = connectionState;
    }

    public long getSlaveId() {
        return slaveId;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return currentState;
    }

    @Override
    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    @Override
    public String getClientAddress() {
        return clientAddress;
    }

    @Override
    public long getSlaveAckOffset() {
        return slaveAckOffset;
    }

    @Override
    public long getTransferredByteInSecond() {
        return flowMonitorThread.getTransferredByteInSecond();
    }

    @Override
    public long getTransferFromWhere() {
        return this.writeSocketService.getNextTransferFromWhere();
    }

    public void changeTransferEpochToNext(final EpochEntry entry) {
        this.currentTransferEpoch = entry.getEpoch();
        this.currentTransferEpochEndOffset = entry.getEndOffset();
        if (entry.getEpoch() == this.epochCache.lastEpoch()) {
            // Use -1 to stand for Long.max
            this.currentTransferEpochEndOffset = -1;
        }
    }

    public boolean isAsyncLearner() {
        return isAsyncLearner;
    }

    public boolean isSyncFromLastFile() {
        return isSyncFromLastFile;
    }

    public synchronized void updateLastTransferInfo() {
        this.lastMasterMaxOffset = this.haService.getDefaultMessageStore().getMaxPhyOffset();
        this.lastTransferTimeMs = System.currentTimeMillis();
    }

    public synchronized void maybeExpandInSyncStateSet(long slaveMaxOffset) {
        if (!this.isAsyncLearner && slaveMaxOffset >= this.lastMasterMaxOffset) {
            long caughtUpTimeMs = this.haService.getDefaultMessageStore().getMaxPhyOffset() == slaveMaxOffset ? System.currentTimeMillis() : this.lastTransferTimeMs;
            this.haService.updateConnectionLastCaughtUpTime(this.slaveId, caughtUpTimeMs);
            this.haService.maybeExpandInSyncStateSet(this.slaveId, slaveMaxOffset);
        }
    }

    public AutoSwitchHAService getHaService() {
        return haService;
    }

    public EpochFileCache getEpochCache() {
        return epochCache;
    }

    public AbstractWriteSocketThread getWriteSocketService() {
        return writeSocketService;
    }

    public ReadSocketThread getReadSocketService() {
        return readSocketThread;
    }

    public FlowMonitorThread getFlowMonitorThread() {
        return flowMonitorThread;
    }

    public long getSlaveRequestOffset() {
        return slaveRequestOffset;
    }

    public int getCurrentTransferEpoch() {
        return currentTransferEpoch;
    }

    public long getCurrentTransferEpochEndOffset() {
        return currentTransferEpochEndOffset;
    }

    public boolean isSlaveSendHandshake() {
        return isSlaveSendHandshake;
    }

    public void setSlaveSendHandshake(boolean slaveSendHandshake) {
        isSlaveSendHandshake = slaveSendHandshake;
    }

    public void setSyncFromLastFile(boolean syncFromLastFile) {
        isSyncFromLastFile = syncFromLastFile;
    }

    public void setAsyncLearner(boolean asyncLearner) {
        isAsyncLearner = asyncLearner;
    }

    public void setSlaveId(long slaveId) {
        this.slaveId = slaveId;
    }

    public void setSlaveRequestOffset(long slaveRequestOffset) {
        this.slaveRequestOffset = slaveRequestOffset;
    }

    public void setSlaveAckOffset(long slaveAckOffset) {
        this.slaveAckOffset = slaveAckOffset;
    }

}
