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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.List;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.lang.thread.ServiceThread;
import org.apache.rocketmq.common.utils.NetworkUtils;
import org.apache.rocketmq.common.utils.TimeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.server.ha.core.HAConnectionState;
import org.apache.rocketmq.store.server.ha.io.HAWriter;

/**
 * @renamed from AbstractWriteSocketService to AbstractWriteSocketThread
 */
public abstract class AbstractWriteSocketThread extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    protected final AutoSwitchHAConnection haConnection;
    protected final Selector selector;
    protected final SocketChannel socketChannel;
    protected final HAWriter haWriter;

    protected final ByteBuffer byteBufferHeader = ByteBuffer.allocate(AutoSwitchHAConnection.TRANSFER_HEADER_SIZE);
    // Store master epochFileCache: (Epoch + startOffset) * 1000
    private final ByteBuffer handShakeBuffer = ByteBuffer.allocate(AutoSwitchHAConnection.EPOCH_ENTRY_SIZE * 1000);

    protected long nextTransferFromWhere = -1;
    protected boolean lastWriteOver = true;
    protected long lastWriteTimestamp = System.currentTimeMillis();
    protected long lastPrintTimestamp = System.currentTimeMillis();
    protected long transferOffset = 0;

    public AbstractWriteSocketThread(final SocketChannel socketChannel, AutoSwitchHAConnection haConnection) throws IOException {
        this.haConnection = haConnection;
        this.selector = NetworkUtils.openSelector();
        this.socketChannel = socketChannel;
        this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
        this.setDaemon(true);

        haWriter = new HAWriter();
        haWriter.registerHook(writeSize -> {
            haConnection.getFlowMonitorThread().addByteCountTransferred(writeSize);
            if (writeSize > 0) {
                AbstractWriteSocketThread.this.lastWriteTimestamp = TimeUtils.now();
            }
        });
    }

    public long getNextTransferFromWhere() {
        return this.nextTransferFromWhere;
    }

    private boolean buildHandshakeBuffer() {
        final List<EpochEntry> epochEntries = haConnection.getEpochCache().getAllEntries();
        final int lastEpoch = haConnection.getEpochCache().lastEpoch();
        final long maxPhyOffset = haConnection.getHaService().getDefaultMessageStore().getMaxPhyOffset();
        this.byteBufferHeader.position(0);
        this.byteBufferHeader.limit(AutoSwitchHAConnection.HANDSHAKE_HEADER_SIZE);
        // State
        this.byteBufferHeader.putInt(haConnection.getCurrentState().ordinal());
        // Body size
        this.byteBufferHeader.putInt(epochEntries.size() * AutoSwitchHAConnection.EPOCH_ENTRY_SIZE);
        // Offset
        this.byteBufferHeader.putLong(maxPhyOffset);
        // Epoch
        this.byteBufferHeader.putInt(lastEpoch);
        this.byteBufferHeader.flip();

        // EpochEntries
        this.handShakeBuffer.position(0);
        this.handShakeBuffer.limit(AutoSwitchHAConnection.EPOCH_ENTRY_SIZE * epochEntries.size());

        for (final EpochEntry entry : epochEntries) {
            if (entry == null) {
                continue;
            }

            this.handShakeBuffer.putInt(entry.getEpoch());
            this.handShakeBuffer.putLong(entry.getStartOffset());
        }

        this.handShakeBuffer.flip();
        LOGGER.info("Master build handshake header: maxEpoch:{}, maxOffset:{}, epochEntries:{}", lastEpoch, maxPhyOffset, epochEntries);
        return true;
    }

    private boolean handshakeWithSlave() throws IOException {
        // Write Header
        boolean result = this.haWriter.write(this.socketChannel, this.byteBufferHeader);
        if (!result) {
            return false;
        }

        // Write Body
        return this.haWriter.write(this.socketChannel, this.handShakeBuffer);
    }

    // Normal transfer method
    private void buildTransferHeaderBuffer(long nextOffset, int bodySize) {
        EpochEntry entry = haConnection.getEpochCache().getEntry(haConnection.getCurrentTransferEpoch());
        if (entry == null) {
            // If broker is started on empty disk and no message entered (nextOffset = -1 and currentTransferEpoch = -1), do not output error log when sending heartbeat
            if (nextOffset != -1 || haConnection.getCurrentTransferEpoch() != -1 || bodySize > 0) {
                LOGGER.error("Failed to find epochEntry with epoch {} when build msg header", haConnection.getCurrentTransferEpoch());
            }

            if (bodySize > 0) {
                return;
            }
            // Maybe it's used for heartbeat
            entry = haConnection.getEpochCache().firstEntry();
        }

        // Build Header
        this.byteBufferHeader.position(0);
        this.byteBufferHeader.limit(AutoSwitchHAConnection.TRANSFER_HEADER_SIZE);
        // State
        this.byteBufferHeader.putInt(haConnection.getCurrentState().ordinal());
        // Body size
        this.byteBufferHeader.putInt(bodySize);
        // Offset
        this.byteBufferHeader.putLong(nextOffset);
        // Epoch
        this.byteBufferHeader.putInt(entry.getEpoch());
        // EpochStartOffset
        this.byteBufferHeader.putLong(entry.getStartOffset());
        // Additional info(confirm offset)
        final long confirmOffset = haConnection.getHaService().getDefaultMessageStore().getConfirmOffset();
        this.byteBufferHeader.putLong(confirmOffset);
        this.byteBufferHeader.flip();
    }

    private boolean sendHeartbeatIfNeeded() throws Exception {
        long interval = TimeUtils.now() - this.lastWriteTimestamp;
        if (interval > haConnection.getHaService().getDefaultMessageStore().getMessageStoreConfig().getHaSendHeartbeatInterval()) {
            buildTransferHeaderBuffer(this.nextTransferFromWhere, 0);
            return this.transferData(0);
        }
        return true;
    }

    private void transferToSlave() throws Exception {
        if (this.lastWriteOver) {
            this.lastWriteOver = sendHeartbeatIfNeeded();
        } else {
            // maxTransferSize == -1 means to continue transfer remaining data.
            this.lastWriteOver = this.transferData(-1);
        }
        if (!this.lastWriteOver) {
            return;
        }

        int size = this.getNextTransferDataSize();
        if (size <= 0) {
            // If size == 0, we should update the lastCatchupTimeMs
            haConnection.getHaService().updateConnectionLastCaughtUpTime(haConnection.getSlaveId(), System.currentTimeMillis());
            haConnection.getHaService().getWaitNotifyObject().allWaitForRunning(100);
            return;
        }

        MessageStoreConfig storeConfig = haConnection.getHaService().getDefaultMessageStore().getMessageStoreConfig();
        if (size > storeConfig.getHaTransferBatchSize()) {
            size = storeConfig.getHaTransferBatchSize();
        }

        int canTransferMaxBytes = haConnection.getFlowMonitorThread().canTransferMaxByteNum();
        if (size > canTransferMaxBytes) {
            if (System.currentTimeMillis() - lastPrintTimestamp > 1000) {
                LOGGER.warn("Trigger HA flow control, max transfer speed {}KB/s, current speed: {}KB/s",
                    String.format("%.2f", haConnection.getFlowMonitorThread().maxTransferByteInSecond() / 1024.0),
                    String.format("%.2f", haConnection.getFlowMonitorThread().getTransferredByteInSecond() / 1024.0));
                lastPrintTimestamp = System.currentTimeMillis();
            }
            size = canTransferMaxBytes;
        }

        if (size <= 0) {
            this.releaseData();
            this.waitForRunning(100);
            return;
        }

        // We must ensure that the transmitted logs are within the same epoch
        // If currentEpochEndOffset == -1, means that currentTransferEpoch = last epoch, so the endOffset = Long.max
        final long currentEpochEndOffset = haConnection.getCurrentTransferEpochEndOffset();
        if (currentEpochEndOffset != -1 && this.nextTransferFromWhere + size > currentEpochEndOffset) {
            final EpochEntry epochEntry = haConnection.getEpochCache().nextEntry(haConnection.getCurrentTransferEpoch());
            if (epochEntry == null) {
                LOGGER.error("Can't find a bigger epochEntry than epoch {}", haConnection.getCurrentTransferEpoch());
                waitForRunning(100);
                return;
            }
            size = (int) (currentEpochEndOffset - this.nextTransferFromWhere);
            haConnection.changeTransferEpochToNext(epochEntry);
        }

        this.transferOffset = this.nextTransferFromWhere;
        this.nextTransferFromWhere += size;
        haConnection.updateLastTransferInfo();
        // Build Header
        buildTransferHeaderBuffer(this.transferOffset, size);
        this.lastWriteOver = this.transferData(size);
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.selector.select(1000);

                switch (haConnection.getCurrentState()) {
                    case HANDSHAKE:
                        // Wait until the slave send it handshake msg to master.
                        if (!haConnection.isSlaveSendHandshake()) {
                            this.waitForRunning(10);
                            continue;
                        }

                        if (this.lastWriteOver) {
                            if (!buildHandshakeBuffer()) {
                                LOGGER.error("AutoSwitchHAConnection build handshake buffer failed");
                                this.waitForRunning(5000);
                                continue;
                            }
                        }

                        this.lastWriteOver = handshakeWithSlave();
                        if (this.lastWriteOver) {
                            // change flag to {false} to wait for slave notification
                            haConnection.setSlaveSendHandshake(false);
                        }
                        break;
                    case TRANSFER:
                        if (-1 == haConnection.getSlaveRequestOffset()) {
                            this.waitForRunning(10);
                            continue;
                        }

                        if (-1 == this.nextTransferFromWhere) {
                            if (0 == haConnection.getSlaveRequestOffset()) {
                                // We must ensure that the starting point of syncing log
                                // must be the startOffset of a file (maybe the last file, or the minOffset)
                                final MessageStoreConfig config = haConnection.getHaService().getDefaultMessageStore().getMessageStoreConfig();
                                if (haConnection.isSyncFromLastFile()) {
                                    long masterOffset = haConnection.getHaService().getDefaultMessageStore().getCommitLog().getMaxOffset();
                                    masterOffset = masterOffset - (masterOffset % config.getMappedFileSizeCommitLog());
                                    if (masterOffset < 0) {
                                        masterOffset = 0;
                                    }
                                    this.nextTransferFromWhere = masterOffset;
                                } else {
                                    this.nextTransferFromWhere = haConnection.getHaService().getDefaultMessageStore().getCommitLog().getMinOffset();
                                }
                            } else {
                                this.nextTransferFromWhere = haConnection.getSlaveRequestOffset();
                            }

                            // nextTransferFromWhere is not found. It may be empty disk and no message is entered
                            if (this.nextTransferFromWhere == -1) {
                                sendHeartbeatIfNeeded();
                                waitForRunning(500);
                                break;
                            }
                            // Setup initial transferEpoch
                            EpochEntry epochEntry = haConnection.getEpochCache().findEpochEntryByOffset(this.nextTransferFromWhere);
                            if (epochEntry == null) {
                                LOGGER.error("Failed to find an epochEntry to match nextTransferFromWhere {}", this.nextTransferFromWhere);
                                sendHeartbeatIfNeeded();
                                waitForRunning(500);
                                break;
                            }
                            haConnection.changeTransferEpochToNext(epochEntry);
                            LOGGER.info("Master transfer data to slave {}, from offset:{}, currentEpoch:{}", haConnection.getClientAddress(), this.nextTransferFromWhere, epochEntry);
                        }
                        transferToSlave();
                        break;
                    default:
                        throw new Exception("unexpected state " + haConnection.getCurrentState());
                }
            } catch (Exception e) {
                LOGGER.error(this.getServiceName() + " service has exception.", e);
                break;
            }
        }

        this.onStop();
        haConnection.changeCurrentState(HAConnectionState.SHUTDOWN);
        this.makeStop();
        haConnection.getReadSocketService().makeStop();
        haConnection.getHaService().removeConnection(haConnection);

        SelectionKey sk = this.socketChannel.keyFor(this.selector);
        if (sk != null) {
            sk.cancel();
        }

        try {
            this.selector.close();
            this.socketChannel.close();
        } catch (IOException e) {
            LOGGER.error("", e);
        }

        haConnection.getFlowMonitorThread().shutdown(true);

        LOGGER.info(this.getServiceName() + " service end");
    }

    abstract protected int getNextTransferDataSize();

    abstract protected void releaseData();

    abstract protected boolean transferData(int maxTransferSize) throws Exception;

    abstract protected void onStop();
}

