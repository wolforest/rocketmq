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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.lang.thread.ServiceThread;
import org.apache.rocketmq.common.utils.NetworkUtils;
import org.apache.rocketmq.common.utils.TimeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.infra.mappedfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.server.ha.core.HAConnectionState;

/**
 * @renamed from WriteSocketService to WriteSocketThread
 */
public class WriteSocketThread extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final Selector selector;
    private final SocketChannel socketChannel;

    private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(DefaultHAConnection.TRANSFER_HEADER_SIZE);
    private long nextTransferFromWhere = -1;
    private SelectMappedBufferResult selectMappedBufferResult;
    private boolean lastWriteOver = true;
    private long lastPrintTimestamp = System.currentTimeMillis();
    private long lastWriteTimestamp = System.currentTimeMillis();

    private final DefaultHAConnection haConnection;

    public WriteSocketThread(final SocketChannel socketChannel, DefaultHAConnection haConnection) throws IOException {
        this.haConnection = haConnection;
        this.selector = NetworkUtils.openSelector();
        this.socketChannel = socketChannel;
        this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
        this.setDaemon(true);
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.selector.select(1000);
                if (-1 == haConnection.getSlaveRequestOffset()) {
                    Thread.sleep(10);
                    continue;
                }

                initOffset();
                if (!transferUnfinishedMessage()) {
                    continue;
                }

                transfer();
            } catch (Exception e) {
                log.error(this.getServiceName() + " service has exception.", e);
                break;
            }
        }

        releaseResource();
    }

    private void transfer() throws Exception {
        SelectMappedBufferResult selectResult = haConnection.getHaService().getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
        if (selectResult == null) {
            haConnection.getHaService().getWaitNotifyObject().allWaitForRunning(100);
            return;
        }

        int size = selectResult.getSize();
        if (size > haConnection.getHaService().getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
            size = haConnection.getHaService().getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
        }

        int canTransferMaxBytes = haConnection.getFlowMonitor().canTransferMaxByteNum();
        if (size > canTransferMaxBytes) {
            if (System.currentTimeMillis() - lastPrintTimestamp > 1000) {
                log.warn("Trigger HA flow control, max transfer speed {}KB/s, current speed: {}KB/s",
                    String.format("%.2f", haConnection.getFlowMonitor().maxTransferByteInSecond() / 1024.0),
                    String.format("%.2f", haConnection.getFlowMonitor().getTransferredByteInSecond() / 1024.0));
                lastPrintTimestamp = System.currentTimeMillis();
            }
            size = canTransferMaxBytes;
        }

        long thisOffset = this.nextTransferFromWhere;
        this.nextTransferFromWhere += size;

        selectResult.getByteBuffer().limit(size);
        this.selectMappedBufferResult = selectResult;

        // Build Header
        this.byteBufferHeader.position(0);
        this.byteBufferHeader.limit(DefaultHAConnection.TRANSFER_HEADER_SIZE);
        this.byteBufferHeader.putLong(thisOffset);
        this.byteBufferHeader.putInt(size);
        this.byteBufferHeader.flip();

        this.lastWriteOver = this.transferData();
    }

    private boolean transferUnfinishedMessage() throws Exception {
        if (!this.lastWriteOver) {
            this.lastWriteOver = this.transferData();
            return this.lastWriteOver;
        }

        long interval = TimeUtils.now() - this.lastWriteTimestamp;
        if (interval <= haConnection.getHaService().getDefaultMessageStore().getMessageStoreConfig().getHaSendHeartbeatInterval()) {
            return true;
        }

        // Build Header
        this.byteBufferHeader.position(0);
        this.byteBufferHeader.limit(DefaultHAConnection.TRANSFER_HEADER_SIZE);
        this.byteBufferHeader.putLong(this.nextTransferFromWhere);
        this.byteBufferHeader.putInt(0);
        this.byteBufferHeader.flip();

        this.lastWriteOver = this.transferData();
        return this.lastWriteOver;
    }

    private void initOffset() {
        if (-1 != this.nextTransferFromWhere) {
            return;
        }

        // If it's a new slave and commit log files haven't been manually copied from the master server
        // syncing from the initial offset of the last commitLog file
        if (0 == haConnection.getSlaveRequestOffset()) {
            long masterOffset = haConnection.getHaService().getDefaultMessageStore().getCommitLog().getMaxOffset();
            long commitLogFileSize = haConnection.getHaService().getDefaultMessageStore().getMessageStoreConfig().getMappedFileSizeCommitLog();

            // Adjust the masterOffset to be a multiple of the size of the message log file,
            // ensuring that it points to the beginning of a complete and newest commit log file.
            masterOffset = masterOffset - (masterOffset % commitLogFileSize);
            if (masterOffset < 0) {
                masterOffset = 0;
            }

            this.nextTransferFromWhere = masterOffset;
        } else {
            this.nextTransferFromWhere = haConnection.getSlaveRequestOffset();
        }

        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + haConnection.getClientAddress()
            + "], and slave request " + haConnection.getSlaveRequestOffset());
    }

    private void releaseResource() {
        haConnection.getHaService().getWaitNotifyObject().removeFromWaitingThreadTable();
        if (this.selectMappedBufferResult != null) {
            this.selectMappedBufferResult.release();
        }

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
            log.error("", e);
        }

        haConnection.getFlowMonitor().shutdown(true);
        log.info(this.getServiceName() + " service end");
    }

    private boolean transferData() throws Exception {
        writeHeader();

        if (null == this.selectMappedBufferResult) {
            return !this.byteBufferHeader.hasRemaining();
        }

        writeBody();

        boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();
        if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
            this.selectMappedBufferResult.release();
            this.selectMappedBufferResult = null;
        }

        return result;
    }

    private void writeHeader() throws Exception {
        int writeSizeZeroTimes = 0;
        // Write Header
        while (this.byteBufferHeader.hasRemaining()) {
            int writeSize = this.socketChannel.write(this.byteBufferHeader);
            if (writeSize > 0) {
                haConnection.getFlowMonitor().addByteCountTransferred(writeSize);
                writeSizeZeroTimes = 0;
                this.lastWriteTimestamp = TimeUtils.now();
            } else if (writeSize == 0) {
                if (++writeSizeZeroTimes >= 3) {
                    break;
                }
            } else {
                throw new Exception("ha master write header error < 0");
            }
        }
    }

    private void writeBody() throws Exception {
        // Write Body
        int writeSizeZeroTimes = 0;
        if (this.byteBufferHeader.hasRemaining()) {
            return;
        }

        while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
            int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
            if (writeSize > 0) {
                writeSizeZeroTimes = 0;
                this.lastWriteTimestamp = TimeUtils.now();
            } else if (writeSize == 0) {
                if (++writeSizeZeroTimes >= 3) {
                    break;
                }
            } else {
                throw new Exception("ha master write body error < 0");
            }
        }
    }

    @Override
    public String getServiceName() {
        if (haConnection.getHaService().getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
            return haConnection.getHaService().getDefaultMessageStore().getBrokerIdentity().getIdentifier() + WriteSocketThread.class.getSimpleName();
        }
        return WriteSocketThread.class.getSimpleName();
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

    public long getNextTransferFromWhere() {
        return nextTransferFromWhere;
    }
}

