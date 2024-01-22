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
import org.apache.rocketmq.store.server.ha.core.HAConnectionState;

/**
 * @renamed from ReadSocketService to ReadSocketThread
 */
public class ReadSocketThread extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
    private final Selector selector;
    private final SocketChannel socketChannel;
    private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    private int processPosition = 0;
    private volatile long lastReadTimestamp = System.currentTimeMillis();

    private final DefaultHAConnection haConnection;

    public ReadSocketThread(final SocketChannel socketChannel, DefaultHAConnection haConnection) throws IOException {
        this.haConnection = haConnection;

        this.selector = NetworkUtils.openSelector();
        this.socketChannel = socketChannel;
        this.socketChannel.register(this.selector, SelectionKey.OP_READ);
        this.setDaemon(true);
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.selector.select(1000);
                boolean ok = this.processReadEvent();
                if (!ok) {
                    log.error("processReadEvent error");
                    break;
                }

                long interval = TimeUtils.now() - this.lastReadTimestamp;
                if (interval > haConnection.getHaService().getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                    log.warn("ha housekeeping, found this connection[" + haConnection.getClientAddress() + "] expired, " + interval);
                    break;
                }
            } catch (Exception e) {
                log.error(this.getServiceName() + " service has exception.", e);
                break;
            }
        }

        haConnection.changeCurrentState(HAConnectionState.SHUTDOWN);
        this.makeStop();
        haConnection.getWriteSocketService().makeStop();
        haConnection.getHaService().removeConnection(haConnection);
        haConnection.getHaService().getConnectionCount().decrementAndGet();

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

    @Override
    public String getServiceName() {
        if (haConnection.getHaService().getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
            return haConnection.getHaService().getDefaultMessageStore().getBrokerIdentity().getIdentifier() + ReadSocketThread.class.getSimpleName();
        }
        return ReadSocketThread.class.getSimpleName();
    }

    private boolean processReadEvent() {
        int readSizeZeroTimes = 0;

        if (!this.byteBufferRead.hasRemaining()) {
            this.byteBufferRead.flip();
            this.processPosition = 0;
        }

        while (this.byteBufferRead.hasRemaining()) {
            try {
                int readSize = this.socketChannel.read(this.byteBufferRead);
                if (readSize < 0) {
                    log.error("read socket[" + haConnection.getClientAddress() + "] < 0");
                    return false;
                }

                if (readSize == 0) {
                    if (++readSizeZeroTimes >= 3) {
                        break;
                    }
                    continue;
                }

                readSizeZeroTimes = 0;
                this.lastReadTimestamp = TimeUtils.now();
                if ((this.byteBufferRead.position() - this.processPosition) < DefaultHAClient.REPORT_HEADER_SIZE) {
                    continue;
                }

                // Adjust the read position to the nearest multiple of REPORT_HEADER size by computing
                // the remainder of the current position divided by the size of the REPORT_HEADER
                int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % DefaultHAClient.REPORT_HEADER_SIZE);
                long readOffset = this.byteBufferRead.getLong(pos - 8);
                // and after the reading is finished, set the current processing position to the adjusted position,
                // ensuring that the next read starts from the correct position.
                this.processPosition = pos;

                haConnection.setSlaveAckOffset(readOffset);
                if (haConnection.getSlaveRequestOffset() < 0) {
                    haConnection.setSlaveRequestOffset(readOffset);
                    log.info("slave[" + haConnection.getClientAddress() + "] request offset " + readOffset);
                }

                haConnection.getHaService().notifyTransferSome(haConnection.getSlaveAckOffset());
            } catch (IOException e) {
                log.error("processReadEvent exception", e);
                return false;
            }
        }

        return true;
    }
}

