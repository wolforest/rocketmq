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
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.lang.thread.ServiceThread;
import org.apache.rocketmq.common.utils.NetworkUtils;
import org.apache.rocketmq.common.utils.TimeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.server.ha.core.HAConnectionState;
import org.apache.rocketmq.store.server.ha.io.AbstractHAReader;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;

public class ReadSocketService extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;

    protected final AutoSwitchHAConnection haConnection;
    private final Selector selector;
    private final SocketChannel socketChannel;
    private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    private final AbstractHAReader haReader;

    private int processPosition = 0;
    private volatile long lastReadTimestamp = System.currentTimeMillis();

    public ReadSocketService(final SocketChannel socketChannel, AutoSwitchHAConnection haConnection) throws IOException {
        this.haConnection = haConnection;

        this.selector = NetworkUtils.openSelector();
        this.socketChannel = socketChannel;
        this.socketChannel.register(this.selector, SelectionKey.OP_READ);

        this.setDaemon(true);

        haReader = new HAServerReader(haConnection, this);
        haReader.registerHook(readSize -> {
            if (readSize > 0) {
                ReadSocketService.this.setLastReadTimestamp(TimeUtils.now());
            }
        });
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.selector.select(1000);
                boolean ok = this.haReader.read(this.socketChannel, this.byteBufferRead);
                if (!ok) {
                    LOGGER.error("processReadEvent error");
                    break;
                }

                long interval = TimeUtils.now() - this.lastReadTimestamp;
                if (interval > haConnection.getHaService().getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                    LOGGER.warn("ha housekeeping, found this connection[" + haConnection.getClientAddress() + "] expired, " + interval);
                    break;
                }
            } catch (Exception e) {
                LOGGER.error(this.getServiceName() + " service has exception.", e);
                break;
            }
        }

        this.makeStop();
        haConnection.changeCurrentState(HAConnectionState.SHUTDOWN);
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
            LOGGER.error("", e);
        }

        haConnection.getFlowMonitorThread().shutdown(true);

        LOGGER.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        DefaultMessageStore store = haConnection.getHaService().getDefaultMessageStore();
        if (store.getBrokerConfig().isInBrokerContainer()) {
            return store.getBrokerIdentity().getIdentifier() + ReadSocketService.class.getSimpleName();
        }
        return ReadSocketService.class.getSimpleName();
    }

    public int getProcessPosition() {
        return processPosition;
    }

    public void setProcessPosition(int processPosition) {
        this.processPosition = processPosition;
    }

    public void setLastReadTimestamp(long lastReadTimestamp) {
        this.lastReadTimestamp = lastReadTimestamp;
    }

}
