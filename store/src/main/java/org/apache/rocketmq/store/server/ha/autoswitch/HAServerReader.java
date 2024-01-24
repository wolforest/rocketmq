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

import java.nio.ByteBuffer;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.server.ha.core.HAConnectionState;
import org.apache.rocketmq.store.server.ha.io.AbstractHAReader;

public class HAServerReader extends AbstractHAReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AutoSwitchHAConnection haConnection;
    private final ReadSocketService readSocketService;

    public HAServerReader(AutoSwitchHAConnection haConnection, ReadSocketService readSocketService) {
        this.haConnection = haConnection;
        this.readSocketService = readSocketService;
    }

    @Override
    protected boolean processReadResult(ByteBuffer byteBufferRead) {
        while (true) {
            int diff = byteBufferRead.position() - readSocketService.getProcessPosition();

            if (diff >= AutoSwitchHAClient.MIN_HEADER_SIZE) {
                int readPosition = readSocketService.getProcessPosition();
                HAConnectionState slaveState = HAConnectionState.values()[byteBufferRead.getInt(readPosition)];

                if (!processSlaveState(slaveState, byteBufferRead, byteBufferRead.position(), readPosition)) {
                    return false;
                }

                changeCurrentState(slaveState);
                continue;
            }

            resetProcessPosition(byteBufferRead);
            break;
        }

        return true;
    }

    private boolean processSlaveState(HAConnectionState slaveState, ByteBuffer byteBufferRead, int readSocketPos, int readPosition) {
        switch (slaveState) {
            case HANDSHAKE:
                processHandshakeState(byteBufferRead, readSocketPos, readPosition);
                break;
            case TRANSFER:
                processTransferState(byteBufferRead, readSocketPos, readPosition);
                break;
            default:
                LOGGER.error("Current state illegal {}", haConnection.getCurrentState());
                return false;
        }

        return true;
    }

    private void processHandshakeState(ByteBuffer byteBufferRead, int readSocketPos, int readPosition) {
        // SlaveBrokerId
        long slaveBrokerId = byteBufferRead.getLong(readPosition + AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE - 8);
        haConnection.setSlaveId(slaveBrokerId);

        // Flag(isSyncFromLastFile)
        short syncFromLastFileFlag = byteBufferRead.getShort(readPosition + AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE - 12);
        if (syncFromLastFileFlag == 1) {
            haConnection.setSyncFromLastFile(true);
        }
        // Flag(isAsyncLearner role)
        short isAsyncLearner = byteBufferRead.getShort(readPosition + AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE - 10);
        if (isAsyncLearner == 1) {
            haConnection.setAsyncLearner(true);
        }

        haConnection.setSlaveSendHandshake(true);
        byteBufferRead.position(readSocketPos);
        readSocketService.setProcessPosition(readSocketService.getProcessPosition() + AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE);
        LOGGER.info("Receive slave handshake, slaveBrokerId:{}, isSyncFromLastFile:{}, isAsyncLearner:{}",
            haConnection.getSlaveId(), haConnection.isSyncFromLastFile(), haConnection.isAsyncLearner());
    }

    private void processTransferState(ByteBuffer byteBufferRead, int readSocketPos, int readPosition) {
        long slaveMaxOffset = byteBufferRead.getLong(readPosition + 4);
        readSocketService.setProcessPosition(readSocketService.getProcessPosition() + AutoSwitchHAClient.TRANSFER_HEADER_SIZE);

        haConnection.setSlaveAckOffset(slaveMaxOffset);
        if (haConnection.getSlaveRequestOffset() < 0) {
            haConnection.setSlaveRequestOffset(slaveMaxOffset);
        }

        byteBufferRead.position(readSocketPos);
        haConnection.maybeExpandInSyncStateSet(slaveMaxOffset);
        haConnection.getHaService().updateConfirmOffsetWhenSlaveAck(haConnection.getSlaveId());
        haConnection.getHaService().notifyTransferSome(haConnection.getSlaveAckOffset());
    }

    private void changeCurrentState(HAConnectionState slaveState) {
        if (slaveState.equals(haConnection.getCurrentState())) {
            return;
        }

        LOGGER.warn("Master change state from {} to {}", haConnection.getCurrentState(), slaveState);
        haConnection.changeCurrentState(slaveState);
    }

    private void resetProcessPosition(ByteBuffer byteBufferRead) {
        if (byteBufferRead.hasRemaining()) {
            return;
        }

        byteBufferRead.position(readSocketService.getProcessPosition());
        byteBufferRead.compact();
        readSocketService.setProcessPosition(0);
    }
}

