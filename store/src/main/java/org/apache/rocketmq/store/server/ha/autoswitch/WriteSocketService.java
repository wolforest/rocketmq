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
import org.apache.rocketmq.store.infra.mappedfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.server.store.DefaultMessageStore;

public class WriteSocketService extends AbstractWriteSocketService {
    private SelectMappedBufferResult selectMappedBufferResult;

    public WriteSocketService(final SocketChannel socketChannel, AutoSwitchHAConnection haConnection) throws IOException {
        super(socketChannel, haConnection);
    }

    @Override
    protected int getNextTransferDataSize() {
        SelectMappedBufferResult selectResult = haConnection.getHaService().getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
        if (selectResult == null || selectResult.getSize() <= 0) {
            return 0;
        }
        this.selectMappedBufferResult = selectResult;
        return selectResult.getSize();
    }

    @Override
    protected void releaseData() {
        this.selectMappedBufferResult.release();
        this.selectMappedBufferResult = null;
    }

    @Override
    protected boolean transferData(int maxTransferSize) throws Exception {
        if (null != this.selectMappedBufferResult && maxTransferSize >= 0) {
            this.selectMappedBufferResult.getByteBuffer().limit(maxTransferSize);
        }

        // Write Header
        boolean result = haWriter.write(this.socketChannel, this.byteBufferHeader);
        if (!result) {
            return false;
        }

        if (null == this.selectMappedBufferResult) {
            return true;
        }

        // Write Body
        result = haWriter.write(this.socketChannel, this.selectMappedBufferResult.getByteBuffer());
        if (result) {
            releaseData();
        }

        return result;
    }

    @Override
    protected void onStop() {
        if (this.selectMappedBufferResult != null) {
            this.selectMappedBufferResult.release();
        }
    }

    @Override
    public String getServiceName() {
        DefaultMessageStore store = haConnection.getHaService().getDefaultMessageStore();
        if (store.getBrokerConfig().isInBrokerContainer()) {
            return store.getBrokerIdentity().getIdentifier() + WriteSocketService.class.getSimpleName();
        }
        return WriteSocketService.class.getSimpleName();
    }
}

