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
package org.apache.rocketmq.broker.service.pop;

import java.util.HashMap;
import org.apache.rocketmq.store.pop.PopCheckPoint;

public class ReviveContext {
    private final ConsumeReviveObj consumeReviveObj;
    private final HashMap<String, PopCheckPoint> mockPointMap;

    private final long startScanTime;
    private long endTime;
    private long firstRt;

    private int noMsgCount;

    private final long consumeOffset;
    private final long oldOffset;
    private long offset;

    public ReviveContext(long consumeOffset, long reviveOffset) {
        this.consumeReviveObj = new ConsumeReviveObj();
        this.mockPointMap = new HashMap<>();

        this.startScanTime = System.currentTimeMillis();
        this.endTime = 0;
        this.firstRt = 0;
        this.noMsgCount = 0;

        this.consumeOffset = consumeOffset;
        this.oldOffset = Math.max(reviveOffset, consumeOffset);
        this.consumeReviveObj.setOldOffset(oldOffset);

        this.offset = oldOffset + 1;
    }

    public void increaseNoMsgCount() {
        this.noMsgCount++;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public void setFirstRt(long firstRt) {
        this.firstRt = firstRt;
    }

    public void setNoMsgCount(int noMsgCount) {
        this.noMsgCount = noMsgCount;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public ConsumeReviveObj getConsumeReviveObj() {
        return consumeReviveObj;
    }

    public HashMap<String, PopCheckPoint> getMap() {
        return consumeReviveObj.getMap();
    }

    public HashMap<String, PopCheckPoint> getMockPointMap() {
        return mockPointMap;
    }

    public long getStartScanTime() {
        return startScanTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public long getFirstRt() {
        return firstRt;
    }

    public int getNoMsgCount() {
        return noMsgCount;
    }

    public long getConsumeOffset() {
        return consumeOffset;
    }

    public long getOldOffset() {
        return oldOffset;
    }

    public long getOffset() {
        return offset;
    }

}
