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
package org.apache.rocketmq.broker.api.controller;

import org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem;

public class PullRewriteContext {
    private long requestOffset;
    private long nextBeginOffset;
    private long minOffset;
    private long maxOffset;
    private int responseCode;
    private LogicQueueMappingItem currentItem;

    public long getRequestOffset() {
        return requestOffset;
    }

    public void setRequestOffset(long requestOffset) {
        this.requestOffset = requestOffset;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    public LogicQueueMappingItem getCurrentItem() {
        return currentItem;
    }

    public void setCurrentItem(LogicQueueMappingItem currentItem) {
        this.currentItem = currentItem;
    }

}
