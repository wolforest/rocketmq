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
package org.apache.rocketmq.broker.domain.transaction.queue;

import java.util.List;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.store.api.dto.GetMessageResult;

public class GetMessageContext {
    private final GetMessageResult getMessageResult;
    private final String group;
    private final String topic;
    private final int queueId;
    private final long offset;

    private PullStatus pullStatus = PullStatus.NO_NEW_MSG;
    private List<MessageExt> foundList = null;

    public GetMessageContext(GetMessageResult getMessageResult, String group, String topic, int queueId, long offset) {
        this.getMessageResult = getMessageResult;
        this.group = group;
        this.topic = topic;
        this.queueId = queueId;
        this.offset = offset;
    }

    public boolean isFoundListEmpty() {
        if (null == foundList) {
            return false;
        }

        return foundList.size() == 0;
    }

    public MessageExt getLastFound() {
        return foundList.get(foundList.size() - 1);
    }

    public PullStatus getPullStatus() {
        return pullStatus;
    }

    public void setPullStatus(PullStatus pullStatus) {
        this.pullStatus = pullStatus;
    }

    public List<MessageExt> getFoundList() {
        return foundList;
    }

    public void setFoundList(List<MessageExt> foundList) {
        this.foundList = foundList;
    }

    public GetMessageResult getGetMessageResult() {
        return getMessageResult;
    }

    public String getGroup() {
        return group;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getOffset() {
        return offset;
    }

}
