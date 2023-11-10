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
package org.apache.rocketmq.broker.util;

import org.apache.rocketmq.common.constant.PopConstants;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.BatchAckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;

public class PopUtils {
    public static String genAckUniqueId(AckMsg ackMsg) {
        return ackMsg.getTopic()
            + PopConstants.SPLIT + ackMsg.getQueueId()
            + PopConstants.SPLIT + ackMsg.getAckOffset()
            + PopConstants.SPLIT + ackMsg.getConsumerGroup()
            + PopConstants.SPLIT + ackMsg.getPopTime()
            + PopConstants.SPLIT + ackMsg.getBrokerName()
            + PopConstants.SPLIT + PopConstants.ACK_TAG;
    }

    public static String genBatchAckUniqueId(BatchAckMsg batchAckMsg) {
        return batchAckMsg.getTopic()
            + PopConstants.SPLIT + batchAckMsg.getQueueId()
            + PopConstants.SPLIT + batchAckMsg.getAckOffsetList().toString()
            + PopConstants.SPLIT + batchAckMsg.getConsumerGroup()
            + PopConstants.SPLIT + batchAckMsg.getPopTime()
            + PopConstants.SPLIT + PopConstants.BATCH_ACK_TAG;
    }

    public static String genCkUniqueId(PopCheckPoint ck) {
        return ck.getTopic()
            + PopConstants.SPLIT + ck.getQueueId()
            + PopConstants.SPLIT + ck.getStartOffset()
            + PopConstants.SPLIT + ck.getCId()
            + PopConstants.SPLIT + ck.getPopTime()
            + PopConstants.SPLIT + ck.getBrokerName()
            + PopConstants.SPLIT + PopConstants.CK_TAG;
    }
}
