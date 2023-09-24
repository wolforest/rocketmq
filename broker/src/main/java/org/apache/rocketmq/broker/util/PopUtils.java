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

import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.BatchAckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;

public class PopUtils {
    public static String genAckUniqueId(AckMsg ackMsg) {
        return ackMsg.getTopic()
            + PopAckConstants.SPLIT + ackMsg.getQueueId()
            + PopAckConstants.SPLIT + ackMsg.getAckOffset()
            + PopAckConstants.SPLIT + ackMsg.getConsumerGroup()
            + PopAckConstants.SPLIT + ackMsg.getPopTime()
            + PopAckConstants.SPLIT + ackMsg.getBrokerName()
            + PopAckConstants.SPLIT + PopAckConstants.ACK_TAG;
    }

    public static String genBatchAckUniqueId(BatchAckMsg batchAckMsg) {
        return batchAckMsg.getTopic()
            + PopAckConstants.SPLIT + batchAckMsg.getQueueId()
            + PopAckConstants.SPLIT + batchAckMsg.getAckOffsetList().toString()
            + PopAckConstants.SPLIT + batchAckMsg.getConsumerGroup()
            + PopAckConstants.SPLIT + batchAckMsg.getPopTime()
            + PopAckConstants.SPLIT + PopAckConstants.BATCH_ACK_TAG;
    }

    public static String genCkUniqueId(PopCheckPoint ck) {
        return ck.getTopic()
            + PopAckConstants.SPLIT + ck.getQueueId()
            + PopAckConstants.SPLIT + ck.getStartOffset()
            + PopAckConstants.SPLIT + ck.getCId()
            + PopAckConstants.SPLIT + ck.getPopTime()
            + PopAckConstants.SPLIT + ck.getBrokerName()
            + PopAckConstants.SPLIT + PopAckConstants.CK_TAG;
    }
}
