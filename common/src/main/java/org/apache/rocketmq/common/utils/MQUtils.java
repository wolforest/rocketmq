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
package org.apache.rocketmq.common.utils;

import org.apache.rocketmq.common.MixAll;

public class MQUtils {
    public static final String RETRY_GROUP_TOPIC_PREFIX = "%RETRY%";
    public static final String DLQ_GROUP_TOPIC_PREFIX = "%DLQ%";
    public static final String REPLY_TOPIC_POSTFIX = "REPLY_TOPIC";
    public static final String UNIQUE_MSG_QUERY_FLAG = "_UNIQUE_KEY_QUERY";
    public static final String DEFAULT_TRACE_REGION_ID = "DefaultRegion";
    public static final String CONSUME_CONTEXT_TYPE = "ConsumeContextType";
    public static final String CID_RMQ_SYS_PREFIX = "CID_RMQ_SYS_";
    public static final String LMQ_PREFIX = "%LMQ%";
    public static final String DEFAULT_CONSUMER_GROUP = "DEFAULT_CONSUMER";
    public static final String TOOLS_CONSUMER_GROUP = "TOOLS_CONSUMER";
    public static final String ROCKETMQ_HOME_ENV = "ROCKETMQ_HOME";
    public static final String ROCKETMQ_HOME_PROPERTY = "rocketmq.home.dir";
    public static final long LMQ_QUEUE_ID = 0;

    public static String getRetryTopic(final String consumerGroup) {
        return RETRY_GROUP_TOPIC_PREFIX + consumerGroup;
    }

    public static String getReplyTopic(final String clusterName) {
        return clusterName + "_" + REPLY_TOPIC_POSTFIX;
    }

    public static boolean isSysConsumerGroup(final String consumerGroup) {
        return consumerGroup.startsWith(CID_RMQ_SYS_PREFIX);
    }

    public static String getDLQTopic(final String consumerGroup) {
        return DLQ_GROUP_TOPIC_PREFIX + consumerGroup;
    }

    public static boolean isLmq(String lmqMetaData) {
        return lmqMetaData != null && lmqMetaData.startsWith(LMQ_PREFIX);
    }

    public static boolean isSysConsumerGroupForNoColdReadLimit(String consumerGroup) {
        if (DEFAULT_CONSUMER_GROUP.equals(consumerGroup)
            || TOOLS_CONSUMER_GROUP.equals(consumerGroup)
            || MixAll.SCHEDULE_CONSUMER_GROUP.equals(consumerGroup)
            || MixAll.FILTERSRV_CONSUMER_GROUP.equals(consumerGroup)
            || MixAll.MONITOR_CONSUMER_GROUP.equals(consumerGroup)
            || MixAll.SELF_TEST_CONSUMER_GROUP.equals(consumerGroup)
            || MixAll.ONS_HTTP_PROXY_GROUP.equals(consumerGroup)
            || MixAll.CID_ONSAPI_PERMISSION_GROUP.equals(consumerGroup)
            || MixAll.CID_ONSAPI_OWNER_GROUP.equals(consumerGroup)
            || MixAll.CID_ONSAPI_PULL_GROUP.equals(consumerGroup)
            || MixAll.CID_SYS_RMQ_TRANS.equals(consumerGroup)
            || consumerGroup.startsWith(CID_RMQ_SYS_PREFIX)) {
            return true;
        }
        return false;
    }
}
