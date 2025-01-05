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
package org.apache.rocketmq.common.domain.constant;

/**
 * @renamed from MixAll to MQConstants
 */
public class MQConstants {
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
    public static final String ROCKETMQ_ZONE_ENV = "ROCKETMQ_ZONE";
    public static final String ROCKETMQ_ZONE_PROPERTY = "rocketmq.zone";
    public static final String ROCKETMQ_ZONE_MODE_ENV = "ROCKETMQ_ZONE_MODE";
    public static final String ROCKETMQ_ZONE_MODE_PROPERTY = "rocketmq.zone.mode";
    public static final String ZONE_NAME = "__ZONE_NAME";
    public static final String ZONE_MODE = "__ZONE_MODE";
    public final static String RPC_REQUEST_HEADER_NAMESPACED_FIELD = "nsd";
    public final static String RPC_REQUEST_HEADER_NAMESPACE_FIELD = "ns";

    public static final String MESSAGE_COMPRESS_TYPE = "rocketmq.message.compressType";
    public static final String MESSAGE_COMPRESS_LEVEL = "rocketmq.message.compressLevel";
    public static final String DEFAULT_PRODUCER_GROUP = "DEFAULT_PRODUCER";
    public static final String SCHEDULE_CONSUMER_GROUP = "SCHEDULE_CONSUMER";
    public static final String FILTERSRV_CONSUMER_GROUP = "FILTERSRV_CONSUMER";
    public static final String MONITOR_CONSUMER_GROUP = "__MONITOR_CONSUMER";
    public static final String CLIENT_INNER_PRODUCER_GROUP = "CLIENT_INNER_PRODUCER";
    public static final String SELF_TEST_PRODUCER_GROUP = "SELF_TEST_P_GROUP";
    public static final String SELF_TEST_CONSUMER_GROUP = "SELF_TEST_C_GROUP";
    public static final String ONS_HTTP_PROXY_GROUP = "CID_ONS-HTTP-PROXY";
    public static final String CID_ONSAPI_PERMISSION_GROUP = "CID_ONSAPI_PERMISSION";
    public static final String CID_ONSAPI_OWNER_GROUP = "CID_ONSAPI_OWNER";
    public static final String CID_ONSAPI_PULL_GROUP = "CID_ONSAPI_PULL";
    public static final String IS_SUPPORT_HEART_BEAT_V2 = "IS_SUPPORT_HEART_BEAT_V2";
    public static final String IS_SUB_CHANGE = "IS_SUB_CHANGE";
    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final long MASTER_ID = 0L;
    public static final long FIRST_SLAVE_ID = 1L;
    public static final long FIRST_BROKER_CONTROLLER_ID = 1L;
    public final static int UNIT_PRE_SIZE_FOR_MSG = 28;
    public final static int ALL_ACK_IN_SYNC_STATE_SET = -1;
    public static final String CID_SYS_RMQ_TRANS = "CID_RMQ_SYS_TRANS";
    public static final String ACL_CONF_TOOLS_FILE = "/conf/tools.yml";
    public static final String REPLY_MESSAGE_FLAG = "reply";
    public static final String MULTI_DISPATCH_QUEUE_SPLITTER = ",";
    public static final String REQ_T = "ReqT";
    public static final String LOGICAL_QUEUE_MOCK_BROKER_PREFIX = "__syslo__";
    public static final String METADATA_SCOPE_GLOBAL = "__global__";
    public static final String LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST = "__syslo__none__";

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
        return DEFAULT_CONSUMER_GROUP.equals(consumerGroup)
            || TOOLS_CONSUMER_GROUP.equals(consumerGroup)
            || SCHEDULE_CONSUMER_GROUP.equals(consumerGroup)
            || FILTERSRV_CONSUMER_GROUP.equals(consumerGroup)
            || MONITOR_CONSUMER_GROUP.equals(consumerGroup)
            || SELF_TEST_CONSUMER_GROUP.equals(consumerGroup)
            || ONS_HTTP_PROXY_GROUP.equals(consumerGroup)
            || CID_ONSAPI_PERMISSION_GROUP.equals(consumerGroup)
            || CID_ONSAPI_OWNER_GROUP.equals(consumerGroup)
            || CID_ONSAPI_PULL_GROUP.equals(consumerGroup)
            || CID_SYS_RMQ_TRANS.equals(consumerGroup)
            || consumerGroup.startsWith(CID_RMQ_SYS_PREFIX);
    }
}
