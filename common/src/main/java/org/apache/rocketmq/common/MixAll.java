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
package org.apache.rocketmq.common;

public class MixAll {
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
    public static final String MULTI_DISPATCH_QUEUE_SPLITTER = ",";
    public static final String REQ_T = "ReqT";
    public static final String ROCKETMQ_ZONE_ENV = "ROCKETMQ_ZONE";
    public static final String ROCKETMQ_ZONE_PROPERTY = "rocketmq.zone";
    public static final String ROCKETMQ_ZONE_MODE_ENV = "ROCKETMQ_ZONE_MODE";
    public static final String ROCKETMQ_ZONE_MODE_PROPERTY = "rocketmq.zone.mode";
    public static final String ZONE_NAME = "__ZONE_NAME";
    public static final String ZONE_MODE = "__ZONE_MODE";
    public static final String LOGICAL_QUEUE_MOCK_BROKER_PREFIX = "__syslo__";
    public static final String METADATA_SCOPE_GLOBAL = "__global__";
    public static final String LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST = "__syslo__none__";

}
