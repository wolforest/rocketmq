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
package org.apache.rocketmq.apitest.manager;

import org.apache.rocketmq.common.utils.StringUtils;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public class GroupManager {
    private static final String GROUP_PREFIX = "MQG_";

    public static boolean createOrderlyGroup(String group) {
        ClusterInfo clusterInfo = BrokerManager.getClusterInfo();
        if (clusterInfo == null) {
            return false;
        }

        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName(group);
        config.setConsumeMessageOrderly(true);

        try {
            for (String addr: clusterInfo.getAllAddr()) {
                ClientManager.getClient().createSubscriptionGroup(addr, config);
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean createGroup(String group) {
        ClusterInfo clusterInfo = BrokerManager.getClusterInfo();
        if (clusterInfo == null) {
            return false;
        }

        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName(group);

        try {
            for (String addr: clusterInfo.getAllAddr()) {
                ClientManager.getClient().createSubscriptionGroup(addr, config);
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    public static SubscriptionGroupConfig findGroup(String group) {
        ClusterInfo clusterInfo = BrokerManager.getClusterInfo();
        if (clusterInfo == null) {
            return null;
        }

        try {
            SubscriptionGroupConfig config = null;
            for (String addr: clusterInfo.getAllAddr()) {
                config = ClientManager.getClient().getSubscriptionGroupConfig(addr, group);
                if (config == null) {
                    return null;
                }
            }

            return config;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void deleteGroup(String group) {
        ClusterInfo clusterInfo = BrokerManager.getClusterInfo();
        if (clusterInfo == null) {
            return ;
        }

        try {
            for (String addr: clusterInfo.getAllAddr()) {
                ClientManager.getClient().deleteSubscriptionGroup(addr, group, true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String createUniqueGroup() {
        return GROUP_PREFIX + StringUtils.UUID();
    }

}
