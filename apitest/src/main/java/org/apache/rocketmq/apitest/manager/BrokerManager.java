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

import java.util.HashMap;
import java.util.Properties;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;

public class BrokerManager {

    public static ClusterInfo getClusterInfo() {
        try {
            String nameAddr = ConfigManager.getConfig().getString("nameAddr");
            return ClientManager.getClient().getBrokerClusterInfo(nameAddr);
        } catch (Exception e) {
            e.printStackTrace();
            ClusterInfo info = new ClusterInfo();
            info.setBrokerAddrTable(new HashMap<>());
            info.setClusterAddrTable(new HashMap<>());
            return info;
        }
    }

    public static void addBroker() {

    }

    public static void removeBroker() {

    }

    public static void updateBrokerConfig() {

    }

    public static Properties getBrokerConfig() {
        return null;
    }

}
