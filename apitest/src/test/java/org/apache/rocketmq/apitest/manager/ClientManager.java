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

import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.MQAdminExt;

import static org.apache.rocketmq.common.utils.NameServerAddressUtils.NAMESRV_ADDR_PROPERTY;

public class ClientManager {
    private static MQAdminExt CLIENT = null;

    private static final ClientServiceProvider provider = ClientServiceProvider.loadService();

    public static ClientServiceProvider getProvider() {
        return provider;
    }

    public static MQAdminExt getClient() {
        return CLIENT;
    }

    public static void start() throws MQClientException {
        if (CLIENT != null) {
            return;
        }

        System.setProperty(NAMESRV_ADDR_PROPERTY, ConfigManager.getConfig().getString("nameAddr"));
        CLIENT = new DefaultMQAdminExt();
        CLIENT.start();
    }

    public static void shutdown() {
        CLIENT.shutdown();
    }

}
