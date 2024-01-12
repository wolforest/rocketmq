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
import org.apache.rocketmq.remoting.client.DefaultRPCClient;

import static org.apache.rocketmq.common.utils.NameServerAddressUtils.NAMESRV_ADDR_PROPERTY;

public class ClientManager {
    private static DefaultRPCClient client = null;

    private static final ClientServiceProvider PROVIDER = ClientServiceProvider.loadService();

    public static ClientServiceProvider getProvider() {
        return PROVIDER;
    }

    public static DefaultRPCClient getClient() {
        return client;
    }

    public static void start() {
        if (client != null) {
            return;
        }

        System.setProperty(NAMESRV_ADDR_PROPERTY, ConfigManager.getConfig().getString("nameAddr"));
        client = new DefaultRPCClient();
        client.start();
    }

    public static void shutdown() {
        client.shutdown();
    }

}
