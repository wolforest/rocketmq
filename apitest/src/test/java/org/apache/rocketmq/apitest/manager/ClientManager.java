package org.apache.rocketmq.apitest.manager;

import org.apache.rocketmq.client.apis.ClientServiceProvider;

public class ClientManager {
    private static ClientServiceProvider provider = ClientServiceProvider.loadService();

    public static ClientServiceProvider getProvider() {
        return provider;
    }
}
