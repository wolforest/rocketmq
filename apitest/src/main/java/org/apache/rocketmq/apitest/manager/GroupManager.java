package org.apache.rocketmq.apitest.manager;

import org.apache.rocketmq.common.utils.StringUtils;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public class GroupManager {
    private static final String GROUP_PREFIX = "MQG_";

    public static boolean createGroup(String group) {
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName(group);

        String brokerAddr = ConfigManager.getConfig().getString("brokerAddr");
        try {
            ClientManager.getClient().createSubscriptionGroup(brokerAddr, config);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    public static SubscriptionGroupConfig findGroup(String group) {
        try {
            String brokerAddr = ConfigManager.getConfig().getString("brokerAddr");
            return ClientManager.getClient().getSubscriptionGroupConfig(brokerAddr, group);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void deleteGroup(String group) {
        try {
            String brokerAddr = ConfigManager.getConfig().getString("brokerAddr");
            ClientManager.getClient().deleteSubscriptionGroup(brokerAddr, group, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String createUniqueGroup() {
        return GROUP_PREFIX + StringUtils.UUID();
    }

}
