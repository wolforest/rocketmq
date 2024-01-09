package org.apache.rocketmq.apitest.admin;

import org.apache.rocketmq.apitest.ApiBaseTest;
import org.apache.rocketmq.apitest.manager.GroupManager;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.Assert;
import org.testng.annotations.Test;

@Test(groups = "admin")
public class GroupTest extends ApiBaseTest {

    @Test
    public void testAddAndDeleteGroupTest() {
        String group = GroupManager.createUniqueGroup();
        boolean status = GroupManager.createGroup(group);
        if (!status) {
            return;
        }

        SubscriptionGroupConfig groupConfig = GroupManager.findGroup(group);
        Assert.assertNotNull(groupConfig);
        Assert.assertEquals(group, groupConfig.getGroupName());

        GroupManager.deleteGroup(group);

        SubscriptionGroupConfig groupConfig1 = GroupManager.findGroup(group);
        Assert.assertNull(groupConfig1);
    }
}
