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
package org.apache.rocketmq.apitest.admin;

import org.apache.rocketmq.apitest.ApiBaseTest;
import org.apache.rocketmq.apitest.manager.BrokerManager;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "admin")
public class ClusterTest extends ApiBaseTest {

    @Test
    public void testGetClusterInfoWork() {
        ClusterInfo clusterInfo = BrokerManager.getClusterInfo();
        if (clusterInfo == null) {
            return;
        }

        Assert.assertFalse(clusterInfo.getBrokerAddrTable().isEmpty());
        Assert.assertFalse(clusterInfo.getClusterAddrTable().isEmpty());
        Assert.assertFalse(clusterInfo.getAllAddr().isEmpty());
    }
}
