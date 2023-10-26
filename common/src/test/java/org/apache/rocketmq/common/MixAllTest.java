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

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.utils.MQUtils;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.common.utils.NumberUtils;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MixAllTest {
    @Test
    public void testGetLocalInetAddress() throws Exception {
        List<String> localInetAddress = NetworkUtil.getLocalInetAddress();
        String local = InetAddress.getLocalHost().getHostAddress();
        assertThat(localInetAddress).contains("127.0.0.1");
        assertThat(local).isNotNull();
    }

    @Test
    public void testCompareAndIncreaseOnly() {
        AtomicLong target = new AtomicLong(5);
        assertThat(NumberUtils.compareAndIncreaseOnly(target, 6)).isTrue();
        assertThat(target.get()).isEqualTo(6);

        assertThat(NumberUtils.compareAndIncreaseOnly(target, 4)).isFalse();
        assertThat(target.get()).isEqualTo(6);
    }

    @Test
    public void testGetLocalhostByNetworkInterface() throws Exception {
        assertThat(NetworkUtil.LOCALHOST).isNotNull();
        assertThat(NetworkUtil.getLocalhostByNetworkInterface()).isNotNull();
    }

    @Test
    public void testIsLmq() {
        String testLmq = null;
        assertThat(MQUtils.isLmq(testLmq)).isFalse();
        testLmq = "lmq";
        assertThat(MQUtils.isLmq(testLmq)).isFalse();
        testLmq = "%LMQ%queue123";
        assertThat(MQUtils.isLmq(testLmq)).isTrue();
        testLmq = "%LMQ%GID_TEST";
        assertThat(MQUtils.isLmq(testLmq)).isTrue();
    }
}
