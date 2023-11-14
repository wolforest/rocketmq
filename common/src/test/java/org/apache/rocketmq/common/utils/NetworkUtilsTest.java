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
package org.apache.rocketmq.common.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NetworkUtilsTest {
    @Test
    public void testGetLocalAddress() {
        String localAddress = NetworkUtils.getLocalAddress();
        assertThat(localAddress).isNotNull();
        assertThat(localAddress.length()).isGreaterThan(0);
        assertThat(localAddress).isNotEqualTo(InetAddress.getLoopbackAddress().getHostAddress());
    }

    @Test
    public void testConvert2IpStringWithIp() {
        String result = NetworkUtils.convert2IpString("127.0.0.1:9876");
        assertThat(result).isEqualTo("127.0.0.1:9876");
    }

    @Test
    public void testConvert2IpStringWithHost() {
        String result = NetworkUtils.convert2IpString("localhost:9876");
        assertThat(result).isEqualTo("127.0.0.1:9876");
    }

    @Test
    public void testGetPid() {
        assertThat(NetworkPortUtils.getPid()).isGreaterThan(0);
    }

    @Test
    public void testIPv6Check() throws UnknownHostException {
        InetAddress nonInternal = InetAddress.getByName("2408:4004:0180:8100:3FAA:1DDE:2B3F:898A");
        InetAddress internal = InetAddress.getByName("FE80:0000:0000:0000:0000:0000:0000:FFFF");
        assertThat(NetworkUtils.isInternalV6IP(nonInternal)).isFalse();
        assertThat(NetworkUtils.isInternalV6IP(internal)).isTrue();
        assertThat(NetworkUtils.ipToIPv6Str(nonInternal.getAddress()).toUpperCase()).isEqualTo("2408:4004:0180:8100:3FAA:1DDE:2B3F:898A");
    }

    @Test
    public void testGetLocalhostByNetworkInterface() throws Exception {
        assertThat(NetworkUtils.LOCALHOST).isNotNull();
        assertThat(NetworkUtils.getLocalhostByNetworkInterface()).isNotNull();
    }

    @Test
    public void testGetLocalInetAddress() throws Exception {
        List<String> localInetAddress = NetworkUtils.getLocalInetAddress();
        String local = InetAddress.getLocalHost().getHostAddress();
        assertThat(localInetAddress).contains("127.0.0.1");
        assertThat(local).isNotNull();
    }
}
