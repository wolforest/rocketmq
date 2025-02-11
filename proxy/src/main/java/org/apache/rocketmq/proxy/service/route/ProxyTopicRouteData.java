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
package org.apache.rocketmq.proxy.service.route;

import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

public class ProxyTopicRouteData {
    private List<QueueData> queueList;
    private List<ProxyBrokerData> brokerList;


    public ProxyTopicRouteData() {
    }

    public ProxyTopicRouteData(TopicRouteData topicRouteData) {
        this.queueList = topicRouteData.getQueueList();
        this.brokerList = new ArrayList<>();

        for (BrokerData brokerData : topicRouteData.getBrokerList()) {
            ProxyTopicRouteData.ProxyBrokerData proxyBrokerData = new ProxyTopicRouteData.ProxyBrokerData();
            proxyBrokerData.setCluster(brokerData.getCluster());
            proxyBrokerData.setBrokerName(brokerData.getBrokerName());
            for (Long brokerId : brokerData.getBrokerAddrs().keySet()) {
                String brokerAddr = brokerData.getBrokerAddrs().get(brokerId);
                HostAndPort hostAndPort = HostAndPort.fromString(brokerAddr);

                proxyBrokerData.getBrokerAddrs().put(brokerId, Lists.newArrayList(new Address(Address.AddressScheme.IPv4, hostAndPort)));
            }
            this.brokerList.add(proxyBrokerData);
        }
    }

    public ProxyTopicRouteData(TopicRouteData topicRouteData, int port) {
        this.queueList = topicRouteData.getQueueList();
        this.brokerList = new ArrayList<>();

        for (BrokerData brokerData : topicRouteData.getBrokerList()) {
            ProxyTopicRouteData.ProxyBrokerData proxyBrokerData = new ProxyTopicRouteData.ProxyBrokerData();
            proxyBrokerData.setCluster(brokerData.getCluster());
            proxyBrokerData.setBrokerName(brokerData.getBrokerName());
            for (Long brokerId : brokerData.getBrokerAddrs().keySet()) {
                String brokerAddr = brokerData.getBrokerAddrs().get(brokerId);
                HostAndPort brokerHostAndPort = HostAndPort.fromString(brokerAddr);
                HostAndPort hostAndPort = HostAndPort.fromParts(brokerHostAndPort.getHost(), port);

                proxyBrokerData.getBrokerAddrs().put(brokerId, Lists.newArrayList(new Address(Address.AddressScheme.IPv4, hostAndPort)));
            }
            this.brokerList.add(proxyBrokerData);
        }
    }

    public ProxyTopicRouteData(TopicRouteData topicRouteData, List<Address> requestHostAndPortList) {
        this.queueList = topicRouteData.getQueueList();
        this.brokerList = new ArrayList<>();

        for (BrokerData brokerData : topicRouteData.getBrokerList()) {
            ProxyTopicRouteData.ProxyBrokerData proxyBrokerData = new ProxyTopicRouteData.ProxyBrokerData();
            proxyBrokerData.setCluster(brokerData.getCluster());
            proxyBrokerData.setBrokerName(brokerData.getBrokerName());
            for (Long brokerId : brokerData.getBrokerAddrs().keySet()) {
                proxyBrokerData.getBrokerAddrs().put(brokerId, requestHostAndPortList);
            }
            this.brokerList.add(proxyBrokerData);
        }
    }

    public static class ProxyBrokerData {
        private String cluster;
        private String brokerName;
        private Map<Long/* brokerId */, List<Address>/* broker address */> brokerAddrs = new HashMap<>();

        public String getCluster() {
            return cluster;
        }

        public void setCluster(String cluster) {
            this.cluster = cluster;
        }

        public String getBrokerName() {
            return brokerName;
        }

        public void setBrokerName(String brokerName) {
            this.brokerName = brokerName;
        }

        public Map<Long, List<Address>> getBrokerAddrs() {
            return brokerAddrs;
        }

        public void setBrokerAddrs(Map<Long, List<Address>> brokerAddrs) {
            this.brokerAddrs = brokerAddrs;
        }

        public BrokerData buildBrokerData() {
            BrokerData brokerData = new BrokerData();
            brokerData.setCluster(cluster);
            brokerData.setBrokerName(brokerName);
            HashMap<Long, String> buildBrokerAddress = new HashMap<>();
            brokerAddrs.forEach((k, v) -> {
                if (!v.isEmpty()) {
                    buildBrokerAddress.put(k, v.get(0).getHostAndPort().toString());
                }
            });
            brokerData.setBrokerAddrs(buildBrokerAddress);
            return brokerData;
        }
    }

    private List<QueueData> queueDatas = new ArrayList<>();
    private List<ProxyBrokerData> brokerDatas = new ArrayList<>();

    public List<QueueData> getQueueList() {
        return queueList;
    }

    public void setQueueList(List<QueueData> queueList) {
        this.queueList = queueList;
    }

    public List<ProxyBrokerData> getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(List<ProxyBrokerData> brokerList) {
        this.brokerList = brokerList;
    }

    public TopicRouteData buildTopicRouteData() {
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setQueueList(queueList);
        topicRouteData.setBrokerList(brokerList.stream()
            .map(ProxyBrokerData::buildBrokerData)
            .collect(Collectors.toList()));
        return topicRouteData;
    }
}
