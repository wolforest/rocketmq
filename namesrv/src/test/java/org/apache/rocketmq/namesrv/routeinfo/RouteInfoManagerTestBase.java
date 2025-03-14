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
package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.remoting.protocol.route.GroupInfo;

public class RouteInfoManagerTestBase {

    protected static class Cluster {
        ConcurrentMap<String, TopicConfig> topicConfig;
        Map<String, GroupInfo> brokerDataMap;

        public Cluster(ConcurrentMap<String, TopicConfig> topicConfig, Map<String, GroupInfo> brokerData) {
            this.topicConfig = topicConfig;
            this.brokerDataMap = brokerData;
        }

        public Set<String> getAllBrokerName() {
            return brokerDataMap.keySet();
        }

        public Set<String> getAllTopicName() {
            return topicConfig.keySet();
        }
    }

    protected Cluster registerCluster(RouteInfoManager routeInfoManager, String cluster,
                                      String brokerNamePrefix,
                                      int brokerNameNumber,
                                      int brokerPerName,
                                      String topicPrefix,
                                      int topicNumber) {

        Map<String, GroupInfo> brokerDataMap = new HashMap<>();

        // no filterServer address
        List<String> filterServerAddr = new ArrayList<>();

        ConcurrentMap<String, TopicConfig> topicConfig = genTopicConfig(topicPrefix, topicNumber);

        for (int i = 0; i < brokerNameNumber; i++) {
            String brokerName = getBrokerName(brokerNamePrefix, i);

            GroupInfo groupInfo = genBrokerData(cluster, brokerName, brokerPerName, true);

            // avoid object reference copy
            ConcurrentMap<String, TopicConfig> topicConfigForBroker = genTopicConfig(topicPrefix, topicNumber);

            registerBrokerWithTopicConfig(routeInfoManager, groupInfo, topicConfigForBroker, filterServerAddr);

            // avoid object reference copy
            brokerDataMap.put(groupInfo.getBrokerName(), genBrokerData(cluster, brokerName, brokerPerName, true));
        }

        return new Cluster(topicConfig, brokerDataMap);
    }

    protected String getBrokerAddr(String cluster, String brokerName, long brokerNumber) {
        return cluster + "-" + brokerName + ":" + brokerNumber;
    }

    protected GroupInfo genBrokerData(String clusterName, String brokerName, long totalBrokerNumber, boolean hasMaster) {
        HashMap<Long, String> brokerAddrMap = new HashMap<>();

        long startId = 0;
        if (hasMaster) {
            brokerAddrMap.put(MQConstants.MASTER_ID, getBrokerAddr(clusterName, brokerName, MQConstants.MASTER_ID));
            startId = 1;
        }

        for (long i = startId; i < totalBrokerNumber; i++) {
            brokerAddrMap.put(i, getBrokerAddr(clusterName, brokerName, i));
        }

        return new GroupInfo(clusterName, brokerName, brokerAddrMap);
    }

    protected void registerBrokerWithTopicConfig(RouteInfoManager routeInfoManager, GroupInfo groupInfo,
                                                 ConcurrentMap<String, TopicConfig> topicConfigTable,
                                                 List<String> filterServerAddr) {

        groupInfo.getBrokerAddrs().forEach((brokerId, brokerAddr) -> {
            registerBrokerWithTopicConfig(routeInfoManager, groupInfo.getCluster(),
                    brokerAddr,
                    groupInfo.getBrokerName(),
                    brokerId,
                    brokerAddr, // set ha server address the same as brokerAddr
                    new ConcurrentHashMap<>(topicConfigTable),
                    new ArrayList<>(filterServerAddr));
        });
    }

    protected void unregisterBrokerAll(RouteInfoManager routeInfoManager, GroupInfo groupInfo) {
        for (Map.Entry<Long, String> entry : groupInfo.getBrokerAddrs().entrySet()) {
            routeInfoManager.unregisterBroker(groupInfo.getCluster(), entry.getValue(), groupInfo.getBrokerName(), entry.getKey());
        }
    }

    protected void unregisterBroker(RouteInfoManager routeInfoManager, GroupInfo groupInfo, long brokerId) {
        HashMap<Long, String> brokerAddrs = groupInfo.getBrokerAddrs();
        if (brokerAddrs.containsKey(brokerId)) {
            String address = brokerAddrs.remove(brokerId);
            routeInfoManager.unregisterBroker(groupInfo.getCluster(), address, groupInfo.getBrokerName(), brokerId);
        }
    }

    protected RegisterBrokerResult registerBrokerWithTopicConfig(RouteInfoManager routeInfoManager, String clusterName,
                                                 String brokerAddr,
                                                 String brokerName,
                                                 long brokerId,
                                                 String haServerAddr,
                                                 ConcurrentMap<String, TopicConfig> topicConfigTable,
                                                 List<String> filterServerAddr) {

        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigTable);

        Channel channel = new EmbeddedChannel();
        return routeInfoManager.registerBroker(clusterName,
                brokerAddr,
                brokerName,
                brokerId,
                "",
                haServerAddr,
                null,
                topicConfigSerializeWrapper,
                filterServerAddr,
                channel);
    }


    protected String getTopicName(String topicPrefix, int topicNumber) {
        return topicPrefix + "-" + topicNumber;
    }

    protected ConcurrentMap<String, TopicConfig> genTopicConfig(String topicPrefix, int topicNumber) {
        ConcurrentMap<String, TopicConfig> topicConfigMap = new ConcurrentHashMap<>();

        for (int i = 0; i < topicNumber; i++) {
            String topicName = getTopicName(topicPrefix, i);

            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setWriteQueueNums(8);
            topicConfig.setTopicName(topicName);
            topicConfig.setPerm(6);
            topicConfig.setReadQueueNums(8);
            topicConfig.setOrder(false);
            topicConfigMap.put(topicName, topicConfig);
        }

        return topicConfigMap;
    }

    protected String getBrokerName(String brokerNamePrefix, long brokerNameNumber) {
        return brokerNamePrefix + "-" + brokerNameNumber;
    }

    protected GroupInfo findBrokerDataByBrokerName(List<GroupInfo> data, String brokerName) {
        return data.stream().filter(bd -> bd.getBrokerName().equals(brokerName)).findFirst().orElse(null);
    }

}
