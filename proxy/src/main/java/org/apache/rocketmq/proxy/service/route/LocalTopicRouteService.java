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
import java.util.HashMap;
import java.util.List;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.domain.message.MessageQueue;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

public class LocalTopicRouteService extends TopicRouteService {

    private final Broker broker;
    private final List<BrokerData> brokerDataList;
    private final int grpcPort;

    public LocalTopicRouteService(Broker broker, MQClientAPIFactory mqClientAPIFactory) {
        super(mqClientAPIFactory);
        this.broker = broker;
        BrokerConfig brokerConfig = this.broker.getBrokerConfig();
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(MQConstants.MASTER_ID, this.broker.getBrokerAddr());
        this.brokerDataList = Lists.newArrayList(
            new BrokerData(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(), brokerAddrs)
        );
        this.grpcPort = ConfigurationManager.getProxyConfig().getGrpcServerPort();
    }

    @Override
    public MessageQueueView getCurrentMessageQueueView(ProxyContext ctx, String topic) throws Exception {
        TopicConfig topicConfig = this.broker.getTopicConfigManager().getTopicConfigTable().get(topic);
        return new MessageQueueView(topic, toTopicRouteData(topicConfig), null);
    }

    /**
     * get route info by topic
     * @renamed from getTopicRouteForProxy to getRouteByTopic
     *
     * @param ctx proxy context
     * @param requestHostAndPortList broker endpoints from request(client config), it is useless here
     * @param topicName topic
     * @return route data
     * @throws Exception e
     */
    @Override
    public ProxyTopicRouteData getTopicRouteForProxy(ProxyContext ctx, List<Address> requestHostAndPortList,
        String topicName) throws Exception {
        MessageQueueView messageQueueView = getAllMessageQueueView(ctx, topicName);
        TopicRouteData topicRouteData = messageQueueView.getTopicRouteData();
        return new ProxyTopicRouteData(topicRouteData, grpcPort);
    }

    @Override
    public String getBrokerAddr(ProxyContext ctx, String brokerName) throws Exception {
        return this.broker.getBrokerAddr();
    }

    @Override
    public AddressableMessageQueue buildAddressableMessageQueue(ProxyContext ctx, MessageQueue messageQueue) throws Exception {
        String brokerAddress = getBrokerAddr(ctx, messageQueue.getBrokerName());
        return new AddressableMessageQueue(messageQueue, brokerAddress);
    }

    protected TopicRouteData toTopicRouteData(TopicConfig topicConfig) {
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setBrokerList(brokerDataList);

        QueueData queueData = new QueueData();
        queueData.setPerm(topicConfig.getPerm());
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());
        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        queueData.setTopicSysFlag(topicConfig.getTopicSysFlag());
        queueData.setBrokerName(this.broker.getBrokerConfig().getBrokerName());
        topicRouteData.setQueueList(Lists.newArrayList(queueData));

        return topicRouteData;
    }
}
