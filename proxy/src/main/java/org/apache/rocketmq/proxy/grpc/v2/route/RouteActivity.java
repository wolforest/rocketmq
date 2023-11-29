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
package org.apache.rocketmq.proxy.grpc.v2.route;

import apache.rocketmq.v2.Address;
import apache.rocketmq.v2.AddressScheme;
import apache.rocketmq.v2.Assignment;
import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Endpoints;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.Permission;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.Resource;
import com.google.common.net.HostAndPort;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.constant.MQConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public class RouteActivity extends AbstractMessingActivity {

    public RouteActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    /**
     * query route info by topic
     *
     * @param ctx ctx
     * @param request {
     *        topic: xxx,
     *        endpoints: from client config, it is a endpoint list, it's a bad design
     *      }
     * @return route info {
     *     message_queues: [
     *     {
     *        topic: xxx,
     *        permission: xxx,
     *        broker: xxxx,
     *        accept_message_type: xxx
     *     }, ...
     *     ]
     * }
     */
    public CompletableFuture<QueryRouteResponse> queryRoute(ProxyContext ctx, QueryRouteRequest request) {
        CompletableFuture<QueryRouteResponse> future = new CompletableFuture<>();
        try {
            validateTopic(request.getTopic());
            List<MessageQueue> messageQueueList = getMessageQueueList(ctx, request);
            QueryRouteResponse response = buildRouteResponse(messageQueueList);
            future.complete(response);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<QueryAssignmentResponse> queryAssignment(ProxyContext ctx, QueryAssignmentRequest request) {
        CompletableFuture<QueryAssignmentResponse> future = new CompletableFuture<>();

        try {
            validateTopicAndConsumerGroup(request.getTopic(), request.getGroup());
            List<org.apache.rocketmq.proxy.common.Address> addressList = this.convertToAddressList(request.getEndpoints());
            String topic = GrpcConverter.getInstance().wrapResourceWithNamespace(request.getTopic());

            ProxyTopicRouteData routeData = this.messagingProcessor.getTopicRouteDataForProxy(ctx, addressList, topic);

            boolean fifo = getFifo(ctx, request);
            List<Assignment> assignments = getAssignmentList(fifo, routeData, request);

            QueryAssignmentResponse response = buildAssignmentResponse(assignments);
            future.complete(response);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    private List<MessageQueue> getMessageQueueList(ProxyContext ctx, QueryRouteRequest request) throws Exception {
        List<org.apache.rocketmq.proxy.common.Address> addressList = this.convertToAddressList(request.getEndpoints());
        String topicName = GrpcConverter.getInstance().wrapResourceWithNamespace(request.getTopic());
        ProxyTopicRouteData proxyTopicRouteData = this.messagingProcessor.getTopicRouteDataForProxy(ctx, addressList, topicName);

        List<MessageQueue> messageQueueList = new ArrayList<>();
        Map<String, Map<Long, Broker>> brokerMap = buildBrokerMap(proxyTopicRouteData.getBrokerDatas());
        TopicMessageType topicMessageType = messagingProcessor.getMetadataService().getTopicMessageType(ctx, topicName);

        for (QueueData queueData : proxyTopicRouteData.getQueueDatas()) {
            parseQueueData(queueData, topicMessageType, request, brokerMap, messageQueueList);
        }

        return messageQueueList;
    }

    private void parseQueueData(QueueData queueData, TopicMessageType topicMessageType, QueryRouteRequest request,
        Map<String, Map<Long, Broker>> brokerMap, List<MessageQueue> messageQueueList) {
        String brokerName = queueData.getBrokerName();
        Map<Long, Broker> brokerIdMap = brokerMap.get(brokerName);
        if (brokerIdMap == null) {
            return;
        }

        for (Broker broker : brokerIdMap.values()) {
            messageQueueList.addAll(this.genMessageQueueFromQueueData(queueData, request.getTopic(), topicMessageType, broker));
        }
    }

    private QueryRouteResponse buildRouteResponse(List<MessageQueue> messageQueueList) {
        return QueryRouteResponse.newBuilder()
            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
            .addAllMessageQueues(messageQueueList)
            .build();
    }

    private boolean getFifo(ProxyContext ctx, QueryAssignmentRequest request) {
        boolean fifo = false;
        SubscriptionGroupConfig config = this.messagingProcessor.getSubscriptionGroupConfig(ctx,
            GrpcConverter.getInstance().wrapResourceWithNamespace(request.getGroup()));
        if (config != null && config.isConsumeMessageOrderly()) {
            fifo = true;
        }

        return fifo;
    }

    private List<Assignment> getAssignmentList(boolean fifo, ProxyTopicRouteData proxyTopicRouteData, QueryAssignmentRequest request) {
        List<Assignment> assignments = new ArrayList<>();
        Map<String, Map<Long, Broker>> brokerMap = buildBrokerMap(proxyTopicRouteData.getBrokerDatas());
        for (QueueData queueData : proxyTopicRouteData.getQueueDatas()) {
            if (!PermName.isReadable(queueData.getPerm()) || queueData.getReadQueueNums() <= 0) {
                continue;
            }

            Map<Long, Broker> brokerIdMap = brokerMap.get(queueData.getBrokerName());
            if (brokerIdMap == null) {
                continue;
            }

            addAssignments(assignments, request, brokerIdMap, queueData, fifo);
        }

        return assignments;
    }

    private void addAssignments(List<Assignment> assignments, QueryAssignmentRequest request, Map<Long, Broker> brokerIdMap, QueueData queueData, boolean fifo) {
        Broker broker = brokerIdMap.get(MQConstants.MASTER_ID);
        Permission permission = this.convertToPermission(queueData.getPerm());
        if (!fifo) {
            addAssignment(assignments, request, -1, permission, broker);
            return;
        }

        for (int i = 0; i < queueData.getReadQueueNums(); i++) {
            addAssignment(assignments, request, i, permission, broker);
        }
    }

    private void addAssignment(List<Assignment> assignments, QueryAssignmentRequest request, int id, Permission permission, Broker broker) {
        MessageQueue defaultMessageQueue = MessageQueue.newBuilder()
            .setTopic(request.getTopic())
            .setId(id)
            .setPermission(permission)
            .setBroker(broker)
            .build();

        assignments.add(Assignment.newBuilder()
            .setMessageQueue(defaultMessageQueue)
            .build());
    }

    private QueryAssignmentResponse buildAssignmentResponse(List<Assignment> assignments) {
        if (assignments.isEmpty()) {
            return QueryAssignmentResponse.newBuilder()
                .setStatus(ResponseBuilder.getInstance().buildStatus(Code.FORBIDDEN, "no readable queue"))
                .build();
        }

        return QueryAssignmentResponse.newBuilder()
            .addAllAssignments(assignments)
            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
            .build();
    }

    protected Permission convertToPermission(int perm) {
        boolean isReadable = PermName.isReadable(perm);
        boolean isWriteable = PermName.isWriteable(perm);
        if (isReadable && isWriteable) {
            return Permission.READ_WRITE;
        }
        if (isReadable) {
            return Permission.READ;
        }
        if (isWriteable) {
            return Permission.WRITE;
        }
        return Permission.NONE;
    }

    protected List<org.apache.rocketmq.proxy.common.Address> convertToAddressList(Endpoints endpoints) {

        boolean useEndpointPort = ConfigurationManager.getProxyConfig().isUseEndpointPortFromRequest();

        List<org.apache.rocketmq.proxy.common.Address> addressList = new ArrayList<>();
        for (Address address : endpoints.getAddressesList()) {
            int port = ConfigurationManager.getProxyConfig().getGrpcServerPort();
            if (useEndpointPort) {
                port = address.getPort();
            }

            addressList.add(new org.apache.rocketmq.proxy.common.Address(
                org.apache.rocketmq.proxy.common.Address.AddressScheme.valueOf(endpoints.getScheme().name()),
                HostAndPort.fromParts(address.getHost(), port)));
        }

        return addressList;

    }

    protected Map<String /*brokerName*/, Map<Long /*brokerID*/, Broker>> buildBrokerMap(List<ProxyTopicRouteData.ProxyBrokerData> brokerDataList) {
        Map<String, Map<Long, Broker>> brokerMap = new HashMap<>();

        for (ProxyTopicRouteData.ProxyBrokerData brokerData : brokerDataList) {
            Map<Long, Broker> brokerIdMap = getBrokerIdMap(brokerData);
            brokerMap.put(brokerData.getBrokerName(), brokerIdMap);
        }

        return brokerMap;
    }

    private Map<Long, Broker> getBrokerIdMap(ProxyTopicRouteData.ProxyBrokerData brokerData) {
        String brokerName = brokerData.getBrokerName();
        Map<Long, Broker> brokerIdMap = new HashMap<>();
        for (Map.Entry<Long, List<org.apache.rocketmq.proxy.common.Address>> entry : brokerData.getBrokerAddrs().entrySet()) {
            Long brokerId = entry.getKey();
            List<Address> addressList = new ArrayList<>();
            AddressScheme scheme = addAddress(addressList, entry);

            Broker broker = createBroker(brokerName, brokerId, scheme, addressList);
            brokerIdMap.put(brokerId, broker);
        }

        return brokerIdMap;
    }

    private AddressScheme addAddress(List<Address> addressList, Map.Entry<Long, List<org.apache.rocketmq.proxy.common.Address>> entry) {
        AddressScheme addressScheme = AddressScheme.IPv4;
        for (org.apache.rocketmq.proxy.common.Address address : entry.getValue()) {
            addressScheme = AddressScheme.valueOf(address.getAddressScheme().name());
            addressList.add(Address.newBuilder()
                .setHost(address.getHostAndPort().getHost())
                .setPort(address.getHostAndPort().getPort())
                .build());
        }

        return addressScheme;
    }

    private Broker createBroker(String brokerName, Long brokerId, AddressScheme addressScheme, List<Address> addressList) {
        Endpoints endpoints = Endpoints.newBuilder()
            .setScheme(addressScheme)
            .addAllAddresses(addressList)
            .build();

        return Broker.newBuilder()
            .setName(brokerName)
            .setId(Math.toIntExact(brokerId))
            .setEndpoints(endpoints)
            .build();
    }

    protected List<MessageQueue> genMessageQueueFromQueueData(QueueData queueData, Resource topic, TopicMessageType topicMessageType, Broker broker) {
        List<MessageQueue> messageQueueList = new ArrayList<>();

        int r = 0;
        int w = 0;
        int rw = 0;
        if (PermName.isWriteable(queueData.getPerm()) && PermName.isReadable(queueData.getPerm())) {
            rw = Math.min(queueData.getWriteQueueNums(), queueData.getReadQueueNums());
            r = queueData.getReadQueueNums() - rw;
            w = queueData.getWriteQueueNums() - rw;
        } else if (PermName.isWriteable(queueData.getPerm())) {
            w = queueData.getWriteQueueNums();
        } else if (PermName.isReadable(queueData.getPerm())) {
            r = queueData.getReadQueueNums();
        }

        // r here means readOnly queue nums, w means writeOnly queue nums, while rw means both readable and writable queue nums.
        addReadOnlyQueue(messageQueueList, topic, topicMessageType, broker, r);
        addWriteOnlyQueue(messageQueueList, topic, topicMessageType, broker, w);
        addReadWriteQueue(messageQueueList, topic, topicMessageType, broker, rw);

        return messageQueueList;
    }

    private void addReadOnlyQueue(List<MessageQueue> messageQueueList, Resource topic, TopicMessageType topicMessageType, Broker broker, int num) {
        int counter = 0;
        for (int i = 0; i < num; i++) {
            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
                .setId(counter++)
                .setPermission(Permission.READ)
                .addAllAcceptMessageTypes(parseTopicMessageType(topicMessageType))
                .build();
            messageQueueList.add(messageQueue);
        }

    }

    private void addWriteOnlyQueue(List<MessageQueue> messageQueueList, Resource topic, TopicMessageType topicMessageType, Broker broker, int num) {
        int counter = 0;
        for (int i = 0; i < num; i++) {
            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
                .setId(counter++)
                .setPermission(Permission.WRITE)
                .addAllAcceptMessageTypes(parseTopicMessageType(topicMessageType))
                .build();
            messageQueueList.add(messageQueue);
        }
    }

    private void addReadWriteQueue(List<MessageQueue> messageQueueList, Resource topic, TopicMessageType topicMessageType, Broker broker, int num) {
        int counter = 0;
        for (int i = 0; i < num; i++) {
            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
                .setId(counter++)
                .setPermission(Permission.READ_WRITE)
                .addAllAcceptMessageTypes(parseTopicMessageType(topicMessageType))
                .build();
            messageQueueList.add(messageQueue);
        }
    }

    private List<MessageType> parseTopicMessageType(TopicMessageType topicMessageType) {
        switch (topicMessageType) {
            case NORMAL:
                return Collections.singletonList(MessageType.NORMAL);
            case FIFO:
                return Collections.singletonList(MessageType.FIFO);
            case TRANSACTION:
                return Collections.singletonList(MessageType.TRANSACTION);
            case DELAY:
                return Collections.singletonList(MessageType.DELAY);
            case MIXED:
                return Arrays.asList(MessageType.NORMAL, MessageType.FIFO, MessageType.DELAY, MessageType.TRANSACTION);
            default:
                return Collections.singletonList(MessageType.MESSAGE_TYPE_UNSPECIFIED);
        }
    }
}
