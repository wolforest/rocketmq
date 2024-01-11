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
package org.apache.rocketmq.remoting.client;

import com.alibaba.fastjson.JSON;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.rocketmq.common.app.config.PlainAccessConfig;
import org.apache.rocketmq.common.domain.constant.FIleReadaheadMode;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.domain.constant.MQVersion;
import org.apache.rocketmq.common.domain.message.MessageDecoder;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.domain.message.MessageQueue;
import org.apache.rocketmq.common.domain.message.MessageRequestMode;
import org.apache.rocketmq.common.domain.topic.TopicConfig;
import org.apache.rocketmq.common.domain.topic.TopicValidator;
import org.apache.rocketmq.common.lang.BoundaryType;
import org.apache.rocketmq.common.lang.Pair;
import org.apache.rocketmq.common.lang.attribute.AttributeParser;
import org.apache.rocketmq.common.utils.BeanUtils;
import org.apache.rocketmq.common.utils.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.common.HeartbeatV2Result;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo;
import org.apache.rocketmq.remoting.protocol.body.BrokerStatsData;
import org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody;
import org.apache.rocketmq.remoting.protocol.body.ClusterAclVersionInfo;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumeStatsList;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.body.EpochEntryCache;
import org.apache.rocketmq.remoting.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.remoting.protocol.body.KVTable;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody;
import org.apache.rocketmq.remoting.protocol.body.ProducerConnection;
import org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo;
import org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody;
import org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody;
import org.apache.rocketmq.remoting.protocol.body.QueryCorrectionOffsetBody;
import org.apache.rocketmq.remoting.protocol.body.QuerySubscriptionResponseBody;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.header.AddBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CloneGroupOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateAccessConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteAccessConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteSubscriptionGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetAllProducerInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetBrokerAclConfigResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsInBrokerHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerConnectionListRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetProducerConnectionListRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetSubscriptionGroupConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicStatsInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumeQueueRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumeTimeSpanRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryCorrectionOffsetHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QuerySubscriptionByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicsByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.RemoveBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetMasterFlushOffsetHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResumeCheckHalfMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateGlobalWhiteAddrsConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateGroupForbiddenRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ViewBrokerStatsDataRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.admin.CleanControllerBrokerDataRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVConfigResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVListByNamespaceRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.PutKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.WipeWritePermOfBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.subscription.GroupForbidden;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.rpchook.DynamicalExtFieldRPCHook;
import org.apache.rocketmq.remoting.rpchook.StreamTypeRPCHook;

import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SUCCESS;

public class RPCClient {
    protected static final Logger LOG = LoggerFactory.getLogger(RPCClient.class);

    static {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
    }

    protected final RemotingClient remotingClient;
    protected long timeoutMillis = 5000;

    public RPCClient() {
        this(null);
    }

    public RPCClient(RPCHook hook) {
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        this.remotingClient = new NettyRemotingClient(nettyClientConfig, null);

        // Inject stream rpc hook first to make reserve field signature
        this.remotingClient.registerRPCHook(new StreamTypeRPCHook());
        registerRPCHook(hook);
        this.remotingClient.registerRPCHook(new DynamicalExtFieldRPCHook());
    }

    public void setTimeoutMillis(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    private void registerRPCHook(RPCHook hook) {
        if (null == hook) {
            return;
        }

        this.remotingClient.registerRPCHook(hook);
    }

    public List<String> getNameServerAddressList() {
        return this.remotingClient.getNameServerAddressList();
    }

    public void updateNameServerAddressList(String addrs) {
        String[] addrArray = addrs.split(";");
        List<String> list = Arrays.asList(addrArray);
        this.remotingClient.updateNameServerAddressList(list);
    }

    public void start() {
        this.remotingClient.start();
    }

    public void shutdown() {
        this.remotingClient.shutdown();
    }

    public void createSubscriptionGroup(String addr, SubscriptionGroupConfig config) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, null);

        byte[] body = RemotingSerializable.encode(config);
        request.setBody(body);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark());

    }

    public void createTopic(String addr, TopicConfig topicConfig) throws RemotingException, InterruptedException {
        Validators.checkTopicConfig(topicConfig);

        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
        requestHeader.setTopic(topicConfig.getTopicName());
        requestHeader.setDefaultTopic(topicConfig.getTopicName());
        requestHeader.setReadQueueNums(topicConfig.getReadQueueNums());
        requestHeader.setWriteQueueNums(topicConfig.getWriteQueueNums());
        requestHeader.setPerm(topicConfig.getPerm());
        requestHeader.setTopicFilterType(topicConfig.getTopicFilterType().name());
        requestHeader.setTopicSysFlag(topicConfig.getTopicSysFlag());
        requestHeader.setOrder(topicConfig.isOrder());
        requestHeader.setAttributes(AttributeParser.parseToString(topicConfig.getAttributes()));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public void createPlainAccessConfig(String addr, PlainAccessConfig plainAccessConfig) throws RemotingException, InterruptedException {
        CreateAccessConfigRequestHeader requestHeader = new CreateAccessConfigRequestHeader();
        requestHeader.setAccessKey(plainAccessConfig.getAccessKey());
        requestHeader.setSecretKey(plainAccessConfig.getSecretKey());
        requestHeader.setAdmin(plainAccessConfig.isAdmin());
        requestHeader.setDefaultGroupPerm(plainAccessConfig.getDefaultGroupPerm());
        requestHeader.setDefaultTopicPerm(plainAccessConfig.getDefaultTopicPerm());
        requestHeader.setWhiteRemoteAddress(plainAccessConfig.getWhiteRemoteAddress());
        requestHeader.setTopicPerms(org.apache.rocketmq.common.utils.StringUtils.join(plainAccessConfig.getTopicPerms(), ","));
        requestHeader.setGroupPerms(org.apache.rocketmq.common.utils.StringUtils.join(plainAccessConfig.getGroupPerms(), ","));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_ACL_CONFIG, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public void deleteAccessConfig(String addr, String accessKey)
        throws RemotingException, InterruptedException {
        DeleteAccessConfigRequestHeader requestHeader = new DeleteAccessConfigRequestHeader();
        requestHeader.setAccessKey(accessKey);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_ACL_CONFIG, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public void updateGlobalWhiteAddrsConfig(String addr, String globalWhiteAddrs, String aclFileFullPath,
        long timeoutMillis) throws InterruptedException, RemotingException {
        UpdateGlobalWhiteAddrsConfigRequestHeader requestHeader = new UpdateGlobalWhiteAddrsConfigRequestHeader();
        requestHeader.setGlobalWhiteAddrs(globalWhiteAddrs);
        requestHeader.setAclFileFullPath(aclFileFullPath);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_GLOBAL_WHITE_ADDRS_CONFIG, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public ClusterAclVersionInfo getBrokerClusterAclInfo(String addr) throws InterruptedException, RemotingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_ACL_INFO, null);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            GetBrokerAclConfigResponseHeader responseHeader = (GetBrokerAclConfigResponseHeader) response.decodeCommandCustomHeader(GetBrokerAclConfigResponseHeader.class);

            ClusterAclVersionInfo clusterAclVersionInfo = new ClusterAclVersionInfo();
            clusterAclVersionInfo.setClusterName(responseHeader.getClusterName());
            clusterAclVersionInfo.setBrokerName(responseHeader.getBrokerName());
            clusterAclVersionInfo.setBrokerAddr(responseHeader.getBrokerAddr());
            clusterAclVersionInfo.setAclConfigDataVersion(DataVersion.fromJson(responseHeader.getVersion(), DataVersion.class));
            Map<String, Object> dataVersionMap = JSON.parseObject(responseHeader.getAllAclFileVersion()).getInnerMap();
            Map<String, DataVersion> allAclConfigDataVersion = new HashMap<>(dataVersionMap.size(), 1);
            for (Map.Entry<String, Object> entry : dataVersionMap.entrySet()) {
                allAclConfigDataVersion.put(entry.getKey(), DataVersion.fromJson(JSON.toJSONString(entry.getValue()), DataVersion.class));
            }
            clusterAclVersionInfo.setAllAclConfigDataVersion(allAclConfigDataVersion);
            return clusterAclVersionInfo;
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);

    }

    public MessageExt viewMessage(String addr, long offset) throws RemotingException, InterruptedException {
        ViewMessageRequestHeader requestHeader = new ViewMessageRequestHeader();
        requestHeader.setOffset(offset);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(response.getBody());
            //If namespace not null , reset Topic without namespace.
            return MessageDecoder.clientDecode(byteBuffer, true);
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    @Deprecated
    public long searchOffset(String addr, String topic, int queueId, long timestamp) throws RemotingException, InterruptedException {
        SearchOffsetRequestHeader requestHeader = new SearchOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setTimestamp(timestamp);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            SearchOffsetResponseHeader responseHeader = (SearchOffsetResponseHeader) response.decodeCommandCustomHeader(SearchOffsetResponseHeader.class);
            return responseHeader.getOffset();
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public long searchOffset(String addr, MessageQueue messageQueue, long timestamp) throws RemotingException, InterruptedException {
        // default return lower boundary offset when there are more than one offsets.
        return searchOffset(addr, messageQueue, timestamp, BoundaryType.LOWER);
    }

    public long searchOffset(String addr, MessageQueue messageQueue, long timestamp, BoundaryType boundaryType) throws RemotingException, InterruptedException {
        SearchOffsetRequestHeader requestHeader = new SearchOffsetRequestHeader();
        requestHeader.setTopic(messageQueue.getTopic());
        requestHeader.setQueueId(messageQueue.getQueueId());
        requestHeader.setBname(messageQueue.getBrokerName());
        requestHeader.setTimestamp(timestamp);
        requestHeader.setBoundaryType(boundaryType);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            SearchOffsetResponseHeader responseHeader = (SearchOffsetResponseHeader) response.decodeCommandCustomHeader(SearchOffsetResponseHeader.class);
            return responseHeader.getOffset();
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public long getMaxOffset(String addr, MessageQueue messageQueue)
        throws RemotingException, InterruptedException {
        GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
        requestHeader.setTopic(messageQueue.getTopic());
        requestHeader.setQueueId(messageQueue.getQueueId());
        requestHeader.setBname(messageQueue.getBrokerName());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            GetMaxOffsetResponseHeader responseHeader = (GetMaxOffsetResponseHeader) response.decodeCommandCustomHeader(GetMaxOffsetResponseHeader.class);
            return responseHeader.getOffset();
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public List<String> getConsumerIdListByGroup(String addr, String consumerGroup) throws RemotingException, InterruptedException {
        GetConsumerListByGroupRequestHeader requestHeader = new GetConsumerListByGroupRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            if (response.getBody() != null) {
                GetConsumerListByGroupResponseBody body = GetConsumerListByGroupResponseBody.decode(response.getBody(), GetConsumerListByGroupResponseBody.class);
                return body.getConsumerIdList();
            }
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public long getMinOffset(String addr, MessageQueue messageQueue) throws RemotingException, InterruptedException {
        GetMinOffsetRequestHeader requestHeader = new GetMinOffsetRequestHeader();
        requestHeader.setTopic(messageQueue.getTopic());
        requestHeader.setQueueId(messageQueue.getQueueId());
        requestHeader.setBname(messageQueue.getBrokerName());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MIN_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            GetMinOffsetResponseHeader responseHeader = (GetMinOffsetResponseHeader) response.decodeCommandCustomHeader(GetMinOffsetResponseHeader.class);
            return responseHeader.getOffset();
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public long getEarliestMsgStoreTime(String addr, MessageQueue mq) throws RemotingException, InterruptedException {
        GetEarliestMsgStoretimeRequestHeader requestHeader = new GetEarliestMsgStoretimeRequestHeader();
        requestHeader.setTopic(mq.getTopic());
        requestHeader.setQueueId(mq.getQueueId());
        requestHeader.setBname(mq.getBrokerName());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_EARLIEST_MSG_STORETIME, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            GetEarliestMsgStoretimeResponseHeader responseHeader = (GetEarliestMsgStoretimeResponseHeader) response.decodeCommandCustomHeader(GetEarliestMsgStoretimeResponseHeader.class);
            return responseHeader.getTimestamp();
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public long queryConsumerOffset(String addr, QueryConsumerOffsetRequestHeader requestHeader) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                QueryConsumerOffsetResponseHeader responseHeader = (QueryConsumerOffsetResponseHeader) response.decodeCommandCustomHeader(QueryConsumerOffsetResponseHeader.class);
                return responseHeader.getOffset();
            }
            case ResponseCode.QUERY_NOT_FOUND: {
                throw new RemotingException(response.getCode(), response.getRemark(), addr);
            }
            default:
                break;
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public void updateConsumerOffset(String addr, UpdateConsumerOffsetRequestHeader requestHeader) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public void updateConsumerOffsetOneway(String addr, UpdateConsumerOffsetRequestHeader requestHeader) throws RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);

        this.remotingClient.invokeOneway(addr, request, timeoutMillis);
    }

    public int sendHeartbeat(String addr, HeartbeatData heartbeatData) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setLanguage(LanguageCode.JAVA);
        request.setBody(heartbeatData.encode());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            return response.getVersion();
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public HeartbeatV2Result sendHeartbeatV2(String addr, HeartbeatData heartbeatData) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setLanguage(LanguageCode.JAVA);
        request.setBody(heartbeatData.encode());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            if (response.getExtFields() != null) {
                return new HeartbeatV2Result(response.getVersion(), Boolean.parseBoolean(response.getExtFields().get(MQConstants.IS_SUB_CHANGE)), Boolean.parseBoolean(response.getExtFields().get(MQConstants.IS_SUPPORT_HEART_BEAT_V2)));
            }
            return new HeartbeatV2Result(response.getVersion(), false, false);
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public void unregisterClient(String addr, String clientID, String producerGroup, String consumerGroup) throws RemotingException, InterruptedException {
        UnregisterClientRequestHeader requestHeader = new UnregisterClientRequestHeader();
        requestHeader.setClientID(clientID);
        requestHeader.setProducerGroup(producerGroup);
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public void endTransactionOneway(String addr, EndTransactionRequestHeader requestHeader, String remark) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, requestHeader);

        request.setRemark(remark);
        this.remotingClient.invokeOneway(addr, request, timeoutMillis);
    }

    public void queryMessage(String addr, QueryMessageRequestHeader requestHeader, long timeoutMillis, InvokeCallback invokeCallback, Boolean isUniqueKey) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, requestHeader);
        request.addExtField(MQConstants.UNIQUE_MSG_QUERY_FLAG, isUniqueKey.toString());
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, invokeCallback);
    }

    public boolean registerClient(String addr, HeartbeatData heartbeat) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);

        request.setBody(heartbeat.encode());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        return response.getCode() == ResponseCode.SUCCESS;
    }

    public void consumerSendMessageBack(String addr, String brokerName, MessageExt msg, String consumerGroup, int delayLevel, long timeoutMillis, int maxConsumeRetryTimes) throws RemotingException, InterruptedException {
        ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);

        requestHeader.setGroup(consumerGroup);
        requestHeader.setOriginTopic(msg.getTopic());
        requestHeader.setOffset(msg.getCommitLogOffset());
        requestHeader.setDelayLevel(delayLevel);
        requestHeader.setOriginMsgId(msg.getMsgId());
        requestHeader.setMaxReconsumeTimes(maxConsumeRetryTimes);
        requestHeader.setBname(brokerName);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public Set<MessageQueue> lockBatchMQ(String addr, LockBatchRequestBody requestBody) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);

        request.setBody(requestBody.encode());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        if (response.getCode() == SUCCESS) {
            LockBatchResponseBody responseBody = LockBatchResponseBody.decode(response.getBody(), LockBatchResponseBody.class);
            return responseBody.getLockOKMQSet();
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public void unlockBatchMQ(String addr, UnlockBatchRequestBody requestBody, long timeoutMillis, boolean oneway) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, null);
        request.setBody(requestBody.encode());

        if (oneway) {
            this.remotingClient.invokeOneway(addr, request, timeoutMillis);
            return;
        }

        RemotingCommand response = this.remotingClient .invokeSync(addr, request, timeoutMillis);
        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public TopicStatsTable getTopicStatsInfo(String addr, String topic) throws InterruptedException, RemotingException {
        GetTopicStatsInfoRequestHeader requestHeader = new GetTopicStatsInfoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_STATS_INFO, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        if (response.getCode() == SUCCESS) {
            return TopicStatsTable.decode(response.getBody(), TopicStatsTable.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public ConsumeStats getConsumeStats(String addr, String consumerGroup) throws InterruptedException, RemotingException {
        return getConsumeStats(addr, consumerGroup, null);
    }

    public ConsumeStats getConsumeStats(String addr, String consumerGroup, String topic) throws InterruptedException, RemotingException {
        GetConsumeStatsRequestHeader requestHeader = new GetConsumeStatsRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUME_STATS, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        if (response.getCode() == SUCCESS) {
            return ConsumeStats.decode(response.getBody(), ConsumeStats.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public ProducerConnection getProducerConnectionList(String addr, String producerGroup) throws InterruptedException, RemotingException {
        GetProducerConnectionListRequestHeader requestHeader = new GetProducerConnectionListRequestHeader();
        requestHeader.setProducerGroup(producerGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_PRODUCER_CONNECTION_LIST, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        if (response.getCode() == SUCCESS) {
            return ProducerConnection.decode(response.getBody(), ProducerConnection.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public ProducerTableInfo getAllProducerInfo(String addr) throws InterruptedException, RemotingException {
        GetAllProducerInfoRequestHeader requestHeader = new GetAllProducerInfoRequestHeader();

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_PRODUCER_INFO, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        if (response.getCode() == SUCCESS) {
            return ProducerTableInfo.decode(response.getBody(), ProducerTableInfo.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public ConsumerConnection getConsumerConnectionList(String addr, String consumerGroup) throws InterruptedException, RemotingException {
        GetConsumerConnectionListRequestHeader requestHeader = new GetConsumerConnectionListRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_CONNECTION_LIST, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        if (response.getCode() == SUCCESS) {
            return ConsumerConnection.decode(response.getBody(), ConsumerConnection.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public KVTable getBrokerRuntimeInfo(String addr) throws InterruptedException, RemotingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_RUNTIME_INFO, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        if (response.getCode() == SUCCESS) {
            return KVTable.decode(response.getBody(), KVTable.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public void addBroker(String addr, String brokerConfigPath) throws InterruptedException, RemotingException {
        AddBrokerRequestHeader requestHeader = new AddBrokerRequestHeader();
        requestHeader.setConfigPath(brokerConfigPath);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ADD_BROKER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public void removeBroker(String addr, String clusterName, String brokerName, long brokerId) throws InterruptedException, RemotingException {
        RemoveBrokerRequestHeader requestHeader = new RemoveBrokerRequestHeader();
        requestHeader.setBrokerClusterName(clusterName);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setBrokerId(brokerId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REMOVE_BROKER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public void updateBrokerConfig(String addr, Properties properties) throws InterruptedException, RemotingException, UnsupportedEncodingException {
        Validators.checkBrokerConfig(properties);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);

        String str = BeanUtils.properties2String(properties);
        if (str.length() <= 0) {
            return;
        }

        request.setBody(str.getBytes(MQConstants.DEFAULT_CHARSET));
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public Properties getBrokerConfig(String addr) throws InterruptedException, RemotingException, UnsupportedEncodingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONFIG, null);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            return BeanUtils.string2Properties(new String(response.getBody(), MQConstants.DEFAULT_CHARSET));
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public void updateColdDataFlowCtrGroupConfig(String addr, Properties properties) throws InterruptedException, RemotingException, UnsupportedEncodingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_COLD_DATA_FLOW_CTR_CONFIG, null);
        String str = BeanUtils.properties2String(properties);
        if (str.length() <= 0) {
            return;
        }

        request.setBody(str.getBytes(MQConstants.DEFAULT_CHARSET));
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        if (response.getCode() == SUCCESS) {
            return;
        }
        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public void removeColdDataFlowCtrGroupConfig(String addr, String consumerGroup) throws InterruptedException, RemotingException, UnsupportedEncodingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REMOVE_COLD_DATA_FLOW_CTR_CONFIG, null);
        if (!StringUtils.isBlank(consumerGroup)) {
            return;
        }

        request.setBody(consumerGroup.getBytes(MQConstants.DEFAULT_CHARSET));
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        if (response.getCode() == SUCCESS) {
            return;
        }
        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public String getColdDataFlowCtrInfo(String addr) throws InterruptedException, RemotingException, UnsupportedEncodingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_COLD_DATA_FLOW_CTR_INFO, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() != SUCCESS) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        if (null != response.getBody() && response.getBody().length > 0) {
            return new String(response.getBody(), MQConstants.DEFAULT_CHARSET);
        }
        return null;
    }

    public String setCommitLogReadAheadMode(String addr, String mode) throws InterruptedException, RemotingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SET_COMMITLOG_READ_MODE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put(FIleReadaheadMode.READ_AHEAD_MODE, mode);
        request.setExtFields(extFields);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() != SUCCESS) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        if (null != response.getRemark() && response.getRemark().length() > 0) {
            return response.getRemark();
        }
        return null;
    }

    /**
     * get cluster info(broker info) from nameServer
     * @param addr namesrv addr
     * @return ClusterInfor
     * @throws InterruptedException e
     * @throws RemotingException e
     */
    public ClusterInfo getBrokerClusterInfo(String addr) throws InterruptedException, RemotingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            return ClusterInfo.decode(response.getBody(), ClusterInfo.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public TopicRouteData getDefaultTopicRouteInfoFromNameServer(long timeoutMillis) throws RemotingException, InterruptedException {
        return getTopicRouteInfoFromNameServer(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC, timeoutMillis, false);
    }

    public TopicRouteData getTopicRouteInfoFromNameServer(String topic) throws RemotingException, InterruptedException {
        return getTopicRouteInfoFromNameServer(topic, timeoutMillis, true);
    }

    public TopicRouteData getTopicRouteInfoFromNameServer(String topic, long timeoutMillis, boolean allowTopicNotExist) throws RemotingException, InterruptedException {
        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.TOPIC_NOT_EXIST: {
                if (allowTopicNotExist) {
                    LOG.warn("get Topic [{}] RouteInfoFromNameServer is not exist value", topic);
                }
                break;
            }
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return TopicRouteData.decode(body, TopicRouteData.class);
                }
            }
            default:
                break;
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public TopicList getTopicListFromNameServer(long timeoutMillis) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, null);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        if (response.getCode() != SUCCESS) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        byte[] body = response.getBody();
        if (body != null) {
            return TopicList.decode(body, TopicList.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public int wipeWritePermOfBroker(String namesrvAddr, String brokerName) throws InterruptedException, RemotingException {
        WipeWritePermOfBrokerRequestHeader requestHeader = new WipeWritePermOfBrokerRequestHeader();
        requestHeader.setBrokerName(brokerName);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.WIPE_WRITE_PERM_OF_BROKER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            WipeWritePermOfBrokerResponseHeader responseHeader = (WipeWritePermOfBrokerResponseHeader) response.decodeCommandCustomHeader(WipeWritePermOfBrokerResponseHeader.class);
            return responseHeader.getWipeTopicCount();
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public int addWritePermOfBroker(String nameSrvAddr, String brokerName) throws InterruptedException, RemotingException {
        AddWritePermOfBrokerRequestHeader requestHeader = new AddWritePermOfBrokerRequestHeader();
        requestHeader.setBrokerName(brokerName);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ADD_WRITE_PERM_OF_BROKER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(nameSrvAddr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            AddWritePermOfBrokerResponseHeader responseHeader = (AddWritePermOfBrokerResponseHeader) response.decodeCommandCustomHeader(AddWritePermOfBrokerResponseHeader.class);
            return responseHeader.getAddTopicCount();
        }
        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public void deleteTopicInBroker(String addr, String topic) throws InterruptedException, RemotingException {
        DeleteTopicRequestHeader requestHeader = new DeleteTopicRequestHeader();
        requestHeader.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_BROKER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public void deleteTopicInNameServer(String addr, String topic) throws InterruptedException, RemotingException {
        DeleteTopicFromNamesrvRequestHeader requestHeader = new DeleteTopicFromNamesrvRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_NAMESRV, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

        assert response != null;
        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public void deleteTopicInNameServer(String addr, String clusterName, String topic) throws InterruptedException, RemotingException {
        DeleteTopicFromNamesrvRequestHeader requestHeader = new DeleteTopicFromNamesrvRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setClusterName(clusterName);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_NAMESRV, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public void deleteSubscriptionGroup(String addr, String groupName, boolean removeOffset) throws InterruptedException, RemotingException {
        DeleteSubscriptionGroupRequestHeader requestHeader = new DeleteSubscriptionGroupRequestHeader();
        requestHeader.setGroupName(groupName);
        requestHeader.setCleanOffset(removeOffset);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_SUBSCRIPTIONGROUP, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public String getKVConfigValue(String namespace, String key) throws RemotingException, InterruptedException {
        GetKVConfigRequestHeader requestHeader = new GetKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KV_CONFIG, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;

        if (response.getCode() == SUCCESS) {
            GetKVConfigResponseHeader responseHeader = (GetKVConfigResponseHeader) response.decodeCommandCustomHeader(GetKVConfigResponseHeader.class);
            return responseHeader.getValue();
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public void putKVConfigValue(String namespace, String key, String value) throws RemotingException, InterruptedException {
        PutKVConfigRequestHeader requestHeader = new PutKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);
        requestHeader.setValue(value);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PUT_KV_CONFIG, requestHeader);
        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList == null) {
            return;
        }

        RemotingCommand errResponse = null;
        for (String namesrvAddr : nameServerAddressList) {
            RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
            assert response != null;
            if (response.getCode() != SUCCESS) {
                errResponse = response;
            }
        }

        if (errResponse != null) {
            throw new RemotingException(errResponse.getCode(), errResponse.getRemark());
        }
    }

    public void deleteKVConfigValue(String namespace, String key) throws RemotingException, InterruptedException {
        DeleteKVConfigRequestHeader requestHeader = new DeleteKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_KV_CONFIG, requestHeader);

        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList == null) {
            return;
        }

        RemotingCommand errResponse = null;
        for (String namesrvAddr : nameServerAddressList) {
            RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
            assert response != null;
            if (response.getCode() != SUCCESS) {
                errResponse = response;
            }
        }

        if (errResponse != null) {
            throw new RemotingException(errResponse.getCode(), errResponse.getRemark());
        }
    }

    public KVTable getKVListByNamespace(String namespace) throws RemotingException, InterruptedException {
        GetKVListByNamespaceRequestHeader requestHeader = new GetKVListByNamespaceRequestHeader();
        requestHeader.setNamespace(namespace);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KVLIST_BY_NAMESPACE, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;

        if (response.getCode() == SUCCESS) {
            return KVTable.decode(response.getBody(), KVTable.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public Map<MessageQueue, Long> invokeBrokerToResetOffset(String addr, String topic, String group, long timestamp, boolean isForce) throws RemotingException, InterruptedException {
        return invokeBrokerToResetOffset(addr, topic, group, timestamp, isForce, timeoutMillis, false);
    }

    public Map<MessageQueue, Long> invokeBrokerToResetOffset(String addr, String topic, String group, long timestamp, int queueId, Long offset) throws RemotingException, InterruptedException {
        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setQueueId(queueId);
        requestHeader.setTimestamp(timestamp);
        requestHeader.setOffset(offset);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, requestHeader);
        RemotingCommand response = remotingClient.invokeSync(addr, request, timeoutMillis);

        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (null != response.getBody()) {
                    return ResetOffsetBody.decode(response.getBody(), ResetOffsetBody.class).getOffsetTable();
                }
                break;
            }
            case ResponseCode.TOPIC_NOT_EXIST:
            case ResponseCode.SUBSCRIPTION_NOT_EXIST:
            case ResponseCode.SYSTEM_ERROR:
                LOG.warn("Invoke broker to reset offset error code={}, remark={}", response.getCode(), response.getRemark());
                break;
            default:
                break;
        }
        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public Map<MessageQueue, Long> invokeBrokerToResetOffset(String addr, String topic, String group, long timestamp, boolean isForce, long timeoutMillis, boolean isC) throws RemotingException, InterruptedException {
        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setTimestamp(timestamp);
        requestHeader.setForce(isForce);
        // offset is -1 means offset is null
        requestHeader.setOffset(-1L);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, requestHeader);
        if (isC) {
            request.setLanguage(LanguageCode.CPP);
        }
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() != SUCCESS) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        if (response.getBody() != null) {
            ResetOffsetBody body = ResetOffsetBody.decode(response.getBody(), ResetOffsetBody.class);
            return body.getOffsetTable();
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public Map<String, Map<MessageQueue, Long>> invokeBrokerToGetConsumerStatus(String addr, String topic, String group, String clientAddr) throws RemotingException, InterruptedException {
        GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setClientAddr(clientAddr);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() != SUCCESS) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        if (response.getBody() != null) {
            GetConsumerStatusBody body = GetConsumerStatusBody.decode(response.getBody(), GetConsumerStatusBody.class);
            return body.getConsumerTable();
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public GroupList queryTopicConsumeByWho(String addr, String topic) throws InterruptedException, RemotingException {
        QueryTopicConsumeByWhoRequestHeader requestHeader = new QueryTopicConsumeByWhoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPIC_CONSUME_BY_WHO, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

        if (response.getCode() == SUCCESS) {
            return GroupList.decode(response.getBody(), GroupList.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public TopicList queryTopicsByConsumer(String addr, String group) throws InterruptedException, RemotingException {
        QueryTopicsByConsumerRequestHeader requestHeader = new QueryTopicsByConsumerRequestHeader();
        requestHeader.setGroup(group);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPICS_BY_CONSUMER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

        if (response.getCode() == SUCCESS) {
            return TopicList.decode(response.getBody(), TopicList.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public SubscriptionData querySubscriptionByConsumer(String addr, String group, String topic) throws InterruptedException, RemotingException {
        QuerySubscriptionByConsumerRequestHeader requestHeader = new QuerySubscriptionByConsumerRequestHeader();
        requestHeader.setGroup(group);
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_SUBSCRIPTION_BY_CONSUMER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

        if (response.getCode() == SUCCESS) {
            QuerySubscriptionResponseBody subscriptionResponseBody = QuerySubscriptionResponseBody.decode(response.getBody(), QuerySubscriptionResponseBody.class);
            return subscriptionResponseBody.getSubscriptionData();
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public List<QueueTimeSpan> queryConsumeTimeSpan(String addr, String topic, String group) throws InterruptedException, RemotingException {
        QueryConsumeTimeSpanRequestHeader requestHeader = new QueryConsumeTimeSpanRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_TIME_SPAN, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

        if (response.getCode() == SUCCESS) {
            QueryConsumeTimeSpanBody consumeTimeSpanBody = GroupList.decode(response.getBody(), QueryConsumeTimeSpanBody.class);
            return consumeTimeSpanBody.getConsumeTimeSpanSet();
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public TopicList getTopicsByCluster(String cluster) throws RemotingException, InterruptedException {
        GetTopicsByClusterRequestHeader requestHeader = new GetTopicsByClusterRequestHeader();
        requestHeader.setCluster(cluster);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPICS_BY_CLUSTER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;

        if (response.getCode() != SUCCESS) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        byte[] body = response.getBody();
        if (body != null) {
            return TopicList.decode(body, TopicList.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public TopicList getSystemTopicList(long timeoutMillis) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS, null);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;

        if (response.getCode() != SUCCESS) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        byte[] body = response.getBody();
        if (body == null) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
        if (topicList.getTopicList() != null && !topicList.getTopicList().isEmpty() && !StringUtils.isBlank(topicList.getBrokerAddr())) {
            TopicList tmp = getSystemTopicListFromBroker(topicList.getBrokerAddr());
            if (tmp.getTopicList() != null && !tmp.getTopicList().isEmpty()) {
                topicList.getTopicList().addAll(tmp.getTopicList());
            }
        }
        return topicList;
    }

    public TopicList getSystemTopicListFromBroker(String addr) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() != SUCCESS) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        byte[] body = response.getBody();
        if (body != null) {
            return TopicList.decode(body, TopicList.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public boolean cleanExpiredConsumeQueue(String addr) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        if (response.getCode() == SUCCESS) {
            return true;
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public boolean deleteExpiredCommitLog(String addr) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_EXPIRED_COMMITLOG, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        if (response.getCode() == SUCCESS) {
            return true;
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public boolean cleanUnusedTopicByAddr(String addr) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_UNUSED_TOPIC, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        if (response.getCode() == SUCCESS) {
            return true;
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public ConsumerRunningInfo getConsumerRunningInfo(String addr, String consumerGroup, String clientId, boolean jstack) throws RemotingException, InterruptedException {
        GetConsumerRunningInfoRequestHeader requestHeader = new GetConsumerRunningInfoRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setClientId(clientId);
        requestHeader.setJstackEnable(jstack);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() != SUCCESS) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        byte[] body = response.getBody();
        if (body != null) {
            return ConsumerRunningInfo.decode(body, ConsumerRunningInfo.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public ConsumeMessageDirectlyResult consumeMessageDirectly(String addr, String consumerGroup, String clientId, String topic, String msgId) throws RemotingException, InterruptedException {
        ConsumeMessageDirectlyResultRequestHeader requestHeader = new ConsumeMessageDirectlyResultRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setClientId(clientId);
        requestHeader.setMsgId(msgId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUME_MESSAGE_DIRECTLY, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() != SUCCESS) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        byte[] body = response.getBody();
        if (body != null) {
            return ConsumeMessageDirectlyResult.decode(body, ConsumeMessageDirectlyResult.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public Map<Integer, Long> queryCorrectionOffset(String addr, String topic, String group, Set<String> filterGroup) throws RemotingException, InterruptedException {
        QueryCorrectionOffsetHeader requestHeader = new QueryCorrectionOffsetHeader();
        requestHeader.setCompareGroup(group);
        requestHeader.setTopic(topic);

        if (filterGroup != null) {
            StringBuilder sb = new StringBuilder();
            String split = "";
            for (String s : filterGroup) {
                sb.append(split).append(s);
                split = ",";
            }
            requestHeader.setFilterGroups(sb.toString());
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CORRECTION_OFFSET, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() != SUCCESS) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        if (response.getBody() != null) {
            QueryCorrectionOffsetBody body = QueryCorrectionOffsetBody.decode(response.getBody(), QueryCorrectionOffsetBody.class);
            return body.getCorrectionOffsets();
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public TopicList getUnitTopicList(boolean containRetry) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_UNIT_TOPIC_LIST, null);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;

        if (response.getCode() != SUCCESS) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        byte[] body = response.getBody();
        if (body == null) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
        if (!containRetry) {
            topicList.getTopicList().removeIf(topic -> topic.startsWith(MQConstants.RETRY_GROUP_TOPIC_PREFIX));
        }

        return topicList;
    }

    public TopicList getHasUnitSubTopicList(boolean containRetry) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST, null);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;

        if (response.getCode() != SUCCESS) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        byte[] body = response.getBody();
        if (body == null) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
        if (!containRetry) {
            topicList.getTopicList().removeIf(topic -> topic.startsWith(MQConstants.RETRY_GROUP_TOPIC_PREFIX));
        }
        return topicList;
    }

    public TopicList getHasUnitSubUnUnitTopicList(boolean containRetry) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST, null);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;

        if (response.getCode() != SUCCESS) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        byte[] body = response.getBody();
        if (body == null) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
        if (!containRetry) {
            topicList.getTopicList().removeIf(topic -> topic.startsWith(MQConstants.RETRY_GROUP_TOPIC_PREFIX));
        }
        return topicList;
    }

    public void cloneGroupOffset(String addr, String srcGroup, String destGroup, String topic, boolean isOffline) throws RemotingException, InterruptedException {
        CloneGroupOffsetRequestHeader requestHeader = new CloneGroupOffsetRequestHeader();
        requestHeader.setSrcGroup(srcGroup);
        requestHeader.setDestGroup(destGroup);
        requestHeader.setTopic(topic);
        requestHeader.setOffline(isOffline);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLONE_GROUP_OFFSET, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public BrokerStatsData viewBrokerStatsData(String brokerAddr, String statsName, String statsKey) throws RemotingException, InterruptedException {
        ViewBrokerStatsDataRequestHeader requestHeader = new ViewBrokerStatsDataRequestHeader();
        requestHeader.setStatsName(statsName);
        requestHeader.setStatsKey(statsKey);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_BROKER_STATS_DATA, requestHeader);
        RemotingCommand response = this.remotingClient .invokeSync(brokerAddr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() != SUCCESS) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        byte[] body = response.getBody();
        if (body != null) {
            return BrokerStatsData.decode(body, BrokerStatsData.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public ConsumeStatsList fetchConsumeStatsInBroker(String brokerAddr, boolean isOrder) throws RemotingException, InterruptedException {
        GetConsumeStatsInBrokerHeader requestHeader = new GetConsumeStatsInBrokerHeader();
        requestHeader.setIsOrder(isOrder);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONSUME_STATS, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() != SUCCESS) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }

        byte[] body = response.getBody();
        if (body != null) {
            return ConsumeStatsList.decode(body, ConsumeStatsList.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public SubscriptionGroupWrapper getAllSubscriptionGroup(String brokerAddr) throws InterruptedException, RemotingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);
        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() == SUCCESS) {
            return SubscriptionGroupWrapper.decode(response.getBody(), SubscriptionGroupWrapper.class);
        }
        throw new RemotingException(response.getCode(), response.getRemark(), brokerAddr);
    }

    public SubscriptionGroupConfig getSubscriptionGroupConfig(String brokerAddr, String group) throws InterruptedException, RemotingException {
        GetSubscriptionGroupConfigRequestHeader header = new GetSubscriptionGroupConfigRequestHeader();
        header.setGroup(group);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SUBSCRIPTIONGROUP_CONFIG, header);
        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() == SUCCESS) {
            return RemotingSerializable.decode(response.getBody(), SubscriptionGroupConfig.class);
        }
        throw new RemotingException(response.getCode(), response.getRemark(), brokerAddr);
    }

    public TopicConfigSerializeWrapper getAllTopicConfig(String addr) throws InterruptedException, RemotingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            return TopicConfigSerializeWrapper.decode(response.getBody(), TopicConfigSerializeWrapper.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public void updateNameServerConfig(Properties properties, List<String> nameServers) throws UnsupportedEncodingException, InterruptedException, RemotingException {
        String str = BeanUtils.properties2String(properties);
        if (str.length() < 1) {
            return;
        }

        List<String> invokeNameServers = (nameServers == null || nameServers.isEmpty()) ? this.remotingClient.getNameServerAddressList() : nameServers;
        if (invokeNameServers == null || invokeNameServers.isEmpty()) {
            return;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_NAMESRV_CONFIG, null);
        request.setBody(str.getBytes(MQConstants.DEFAULT_CHARSET));

        RemotingCommand errResponse = null;
        for (String nameServer : invokeNameServers) {
            RemotingCommand response = this.remotingClient.invokeSync(nameServer, request, timeoutMillis);
            assert response != null;
            if (response.getCode() != SUCCESS) {
                errResponse = response;
            }
        }

        if (errResponse != null) {
            throw new RemotingException(errResponse.getCode(), errResponse.getRemark());
        }
    }

    public Map<String, Properties> getNameServerConfig(List<String> nameServers) throws InterruptedException, RemotingException, UnsupportedEncodingException {
        List<String> invokeNameServers = (nameServers == null || nameServers.isEmpty())
            ? this.remotingClient.getNameServerAddressList()
            : nameServers;

        if (invokeNameServers == null || invokeNameServers.isEmpty()) {
            return null;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_NAMESRV_CONFIG, null);

        Map<String, Properties> configMap = new HashMap<>(4);
        for (String nameServer : invokeNameServers) {
            RemotingCommand response = this.remotingClient.invokeSync(nameServer, request, timeoutMillis);

            assert response != null;
            if (ResponseCode.SUCCESS != response.getCode()) {
                throw new RemotingException(response.getCode(), response.getRemark());
            }

            configMap.put(nameServer, BeanUtils.string2Properties(new String(response.getBody(), MQConstants.DEFAULT_CHARSET)));
        }
        return configMap;
    }

    public QueryConsumeQueueResponseBody queryConsumeQueue(String brokerAddr, String topic, int queueId, long index, int count, String consumerGroup) throws InterruptedException, RemotingException {
        QueryConsumeQueueRequestHeader requestHeader = new QueryConsumeQueueRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setIndex(index);
        requestHeader.setCount(count);
        requestHeader.setConsumerGroup(consumerGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_QUEUE, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, timeoutMillis);
        assert response != null;

        if (ResponseCode.SUCCESS == response.getCode()) {
            return QueryConsumeQueueResponseBody.decode(response.getBody(), QueryConsumeQueueResponseBody.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public void checkClientInBroker(String brokerAddr, String consumerGroup, String clientId, SubscriptionData subscriptionData) throws InterruptedException, RemotingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHECK_CLIENT_CONFIG, null);

        CheckClientRequestBody requestBody = new CheckClientRequestBody();
        requestBody.setClientId(clientId);
        requestBody.setGroup(consumerGroup);
        requestBody.setSubscriptionData(subscriptionData);
        request.setBody(requestBody.encode());

        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, timeoutMillis);
        assert response != null;

        if (ResponseCode.SUCCESS != response.getCode()) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }
    }

    public boolean resumeCheckHalfMessage(String addr, String msgId) throws RemotingException, InterruptedException {
        ResumeCheckHalfMessageRequestHeader requestHeader = new ResumeCheckHalfMessageRequestHeader();
        requestHeader.setMsgId(msgId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.RESUME_CHECK_HALF_MESSAGE, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() == SUCCESS) {
            return true;
        }
        LOG.error("Failed to resume half message check logic. Remark={}", response.getRemark());
        return false;
    }

    public void setMessageRequestMode(String brokerAddr, String topic, String consumerGroup, MessageRequestMode mode, int popShareQueueNum) throws InterruptedException, RemotingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SET_MESSAGE_REQUEST_MODE, null);

        SetMessageRequestModeRequestBody requestBody = new SetMessageRequestModeRequestBody();
        requestBody.setTopic(topic);
        requestBody.setConsumerGroup(consumerGroup);
        requestBody.setMode(mode);
        requestBody.setPopShareQueueNum(popShareQueueNum);
        request.setBody(requestBody.encode());

        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, timeoutMillis);
        assert response != null;

        if (ResponseCode.SUCCESS != response.getCode()) {
            throw new RemotingException(response.getCode(), response.getRemark());
        }
    }

    public TopicConfigAndQueueMapping getTopicConfig(String brokerAddr, String topic) throws InterruptedException, RemotingException {
        GetTopicConfigRequestHeader header = new GetTopicConfigRequestHeader();
        header.setTopic(topic);
        header.setLo(true);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_CONFIG, header);
        RemotingCommand response = this.remotingClient .invokeSync(brokerAddr, request, timeoutMillis);
        assert response != null;

        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return RemotingSerializable.decode(response.getBody(), TopicConfigAndQueueMapping.class);
            }
            //should check exist
            case ResponseCode.TOPIC_NOT_EXIST: {
                //should return null?
                break;
            }
            default:
                break;
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public void createStaticTopic(String addr, String defaultTopic, TopicConfig topicConfig, TopicQueueMappingDetail topicQueueMappingDetail, boolean force) throws InterruptedException, RemotingException {
        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
        requestHeader.setTopic(topicConfig.getTopicName());
        requestHeader.setDefaultTopic(defaultTopic);
        requestHeader.setReadQueueNums(topicConfig.getReadQueueNums());
        requestHeader.setWriteQueueNums(topicConfig.getWriteQueueNums());
        requestHeader.setPerm(topicConfig.getPerm());
        requestHeader.setTopicFilterType(topicConfig.getTopicFilterType().name());
        requestHeader.setTopicSysFlag(topicConfig.getTopicSysFlag());
        requestHeader.setOrder(topicConfig.isOrder());
        requestHeader.setForce(force);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_STATIC_TOPIC, requestHeader);
        request.setBody(topicQueueMappingDetail.encode());

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() == SUCCESS) {
            return;
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public GroupForbidden updateAndGetGroupForbidden(String addr, UpdateGroupForbiddenRequestHeader requestHeader) throws InterruptedException, RemotingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_GET_GROUP_FORBIDDEN, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            return RemotingSerializable.decode(response.getBody(), GroupForbidden.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark(), addr);
    }

    public void resetMasterFlushOffset(String brokerAddr, long masterFlushOffset) throws InterruptedException, RemotingException {
        ResetMasterFlushOffsetHeader requestHeader = new ResetMasterFlushOffsetHeader();
        requestHeader.setMasterFlushOffset(masterFlushOffset);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.RESET_MASTER_FLUSH_OFFSET, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, 3000);

        assert response != null;
        if (response.getCode() == SUCCESS) {
            return;
        }
        throw new RemotingException(response.getCode(), response.getRemark(), brokerAddr);
    }

    public HARuntimeInfo getBrokerHAStatus(String brokerAddr) throws InterruptedException, RemotingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_HA_STATUS, null);
        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, timeoutMillis);
        assert response != null;

        if (response.getCode() == SUCCESS) {
            return HARuntimeInfo.decode(response.getBody(), HARuntimeInfo.class);
        }

        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public GetMetaDataResponseHeader getControllerMetaData(String controllerAddress) throws InterruptedException, RemotingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_METADATA_INFO, null);
        RemotingCommand response = this.remotingClient.invokeSync(controllerAddress, request, 3000);

        assert response != null;
        if (response.getCode() == SUCCESS) {
            return (GetMetaDataResponseHeader) response.decodeCommandCustomHeader(GetMetaDataResponseHeader.class);
        }
        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public BrokerReplicasInfo getInSyncStateData(String controllerAddress, List<String> brokers) throws InterruptedException, RemotingException {
        // Get controller leader address.
        GetMetaDataResponseHeader controllerMetaData = getControllerMetaData(controllerAddress);
        assert controllerMetaData != null;
        assert controllerMetaData.getControllerLeaderAddress() != null;
        String leaderAddress = controllerMetaData.getControllerLeaderAddress();

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_SYNC_STATE_DATA, null);
        byte[] body = RemotingSerializable.encode(brokers);
        request.setBody(body);

        RemotingCommand response = this.remotingClient.invokeSync(leaderAddress, request, 3000);
        assert response != null;

        if (response.getCode() == SUCCESS) {
            return RemotingSerializable.decode(response.getBody(), BrokerReplicasInfo.class);
        }
        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public EpochEntryCache getBrokerEpochCache(String brokerAddr) throws InterruptedException, RemotingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_EPOCH_CACHE, null);
        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, 3000);
        assert response != null;

        if (response.getCode() == SUCCESS) {
            return RemotingSerializable.decode(response.getBody(), EpochEntryCache.class);
        }
        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public Map<String, Properties> getControllerConfig(List<String> controllerServers) throws InterruptedException, RemotingException, UnsupportedEncodingException {
        List<String> invokeControllerServers = (controllerServers == null || controllerServers.isEmpty()) ? this.remotingClient.getNameServerAddressList() : controllerServers;
        if (invokeControllerServers == null || invokeControllerServers.isEmpty()) {
            return null;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONTROLLER_CONFIG, null);

        Map<String, Properties> configMap = new HashMap<>(4);
        for (String controller : invokeControllerServers) {
            RemotingCommand response = this.remotingClient.invokeSync(controller, request, timeoutMillis);

            assert response != null;
            if (ResponseCode.SUCCESS != response.getCode()) {
                throw new RemotingException(response.getCode(), response.getRemark());
            }

            configMap.put(controller, BeanUtils.string2Properties(new String(response.getBody(), MQConstants.DEFAULT_CHARSET)));
        }
        return configMap;
    }

    public void updateControllerConfig(Properties properties, List<String> controllers) throws InterruptedException, UnsupportedEncodingException, RemotingException {
        String str = BeanUtils.properties2String(properties);
        if (str.length() < 1 || controllers == null || controllers.isEmpty()) {
            return;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONTROLLER_CONFIG, null);
        request.setBody(str.getBytes(MQConstants.DEFAULT_CHARSET));

        RemotingCommand errResponse = null;
        for (String controller : controllers) {
            RemotingCommand response = this.remotingClient.invokeSync(controller, request, timeoutMillis);
            assert response != null;
            if (response.getCode() != SUCCESS) {
                errResponse = response;
            }
        }

        if (errResponse != null) {
            throw new RemotingException(errResponse.getCode(), errResponse.getRemark());
        }
    }

    public Pair<ElectMasterResponseHeader, BrokerMemberGroup> electMaster(String controllerAddr, String clusterName, String brokerName, Long brokerId) throws RemotingException, InterruptedException {
        //get controller leader address
        GetMetaDataResponseHeader controllerMetaData = this.getControllerMetaData(controllerAddr);
        assert controllerMetaData != null;
        assert controllerMetaData.getControllerLeaderAddress() != null;
        String leaderAddress = controllerMetaData.getControllerLeaderAddress();
        ElectMasterRequestHeader electRequestHeader = ElectMasterRequestHeader.ofAdminTrigger(clusterName, brokerName, brokerId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_ELECT_MASTER, electRequestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(leaderAddress, request, 3000);
        assert response != null;

        if (response.getCode() == SUCCESS) {
            BrokerMemberGroup brokerMemberGroup = RemotingSerializable.decode(response.getBody(), BrokerMemberGroup.class);
            ElectMasterResponseHeader responseHeader = (ElectMasterResponseHeader) response.decodeCommandCustomHeader(ElectMasterResponseHeader.class);
            return new Pair<>(responseHeader, brokerMemberGroup);
        }
        throw new RemotingException(response.getCode(), response.getRemark());
    }

    public void cleanControllerBrokerData(String controllerAddr, String clusterName, String brokerName, String brokerControllerIdsToClean, boolean isCleanLivingBroker) throws InterruptedException, RemotingException {
        //get controller leader address
        GetMetaDataResponseHeader controllerMetaData = this.getControllerMetaData(controllerAddr);
        assert controllerMetaData != null;
        assert controllerMetaData.getControllerLeaderAddress() != null;
        String leaderAddress = controllerMetaData.getControllerLeaderAddress();

        CleanControllerBrokerDataRequestHeader cleanHeader = new CleanControllerBrokerDataRequestHeader(clusterName, brokerName, brokerControllerIdsToClean, isCleanLivingBroker);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_BROKER_DATA, cleanHeader);

        RemotingCommand response = this.remotingClient.invokeSync(leaderAddress, request, 3000);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            return;
        }
        throw new RemotingException(response.getCode(), response.getRemark());
    }
}
