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

package org.apache.rocketmq.proxy.remoting.activity;

import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.net.HostAndPort;
import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.domain.constant.MQVersion;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class GetTopicRouteActivity extends AbstractRemotingActivity {
    public GetTopicRouteActivity(RequestPipeline requestPipeline,
        MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
    }

    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        response.setBody(getBody(request, context));
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private byte[] getBody(RemotingCommand request, ProxyContext context) throws Exception {
        final GetRouteInfoRequestHeader requestHeader = (GetRouteInfoRequestHeader) request.decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);
        TopicRouteData topicRouteData = buildTopicRouteData(requestHeader, context);
        Boolean standardJsonOnly = requestHeader.getAcceptStandardJsonOnly();

        byte[] content;
        if (request.getVersion() >= MQVersion.Version.V4_9_4.ordinal() || null != standardJsonOnly && standardJsonOnly) {
            content = topicRouteData.encode(SerializerFeature.BrowserCompatible,
                SerializerFeature.QuoteFieldNames, SerializerFeature.SkipTransientField,
                SerializerFeature.MapSortField);
        } else {
            content = topicRouteData.encode();
        }

        return content;
    }

    private TopicRouteData buildTopicRouteData(GetRouteInfoRequestHeader requestHeader, ProxyContext context) throws Exception {
        List<Address> addressList = createAddressList();
        ProxyTopicRouteData proxyTopicRouteData = messagingProcessor.getTopicRouteDataForProxy(context, addressList, requestHeader.getTopic());

        return proxyTopicRouteData.buildTopicRouteData();
    }

    private List<Address> createAddressList() {
        List<Address> addressList = new ArrayList<>();
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        // AddressScheme is just a placeholder and will not affect topic route result in this case.
        addressList.add(new Address(Address.AddressScheme.IPv4, HostAndPort.fromParts(proxyConfig.getRemotingAccessAddr(), proxyConfig.getRemotingListenPort())));

        return addressList;
    }

}
