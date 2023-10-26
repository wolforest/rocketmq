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

package org.apache.rocketmq.container;

import io.netty.channel.ChannelHandlerContext;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.service.SystemConfigFileHelper;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.PropertyUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.AddBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetBrokerConfigResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.RemoveBrokerRequestHeader;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class BrokerContainerProcessor implements NettyRequestProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerContainer brokerContainer;
    private List<BrokerBootHook> brokerBootHookList;

    public BrokerContainerProcessor(BrokerContainer brokerContainer) {
        this.brokerContainer = brokerContainer;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()) {
            case RequestCode.ADD_BROKER:
                return this.addBroker(ctx, request);
            case RequestCode.REMOVE_BROKER:
                return this.removeBroker(ctx, request);
            case RequestCode.GET_BROKER_CONFIG:
                return this.getBrokerConfig(ctx, request);
            case RequestCode.UPDATE_BROKER_CONFIG:
                return this.updateBrokerConfig(ctx, request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private synchronized RemotingCommand addBroker(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final AddBrokerRequestHeader requestHeader = (AddBrokerRequestHeader) request.decodeCommandCustomHeader(AddBrokerRequestHeader.class);
        LOGGER.info("addBroker called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        String configPath = requestHeader.getConfigPath();
        Properties brokerProperties = getConfigProperties(configPath, response);
        if (brokerProperties == null) {
            return  response;
        }

        BrokerConfig brokerConfig = getBrokerConfig(brokerProperties, configPath);
        MessageStoreConfig messageStoreConfig = getMessageStoreConfig(brokerProperties, brokerConfig);

        RemotingCommand validateResult = validateConfig(brokerConfig, messageStoreConfig, response);
        if (validateResult != null) {
            return validateResult;
        }

        BrokerController brokerController;
        try {
            brokerController = this.brokerContainer.addBroker(brokerConfig, messageStoreConfig);
        } catch (Exception e) {
            LOGGER.error("addBroker exception {}", e);
            return response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, e.getMessage());
        }

        return startBroker(brokerController, brokerProperties, brokerConfig, messageStoreConfig, response);
    }

    private Properties getConfigProperties(String configPath, RemotingCommand response) throws UnsupportedEncodingException {
        Properties brokerProperties = null;
        if (configPath != null && !configPath.isEmpty()) {
            SystemConfigFileHelper configFileHelper = new SystemConfigFileHelper();
            configFileHelper.setFile(configPath);

            try {
                brokerProperties = configFileHelper.loadConfig();
            } catch (Exception e) {
                LOGGER.error("addBroker load config from {} failed, {}", configPath, e);
            }
        } else {
            LOGGER.error("addBroker config path is empty");
            response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "addBroker config path is empty");
            return null;
        }

        if (null == brokerProperties) {
            LOGGER.error("addBroker properties empty");
            response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "addBroker properties empty");
            return null;
        }

        return brokerProperties;
    }

    private static BrokerConfig getBrokerConfig(Properties brokerProperties, String filePath) {
        BrokerConfig brokerConfig = new BrokerConfig();

        PropertyUtils.properties2Object(brokerProperties, brokerConfig);
        if (filePath != null && !filePath.isEmpty()) {
            brokerConfig.setBrokerConfigPath(filePath);
        }

        return brokerConfig;
    }

    private static MessageStoreConfig getMessageStoreConfig(Properties brokerProperties, BrokerConfig brokerConfig) {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

        PropertyUtils.properties2Object(brokerProperties, messageStoreConfig);
        messageStoreConfig.setHaListenPort(brokerConfig.getListenPort() + 1);

        return messageStoreConfig;
    }

    private RemotingCommand validateConfig(BrokerConfig brokerConfig, MessageStoreConfig messageStoreConfig, RemotingCommand response) {
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            return null;
        }

        if (!brokerConfig.isEnableControllerMode()) {
            switch (messageStoreConfig.getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                case SLAVE:
                    if (brokerConfig.getBrokerId() <= 0) {
                        return response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "slave broker id must be > 0");
                    }
                    break;
                default:
                    break;

            }
        }

        if (messageStoreConfig.getTotalReplicas() < messageStoreConfig.getInSyncReplicas()
            || messageStoreConfig.getTotalReplicas() < messageStoreConfig.getMinInSyncReplicas()
            || messageStoreConfig.getInSyncReplicas() < messageStoreConfig.getMinInSyncReplicas()) {
            return response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "invalid replicas number");
        }

        return null;
    }

    private RemotingCommand handleStartException(Exception e, BrokerController brokerController, BrokerConfig brokerConfig, MessageStoreConfig messageStoreConfig, RemotingCommand response) throws Exception {
        LOGGER.error("start broker exception {}", e);
        BrokerIdentity brokerIdentity;
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            brokerIdentity = new BrokerIdentity(brokerConfig.getBrokerClusterName(),
                brokerConfig.getBrokerName(), Integer.parseInt(messageStoreConfig.getdLegerSelfId().substring(1)));
        } else {
            brokerIdentity = new BrokerIdentity(brokerConfig.getBrokerClusterName(),
                brokerConfig.getBrokerName(), brokerConfig.getBrokerId());
        }
        this.brokerContainer.removeBroker(brokerIdentity);
        brokerController.shutdown();
        return response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "start broker failed, " + e);
    }

    private RemotingCommand startBroker(BrokerController brokerController, Properties brokerProperties, BrokerConfig brokerConfig, MessageStoreConfig messageStoreConfig, RemotingCommand response) throws Exception {
        if (brokerController == null) {
            return response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "add broker return null");
        }

        brokerController.getConfiguration().registerConfig(brokerProperties);
        try {
            for (BrokerBootHook brokerBootHook : brokerBootHookList) {
                brokerBootHook.executeBeforeStart(brokerController, brokerProperties);
            }
            brokerController.start();

            for (BrokerBootHook brokerBootHook : brokerBootHookList) {
                brokerBootHook.executeAfterStart(brokerController, brokerProperties);
            }
        } catch (Exception e) {
            return handleStartException(e, brokerController, brokerConfig, messageStoreConfig, response);
        }

        return response.setCodeAndRemark(ResponseCode.SUCCESS, null);
    }

    private synchronized RemotingCommand removeBroker(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final RemoveBrokerRequestHeader requestHeader = (RemoveBrokerRequestHeader) request.decodeCommandCustomHeader(RemoveBrokerRequestHeader.class);

        LOGGER.info("removeBroker called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        BrokerIdentity brokerIdentity = new BrokerIdentity(requestHeader.getBrokerClusterName(), requestHeader.getBrokerName(), requestHeader.getBrokerId());

        BrokerController brokerController;
        try {
            brokerController = this.brokerContainer.removeBroker(brokerIdentity);
        } catch (Exception e) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(e.getMessage());
            return response;
        }

        if (brokerController != null) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            response.setCode(ResponseCode.BROKER_NOT_EXIST);
            response.setRemark("Broker not exist");
        }
        return response;
    }

    public void registerBrokerBootHook(List<BrokerBootHook> brokerBootHookList) {
        this.brokerBootHookList = brokerBootHookList;
    }

    private RemotingCommand updateBrokerConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        LOGGER.info("updateSharedBrokerConfig called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        byte[] body = request.getBody();
        if (body != null) {
            try {
                String bodyStr = new String(body, MixAll.DEFAULT_CHARSET);
                Properties properties = PropertyUtils.string2Properties(bodyStr);
                if (properties != null) {
                    LOGGER.info("updateSharedBrokerConfig, new config: [{}] client: {} ", properties, ctx.channel().remoteAddress());
                    this.brokerContainer.getConfiguration().update(properties);
                } else {
                    LOGGER.error("string2Properties error");
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("string2Properties error");
                    return response;
                }
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getBrokerConfig(ChannelHandlerContext ctx, RemotingCommand request) {

        final RemotingCommand response = RemotingCommand.createResponseCommand(GetBrokerConfigResponseHeader.class);
        final GetBrokerConfigResponseHeader responseHeader = (GetBrokerConfigResponseHeader) response.readCustomHeader();

        String content = this.brokerContainer.getConfiguration().getAllConfigsFormatString();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        responseHeader.setVersion(this.brokerContainer.getConfiguration().getDataVersionJson());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }
}
