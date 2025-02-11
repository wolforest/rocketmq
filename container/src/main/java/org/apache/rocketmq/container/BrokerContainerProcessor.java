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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.daemon.SystemConfigFileHelper;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.app.BrokerIdentity;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.utils.BeanUtils;
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
import org.apache.rocketmq.store.server.config.MessageStoreConfig;

public class BrokerContainerProcessor implements NettyRequestProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerContainer brokerContainer;
    private List<BrokerBootHook> brokerBootHookList;
    private final Set<String> configBlackList = new HashSet<>();

    public BrokerContainerProcessor(BrokerContainer brokerContainer) {
        this.brokerContainer = brokerContainer;
        initConfigBlackList();
    }

    private void initConfigBlackList() {
        configBlackList.add("brokerConfigPaths");
        configBlackList.add("rocketmqHome");
        configBlackList.add("configBlackList");
        String[] configArray = brokerContainer.getBrokerContainerConfig().getConfigBlackList().split(";");
        configBlackList.addAll(Arrays.asList(configArray));
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

        Broker broker;
        try {
            broker = this.brokerContainer.addBroker(brokerConfig, messageStoreConfig);
        } catch (Exception e) {
            LOGGER.error("addBroker exception {}", e);
            return response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, e.getMessage());
        }

        return startBroker(broker, brokerProperties, brokerConfig, messageStoreConfig, response);
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

        BeanUtils.properties2Object(brokerProperties, brokerConfig);
        if (filePath != null && !filePath.isEmpty()) {
            brokerConfig.setBrokerConfigPath(filePath);
        }

        return brokerConfig;
    }

    private static MessageStoreConfig getMessageStoreConfig(Properties brokerProperties, BrokerConfig brokerConfig) {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

        BeanUtils.properties2Object(brokerProperties, messageStoreConfig);
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
                    brokerConfig.setBrokerId(MQConstants.MASTER_ID);
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

    private RemotingCommand handleStartException(Exception e, Broker broker, BrokerConfig brokerConfig, MessageStoreConfig messageStoreConfig, RemotingCommand response) throws Exception {
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
        broker.shutdown();
        return response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "start broker failed, " + e);
    }

    private RemotingCommand startBroker(Broker broker, Properties brokerProperties, BrokerConfig brokerConfig, MessageStoreConfig messageStoreConfig, RemotingCommand response) throws Exception {
        if (broker == null) {
            return response.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "add broker return null");
        }

        broker.getConfiguration().registerConfig(brokerProperties);
        try {
            for (BrokerBootHook brokerBootHook : brokerBootHookList) {
                brokerBootHook.executeBeforeStart(broker, brokerProperties);
            }
            broker.start();

            for (BrokerBootHook brokerBootHook : brokerBootHookList) {
                brokerBootHook.executeAfterStart(broker, brokerProperties);
            }
        } catch (Exception e) {
            return handleStartException(e, broker, brokerConfig, messageStoreConfig, response);
        }

        return response.setCodeAndRemark(ResponseCode.SUCCESS, null);
    }

    private synchronized RemotingCommand removeBroker(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final RemoveBrokerRequestHeader requestHeader = (RemoveBrokerRequestHeader) request.decodeCommandCustomHeader(RemoveBrokerRequestHeader.class);

        LOGGER.info("removeBroker called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        BrokerIdentity brokerIdentity = new BrokerIdentity(requestHeader.getBrokerClusterName(), requestHeader.getBrokerName(), requestHeader.getBrokerId());

        Broker broker;
        try {
            broker = this.brokerContainer.removeBroker(brokerIdentity);
        } catch (Exception e) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(e.getMessage());
            return response;
        }

        if (broker != null) {
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
                String bodyStr = new String(body, MQConstants.DEFAULT_CHARSET);
                Properties properties = BeanUtils.string2Properties(bodyStr);

                if (properties == null) {
                    LOGGER.error("string2Properties error");
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("string2Properties error");
                    return response;
                }

                if (validateBlackListConfigExist(properties)) {
                    response.setCode(ResponseCode.NO_PERMISSION);
                    response.setRemark("Can not update config in black list.");
                    return response;
                }

                LOGGER.info("updateBrokerContainerConfig, new config: [{}] client: {} ", properties, ctx.channel().remoteAddress());
                this.brokerContainer.getConfiguration().update(properties);
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

    private boolean validateBlackListConfigExist(Properties properties) {
        for (String blackConfig : configBlackList) {
            if (properties.containsKey(blackConfig)) {
                return true;
            }
        }
        return false;
    }

    private RemotingCommand getBrokerConfig(ChannelHandlerContext ctx, RemotingCommand request) {

        final RemotingCommand response = RemotingCommand.createResponseCommand(GetBrokerConfigResponseHeader.class);
        final GetBrokerConfigResponseHeader responseHeader = (GetBrokerConfigResponseHeader) response.readCustomHeader();

        String content = this.brokerContainer.getConfiguration().getAllConfigsFormatString();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MQConstants.DEFAULT_CHARSET));
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
