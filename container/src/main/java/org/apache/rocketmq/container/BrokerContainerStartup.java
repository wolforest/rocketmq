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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.domain.constant.MQVersion;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.apache.rocketmq.common.utils.NetworkUtils;
import org.apache.rocketmq.common.utils.BeanUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.common.utils.ServerUtil;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;

public class BrokerContainerStartup {
    private static final String BROKER_CONTAINER_CONFIG_OPTION = "c";
    private static final String BROKER_CONFIG_OPTION = "b";
    private static final String PRINT_PROPERTIES_OPTION = "p";
    private static final String PRINT_IMPORTANT_PROPERTIES_OPTION = "m";
    public static Properties properties = null;
    public static CommandLine commandLine = null;
    public static String configFile = null;
    public static Logger log;
    public static final SystemConfigFileHelper CONFIG_FILE_HELPER = new SystemConfigFileHelper();
    public static String rocketmqHome = null;

    public static void main(String[] args) {
        final BrokerContainer brokerContainer = startBrokerContainer(createBrokerContainer(args));
        createAndStartBrokers(brokerContainer);
    }

    public static BrokerController createBrokerController(String[] args) {
        final BrokerContainer brokerContainer = startBrokerContainer(createBrokerContainer(args));
        return createAndInitializeBroker(brokerContainer, configFile, properties);
    }

    public static List<BrokerController> createAndStartBrokers(BrokerContainer brokerContainer) {
        String[] configPaths = parseBrokerConfigPath();
        List<BrokerController> brokerControllerList = new ArrayList<>();
        if (configPaths == null || configPaths.length <= 0) {
            return brokerControllerList;
        }

        SystemConfigFileHelper configFileHelper = new SystemConfigFileHelper();
        for (String configPath : configPaths) {
            System.out.printf("Start broker from config file path %s%n", configPath);
            Properties brokerProperties = loadConfigProperties(configFileHelper, configPath);

            final BrokerController brokerController = createAndInitializeBroker(brokerContainer, configPath, brokerProperties);
            if (brokerController == null) {
                continue;
            }

            brokerControllerList.add(brokerController);
            startBrokerController(brokerContainer, brokerController, brokerProperties);
        }

        return brokerControllerList;
    }

    private static String[] parseBrokerConfigPath() {
        String brokerConfigPath = getBrokerConfigPath();

        if (brokerConfigPath != null) {
            return brokerConfigPath.split(":");
        }
        return null;
    }

    private static Properties loadConfigProperties(SystemConfigFileHelper configFileHelper, String configPath) {
        configFileHelper.setFile(configPath);

        Properties brokerProperties = null;
        try {
            brokerProperties = configFileHelper.loadConfig();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return brokerProperties;
    }

    private static String getBrokerConfigPath() {
        if (commandLine.hasOption(BROKER_CONFIG_OPTION)) {
            return commandLine.getOptionValue(BROKER_CONFIG_OPTION);
        }

        if (!commandLine.hasOption(BROKER_CONTAINER_CONFIG_OPTION)) {
            return null;
        }

        String brokerContainerConfigPath = commandLine.getOptionValue(BROKER_CONTAINER_CONFIG_OPTION);
        if (brokerContainerConfigPath == null) {
            return null;
        }

        BrokerContainerConfig brokerContainerConfig = new BrokerContainerConfig();
        SystemConfigFileHelper configFileHelper = new SystemConfigFileHelper();

        Properties properties = loadConfigProperties(configFileHelper, brokerContainerConfigPath);
        if (properties != null) {
            BeanUtils.properties2Object(properties, brokerContainerConfig);
        }
        return brokerContainerConfig.getBrokerConfigPaths();
    }


    private static BrokerController createAndInitializeBroker(BrokerContainer brokerContainer, String filePath, Properties brokerProperties) {
        if (brokerProperties != null) {
            properties2SystemEnv(brokerProperties);
        }

        BrokerConfig brokerConfig = getBrokerConfig(brokerProperties, filePath);
        MessageStoreConfig messageStoreConfig = getMessageStoreConfig(brokerProperties, brokerConfig);

        setBrokerId(brokerConfig, messageStoreConfig);
        validateReplicas(messageStoreConfig);
        logConfigProperties(brokerConfig, messageStoreConfig);

        try {
            BrokerController brokerController = brokerContainer.addBroker(brokerConfig, messageStoreConfig);
            if (brokerController != null) {
                brokerController.getConfiguration().registerConfig(brokerProperties);
                return brokerController;
            }

            System.out.printf("Add broker [%s-%s] failed.%n", brokerConfig.getBrokerName(), brokerConfig.getBrokerId());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    private static BrokerConfig getBrokerConfig(Properties brokerProperties, String filePath) {
        BrokerConfig brokerConfig = new BrokerConfig();

        if (brokerProperties != null) {
            BeanUtils.properties2Object(brokerProperties, brokerConfig);
        }

        BeanUtils.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);
        brokerConfig.setBrokerConfigPath(filePath);

        return brokerConfig;
    }

    private static MessageStoreConfig getMessageStoreConfig(Properties brokerProperties, BrokerConfig brokerConfig) {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

        if (brokerProperties != null) {
            BeanUtils.properties2Object(brokerProperties, messageStoreConfig);
        }

        messageStoreConfig.setHaListenPort(brokerConfig.getListenPort() + 1);
        return messageStoreConfig;
    }

    private static void setBrokerId(BrokerConfig brokerConfig, MessageStoreConfig messageStoreConfig) {
        if (brokerConfig.isEnableControllerMode()) {
            return;
        }

        switch (messageStoreConfig.getBrokerRole()) {
            case ASYNC_MASTER:
            case SYNC_MASTER:
                brokerConfig.setBrokerId(MQConstants.MASTER_ID);
                break;
            case SLAVE:
                if (brokerConfig.getBrokerId() <= 0) {
                    System.out.printf("Slave's brokerId must be > 0%n");
                    System.exit(-3);
                }

                break;
            default:
                break;
        }
    }

    private static void validateReplicas(MessageStoreConfig messageStoreConfig) {
        if (messageStoreConfig.getTotalReplicas() < messageStoreConfig.getInSyncReplicas()
            || messageStoreConfig.getTotalReplicas() < messageStoreConfig.getMinInSyncReplicas()
            || messageStoreConfig.getInSyncReplicas() < messageStoreConfig.getMinInSyncReplicas()) {
            System.out.printf("invalid replicas number%n");
            System.exit(-3);
        }
    }

    private static void logConfigProperties(BrokerConfig brokerConfig, MessageStoreConfig messageStoreConfig) {
        log = LoggerFactory.getLogger(brokerConfig.getIdentifier() + LoggerName.BROKER_LOGGER_NAME);
        BeanUtils.printObjectProperties(log, brokerConfig);
        BeanUtils.printObjectProperties(log, messageStoreConfig);
    }

    public static BrokerContainer startBrokerContainer(BrokerContainer brokerContainer) {
        try {

            brokerContainer.start();
            logContainerStartInfo(brokerContainer);

            return brokerContainer;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static void startBrokerController(BrokerContainer brokerContainer,
        BrokerController brokerController, Properties brokerProperties) {
        try {
            executeBeforeStart(brokerContainer, brokerController, brokerProperties);
            brokerController.start();
            executeAfterStart(brokerContainer, brokerController, brokerProperties);

            logControllerStartInfo(brokerController);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void logControllerStartInfo(BrokerController brokerController) {
        String tip = String.format("Broker [%s-%s] boot success. serializeType=%s",
            brokerController.getBrokerConfig().getBrokerName(),
            brokerController.getBrokerConfig().getBrokerId(),
            RemotingCommand.getSerializeTypeConfigInThisServer());

        log.info(tip);
        System.out.printf("%s%n", tip);
    }

    private static void logContainerStartInfo(BrokerContainer brokerContainer) {
        String tip = "The broker container boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
        if (null != brokerContainer.getBrokerContainerConfig().getNamesrvAddr()) {
            tip += " and name server is " + brokerContainer.getBrokerContainerConfig().getNamesrvAddr();
        }

        log.info(tip);
        System.out.printf("%s%n", tip);
    }

    private static void executeBeforeStart(BrokerContainer brokerContainer, BrokerController brokerController, Properties brokerProperties) throws Exception {
        for (BrokerBootHook hook : brokerContainer.getBrokerBootHookList()) {
            hook.executeBeforeStart(brokerController, brokerProperties);
        }
    }

    private static void executeAfterStart(BrokerContainer brokerContainer, BrokerController brokerController, Properties brokerProperties) throws Exception {
        for (BrokerBootHook hook : brokerContainer.getBrokerBootHookList()) {
            hook.executeAfterStart(brokerController, brokerProperties);
        }
    }

    public static void shutdown(final BrokerContainer controller) {
        if (null != controller) {
            controller.shutdown();
        }
    }

    public static BrokerContainer createBrokerContainer(String[] args) {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        initSocketBufSize();

        try {
            //PackageConflictDetect.detectFastjson();
            initCommandLine(args);

            final BrokerContainerConfig containerConfig = new BrokerContainerConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            final NettyClientConfig nettyClientConfig = new NettyClientConfig();
            nettyServerConfig.setListenPort(10811);

            initPathConfig();
            properties2Object(containerConfig, nettyServerConfig, nettyClientConfig);
            initRocketmqHome(containerConfig);
            validateNameSrvAddr(containerConfig);
            printObjectProperties(containerConfig, nettyServerConfig, nettyClientConfig);

            final BrokerContainer brokerContainer = new BrokerContainer(containerConfig, nettyServerConfig, nettyClientConfig);
            // remember all configs to prevent discard
            brokerContainer.getConfiguration().registerConfig(properties);

            initContainer(brokerContainer);
            addShutdownHook(brokerContainer);

            return brokerContainer;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    private static void initSocketBufSize() {
        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE)) {
            NettySystemConfig.socketSndbufSize = 131072;
        }

        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE)) {
            NettySystemConfig.socketRcvbufSize = 131072;
        }
    }

    private static void initCommandLine(String[] args) {
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        commandLine = ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options),
            new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
        }
    }

    private static void initPathConfig() {
        if (!commandLine.hasOption(BROKER_CONTAINER_CONFIG_OPTION)) {
            return;
        }

        String file = commandLine.getOptionValue(BROKER_CONTAINER_CONFIG_OPTION);
        if (file == null) {
            return;
        }

        CONFIG_FILE_HELPER.setFile(file);
        configFile = file;
        BrokerPathConfigHelper.setBrokerConfigPath(file);
    }

    private static void properties2Object(BrokerContainerConfig containerConfig, NettyServerConfig nettyServerConfig, NettyClientConfig nettyClientConfig) throws Exception {
        properties = CONFIG_FILE_HELPER.loadConfig();
        if (properties != null) {
            properties2SystemEnv(properties);
            BeanUtils.properties2Object(properties, containerConfig);
            BeanUtils.properties2Object(properties, nettyServerConfig);
            BeanUtils.properties2Object(properties, nettyClientConfig);
        }

        BeanUtils.properties2Object(ServerUtil.commandLine2Properties(commandLine), containerConfig);
    }

    private static void initRocketmqHome(BrokerContainerConfig containerConfig) {
        if (null == containerConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation", MQConstants.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }
        rocketmqHome = containerConfig.getRocketmqHome();
    }

    private static void validateNameSrvAddr(BrokerContainerConfig containerConfig) {
        String namesrvAddr = containerConfig.getNamesrvAddr();
        if (null == namesrvAddr) {
            return;
        }

        try {
            String[] addrArray = namesrvAddr.split(";");
            for (String addr : addrArray) {
                NetworkUtils.string2SocketAddress(addr);
            }
        } catch (Exception e) {
            System.out.printf(
                "The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"%n",
                namesrvAddr);
            System.exit(-3);
        }
    }

    private static void printObjectProperties(BrokerContainerConfig containerConfig, NettyServerConfig nettyServerConfig, NettyClientConfig nettyClientConfig) {
        if (commandLine.hasOption(PRINT_PROPERTIES_OPTION)) {
            Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
            BeanUtils.printObjectProperties(console, containerConfig);
            BeanUtils.printObjectProperties(console, nettyServerConfig);
            BeanUtils.printObjectProperties(console, nettyClientConfig);
            System.exit(0);
        } else if (commandLine.hasOption(PRINT_IMPORTANT_PROPERTIES_OPTION)) {
            Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
            BeanUtils.printObjectProperties(console, containerConfig, true);
            BeanUtils.printObjectProperties(console, nettyServerConfig, true);
            BeanUtils.printObjectProperties(console, nettyClientConfig, true);
            System.exit(0);
        }

        log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
        BeanUtils.printObjectProperties(log, containerConfig);
        BeanUtils.printObjectProperties(log, nettyServerConfig);
        BeanUtils.printObjectProperties(log, nettyClientConfig);
    }

    private static void initContainer(BrokerContainer brokerContainer) {
        boolean initResult = brokerContainer.initialize();
        if (!initResult) {
            brokerContainer.shutdown();
            System.exit(-3);
        }
    }

    private static void addShutdownHook(BrokerContainer brokerContainer) {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            private volatile boolean hasShutdown = false;
            private AtomicInteger shutdownTimes = new AtomicInteger(0);

            @Override
            public void run() {
                synchronized (this) {
                    log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                    if (this.hasShutdown) {
                        return;
                    }

                    this.hasShutdown = true;
                    long beginTime = System.currentTimeMillis();
                    brokerContainer.shutdown();
                    long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                    log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                }
            }
        }, "ShutdownHook"));
    }

    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", NetworkUtils.WS_DOMAIN_NAME);
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", NetworkUtils.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    private static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option(BROKER_CONTAINER_CONFIG_OPTION, "configFile", true, "Config file for shared broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(PRINT_PROPERTIES_OPTION, "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(PRINT_IMPORTANT_PROPERTIES_OPTION, "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(BROKER_CONFIG_OPTION, "brokerConfigFiles", true, "The path of broker config files, split by ':'");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static class SystemConfigFileHelper {
        private static final Logger LOGGER = LoggerFactory.getLogger(SystemConfigFileHelper.class);

        private String file;

        public SystemConfigFileHelper() {
        }

        public Properties loadConfig() throws Exception {
            InputStream in = new BufferedInputStream(new FileInputStream(file));
            Properties properties = new Properties();
            properties.load(in);
            in.close();
            return properties;
        }

        public void update(Properties properties) throws Exception {
            LOGGER.error("[SystemConfigFileHelper] update no thing.");
        }

        public void setFile(String file) {
            this.file = file;
        }

        public String getFile() {
            return file;
        }
    }

}
