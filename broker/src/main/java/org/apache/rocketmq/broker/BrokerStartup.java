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
package org.apache.rocketmq.broker;

import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.service.BrokerShutdownThread;
import org.apache.rocketmq.broker.service.SystemConfigFileHelper;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.constant.MQVersion;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.MQConstants;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.common.utils.PropertyUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class BrokerStartup {
    public static Logger log;
    public static final SystemConfigFileHelper CONFIG_FILE_HELPER = new SystemConfigFileHelper();

    public static void main(String[] args) {
        start(createBrokerController(args));
    }

    public static BrokerController start(BrokerController controller) {
        try {
            controller.start();
            printBrokerStartInfo(controller);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static void shutdown(final BrokerController controller) {
        if (null != controller) {
            controller.shutdown();
        }
    }

    public static BrokerController buildBrokerController(String[] args) throws Exception {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        final BrokerConfig brokerConfig = new BrokerConfig();
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        final NettyClientConfig nettyClientConfig = new NettyClientConfig();
        final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        nettyServerConfig.setListenPort(10911);
        messageStoreConfig.setHaListenPort(0);

        CommandLine commandLine = initCommandLine(args);
        Properties properties = initProperties(commandLine, brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);
        commandLineToBrokerConfig(commandLine, brokerConfig);

        validateNamesrvAddr(brokerConfig);
        initAccessMessageInMemoryMaxRatio(messageStoreConfig);
        initBrokerID(brokerConfig, messageStoreConfig);
        setHaListenPort(nettyServerConfig, messageStoreConfig);

        brokerConfig.setInBrokerContainer(false);
        setBrokerLogDir(brokerConfig, messageStoreConfig);
        printConfigInfo(commandLine, brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);

        BrokerController controller = new BrokerController(brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);

        // Remember all configs to prevent discard
        controller.getConfiguration().registerConfig(properties);

        return controller;
    }

    public static BrokerController createBrokerController(String[] args) {
        try {
            BrokerController controller = buildBrokerController(args);
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }
            Runtime.getRuntime().addShutdownHook(new Thread(buildShutdownHook(controller)));
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    private static Properties initProperties(CommandLine commandLine, BrokerConfig brokerConfig, NettyServerConfig nettyServerConfig, NettyClientConfig nettyClientConfig, MessageStoreConfig messageStoreConfig) throws Exception {
        Properties properties = null;
        if (!commandLine.hasOption('c')) {
            return null;
        }

        String file = commandLine.getOptionValue('c');
        if (file != null) {
            CONFIG_FILE_HELPER.setFile(file);
            BrokerPathConfigHelper.setBrokerConfigPath(file);
            properties = CONFIG_FILE_HELPER.loadConfig();
        }

        if (properties != null) {
            properties2SystemEnv(properties);
            PropertyUtils.properties2Object(properties, brokerConfig);
            PropertyUtils.properties2Object(properties, nettyServerConfig);
            PropertyUtils.properties2Object(properties, nettyClientConfig);
            PropertyUtils.properties2Object(properties, messageStoreConfig);
        }

        return properties;
    }

    private static CommandLine initCommandLine(String[] args) {
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine(
            "mqbroker", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
        }

        return commandLine;
    }

    private static void commandLineToBrokerConfig(CommandLine commandLine, BrokerConfig brokerConfig) {
        PropertyUtils.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);
        if (null == brokerConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment " +
                "to match the location of the RocketMQ installation", MQConstants.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }
    }

    private static void validateNamesrvAddr(BrokerConfig brokerConfig) {
        // Validate namesrvAddr
        String namesrvAddr = brokerConfig.getNamesrvAddr();
        if (StringUtils.isNotBlank(namesrvAddr)) {
            try {
                String[] addrArray = namesrvAddr.split(";");
                for (String addr : addrArray) {
                    NetworkUtil.string2SocketAddress(addr);
                }
            } catch (Exception e) {
                System.out.printf("The Name Server Address[%s] illegal, please set it as follows, " +
                    "\"127.0.0.1:9876;192.168.0.1:9876\"%n", namesrvAddr);
                System.exit(-3);
            }
        }
    }

    private static void initAccessMessageInMemoryMaxRatio(MessageStoreConfig messageStoreConfig) {
        if (BrokerRole.SLAVE != messageStoreConfig.getBrokerRole()) {
            return;
        }

        int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
        messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
    }

    private static void parseBrokerRole(BrokerConfig brokerConfig, MessageStoreConfig messageStoreConfig) {
        if (brokerConfig.isEnableControllerMode()) {
            return;
        }

        switch (messageStoreConfig.getBrokerRole()) {
            case ASYNC_MASTER:
            case SYNC_MASTER:
                brokerConfig.setBrokerId(MQConstants.MASTER_ID);
                break;
            case SLAVE:
                if (brokerConfig.getBrokerId() <= MQConstants.MASTER_ID) {
                    System.out.printf("Slave's brokerId must be > 0%n");
                    System.exit(-3);
                }
                break;
            default:
                break;
        }
    }

    private static void initBrokerID(BrokerConfig brokerConfig, MessageStoreConfig messageStoreConfig) {
        // Set broker role according to ha config
        parseBrokerRole(brokerConfig, messageStoreConfig);

        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            brokerConfig.setBrokerId(-1);
        }

        if (brokerConfig.isEnableControllerMode() && messageStoreConfig.isEnableDLegerCommitLog()) {
            System.out.printf("The config enableControllerMode and enableDLegerCommitLog cannot both be true.%n");
            System.exit(-4);
        }
    }

    private static void setHaListenPort(NettyServerConfig nettyServerConfig, MessageStoreConfig messageStoreConfig) {
        if (messageStoreConfig.getHaListenPort() > 0) {
            return;
        }

        messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);
    }

    private static void setBrokerLogDir(BrokerConfig brokerConfig, MessageStoreConfig messageStoreConfig) {
        System.setProperty("brokerLogDir", "");
        if (brokerConfig.isIsolateLogEnable()) {
            System.setProperty("brokerLogDir", brokerConfig.getBrokerName() + "_" + brokerConfig.getBrokerId());
        }
        if (brokerConfig.isIsolateLogEnable() && messageStoreConfig.isEnableDLegerCommitLog()) {
            System.setProperty("brokerLogDir", brokerConfig.getBrokerName() + "_" + messageStoreConfig.getdLegerSelfId());
        }
    }

    private static void printConfigInfo(CommandLine commandLine, BrokerConfig brokerConfig, NettyServerConfig nettyServerConfig, NettyClientConfig nettyClientConfig, MessageStoreConfig messageStoreConfig) {
        if (commandLine.hasOption('p')) {
            Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
            PropertyUtils.printObjectProperties(console, brokerConfig);
            PropertyUtils.printObjectProperties(console, nettyServerConfig);
            PropertyUtils.printObjectProperties(console, nettyClientConfig);
            PropertyUtils.printObjectProperties(console, messageStoreConfig);
            System.exit(0);
        } else if (commandLine.hasOption('m')) {
            Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
            PropertyUtils.printObjectProperties(console, brokerConfig, true);
            PropertyUtils.printObjectProperties(console, nettyServerConfig, true);
            PropertyUtils.printObjectProperties(console, nettyClientConfig, true);
            PropertyUtils.printObjectProperties(console, messageStoreConfig, true);
            System.exit(0);
        }

        log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
        PropertyUtils.printObjectProperties(log, brokerConfig);
        PropertyUtils.printObjectProperties(log, nettyServerConfig);
        PropertyUtils.printObjectProperties(log, nettyClientConfig);
        PropertyUtils.printObjectProperties(log, messageStoreConfig);
    }

    private static Runnable buildShutdownHook(BrokerController brokerController) {
        return new BrokerShutdownThread(brokerController);
    }

    private static void printBrokerStartInfo(BrokerController controller) {
        String tip = String.format("The broker[%s, %s] boot success. serializeType=%s",
            controller.getBrokerConfig().getBrokerName(), controller.getBrokerAddr(),
            RemotingCommand.getSerializeTypeConfigInThisServer());

        if (null != controller.getBrokerConfig().getNamesrvAddr()) {
            tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
        }

        log.info(tip);
        System.out.printf("%s%n", tip);
    }

    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", NetworkUtil.WS_DOMAIN_NAME);
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", NetworkUtil.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    private static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

}
