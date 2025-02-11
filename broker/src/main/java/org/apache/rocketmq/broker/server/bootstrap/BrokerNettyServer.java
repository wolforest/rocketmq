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
package org.apache.rocketmq.broker.server.bootstrap;

import io.netty.channel.Channel;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.acl.plain.PlainAccessValidator;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.connection.ClientHousekeepingService;
import org.apache.rocketmq.broker.domain.consumer.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.domain.consumer.DefaultConsumerIdsChangeListener;
import org.apache.rocketmq.broker.domain.coldctr.ColdDataPullRequestHoldThread;
import org.apache.rocketmq.broker.server.daemon.BrokerFastFailure;
import org.apache.rocketmq.broker.server.connection.longpolling.LmqPullRequestHoldThread;
import org.apache.rocketmq.broker.server.connection.longpolling.NotifyMessageArrivingListener;
import org.apache.rocketmq.broker.server.connection.longpolling.PullRequestHoldThread;
import org.apache.rocketmq.broker.domain.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.domain.mqtrace.SendMessageHook;
import org.apache.rocketmq.broker.api.controller.AckMessageProcessor;
import org.apache.rocketmq.broker.api.controller.AdminBrokerProcessor;
import org.apache.rocketmq.broker.api.controller.ChangeInvisibleTimeProcessor;
import org.apache.rocketmq.broker.api.controller.ClientManageProcessor;
import org.apache.rocketmq.broker.api.controller.ConsumerManageProcessor;
import org.apache.rocketmq.broker.api.controller.EndTransactionProcessor;
import org.apache.rocketmq.broker.api.controller.NotificationProcessor;
import org.apache.rocketmq.broker.api.controller.PeekMessageProcessor;
import org.apache.rocketmq.broker.api.controller.PollingInfoProcessor;
import org.apache.rocketmq.broker.api.controller.PopMessageProcessor;
import org.apache.rocketmq.broker.server.daemon.pop.PopServiceManager;
import org.apache.rocketmq.broker.api.controller.PullMessageProcessor;
import org.apache.rocketmq.broker.api.controller.QueryAssignmentProcessor;
import org.apache.rocketmq.broker.api.controller.QueryMessageProcessor;
import org.apache.rocketmq.broker.api.controller.ReplyMessageProcessor;
import org.apache.rocketmq.broker.api.controller.SendMessageProcessor;
import org.apache.rocketmq.common.app.config.BrokerConfig;
import org.apache.rocketmq.common.lang.thread.ThreadFactoryImpl;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.utils.ServiceProvider;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.common.utils.TimeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.common.lang.thread.FileWatchService;
import org.apache.rocketmq.remoting.protocol.RequestHeaderRegistry;
import org.apache.rocketmq.store.api.plugin.MessageArrivingListener;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;

public class BrokerNettyServer {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final Logger LOG_WATER_MARK = LoggerFactory.getLogger(LoggerName.WATER_MARK_LOGGER_NAME);

    private PullMessageProcessor pullMessageProcessor;
    private PeekMessageProcessor peekMessageProcessor;
    private PopMessageProcessor popMessageProcessor;
    private AckMessageProcessor ackMessageProcessor;
    private ChangeInvisibleTimeProcessor changeInvisibleTimeProcessor;
    private NotificationProcessor notificationProcessor;
    private PollingInfoProcessor pollingInfoProcessor;
    private QueryAssignmentProcessor queryAssignmentProcessor;
    private ClientManageProcessor clientManageProcessor;
    private SendMessageProcessor sendMessageProcessor;
    private ReplyMessageProcessor replyMessageProcessor;
    private EndTransactionProcessor endTransactionProcessor;

    private ConsumerIdsChangeListener consumerIdsChangeListener;
    private ClientHousekeepingService clientHousekeepingService;
    private PullRequestHoldThread pullRequestHoldThread;
    private MessageArrivingListener messageArrivingListener;
    private FileWatchService fileWatchService;
    private PopServiceManager popServiceManager;

    private BlockingQueue<Runnable> sendThreadPoolQueue;
    private BlockingQueue<Runnable> putThreadPoolQueue;
    private BlockingQueue<Runnable> ackThreadPoolQueue;
    private BlockingQueue<Runnable> pullThreadPoolQueue;
    private BlockingQueue<Runnable> litePullThreadPoolQueue;
    private BlockingQueue<Runnable> replyThreadPoolQueue;
    private BlockingQueue<Runnable> queryThreadPoolQueue;
    private BlockingQueue<Runnable> clientManagerThreadPoolQueue;
    private BlockingQueue<Runnable> heartbeatThreadPoolQueue;
    private BlockingQueue<Runnable> consumerManagerThreadPoolQueue;
    private BlockingQueue<Runnable> endTransactionThreadPoolQueue;
    private BlockingQueue<Runnable> adminBrokerThreadPoolQueue;
    private BlockingQueue<Runnable> loadBalanceThreadPoolQueue;

    private final List<SendMessageHook> sendMessageHookList = new ArrayList<>();
    private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<>();
    private final Map<Class, AccessValidator> accessValidatorMap = new HashMap<>();

    private ExecutorService sendMessageExecutor;
    private ExecutorService pullMessageExecutor;
    private ExecutorService litePullMessageExecutor;
    private ExecutorService putMessageFutureExecutor;
    private ExecutorService ackMessageExecutor;
    private ExecutorService replyMessageExecutor;
    private ExecutorService queryMessageExecutor;
    private ExecutorService adminBrokerExecutor;
    private ExecutorService clientManageExecutor;
    private ExecutorService heartbeatExecutor;
    private ExecutorService consumerManageExecutor;
    private ExecutorService loadBalanceExecutor;
    private ExecutorService endTransactionExecutor;

    private final BrokerConfig brokerConfig;
    private final MessageStoreConfig messageStoreConfig;
    private final NettyServerConfig nettyServerConfig;
    private final Broker broker;

    private InetSocketAddress storeHost;
    private RemotingServer remotingServer;
    private RemotingServer fastRemotingServer;

    public BrokerNettyServer(BrokerConfig brokerConfig, MessageStoreConfig storeConfig, NettyServerConfig serverConfig, Broker broker) {
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = storeConfig;
        this.nettyServerConfig = serverConfig;
        this.broker = broker;

        this.setStoreHost(new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), broker.getBrokerConfig().getListenPort()));

        initProcessors();
        initThreadPoolQueue();
        initServices();
    }

    public void init() throws CloneNotSupportedException {
        initRemotingServer();
        initResources();
        registerProcessor();
        initAcl();
        initRpcHooks();
    }

    public boolean initFileWatchService() {
        if (TlsSystemConfig.tlsMode == TlsMode.DISABLED) {
            return true;
        }

        // Register a listener to reload SslContext
        try {
            String[] files = new String[] {
                TlsSystemConfig.tlsServerCertPath,
                TlsSystemConfig.tlsServerKeyPath,
                TlsSystemConfig.tlsServerTrustCertPath
            };

            fileWatchService = new FileWatchService(files, createFileWatchListener());

        } catch (Exception e) {
            LOG.warn("FileWatchService created error, can't load the certificate dynamically");
            return false;
        }

        return true;
    }

    private FileWatchService.Listener createFileWatchListener() {
        return new FileWatchService.Listener() {
            boolean certChanged, keyChanged = false;

            @Override
            public void onChanged(String path) {
                if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                    LOG.info("The trust certificate changed, reload the ssl context");
                    reloadServerSslContext();
                }
                if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                    certChanged = true;
                }
                if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                    keyChanged = true;
                }
                if (certChanged && keyChanged) {
                    LOG.info("The certificate and private key changed, reload the ssl context");
                    certChanged = keyChanged = false;
                    reloadServerSslContext();
                }
            }

            private void reloadServerSslContext() {
                ((NettyRemotingServer) getRemotingServer()).loadSslContext();
                ((NettyRemotingServer) getFastRemotingServer()).loadSslContext();
            }
        };
    }

    public void registerServerRPCHook(RPCHook rpcHook) {
        this.remotingServer.registerRPCHook(rpcHook);
        this.fastRemotingServer.registerRPCHook(rpcHook);
    }

    public void start() {
        if (this.popServiceManager != null) {
            this.popServiceManager.start();
        }

        if (this.pullRequestHoldThread != null) {
            this.pullRequestHoldThread.start();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.start();
        }

        if (this.fileWatchService != null) {
            this.fileWatchService.start();
        }

        if (this.remotingServer != null) {
            this.remotingServer.start();

            // In test scenarios where it is up to OS to pick up an available port, set the listening port back to config
            if (null != nettyServerConfig && 0 == nettyServerConfig.getListenPort()) {
                nettyServerConfig.setListenPort(remotingServer.localListenPort());
            }
        }

        if (this.fastRemotingServer != null) {
            this.fastRemotingServer.start();
        }
    }

    public void shutdown() {
        if (this.sendMessageExecutor != null) {
            this.sendMessageExecutor.shutdown();
        }

        if (this.litePullMessageExecutor != null) {
            this.litePullMessageExecutor.shutdown();
        }

        if (this.pullMessageExecutor != null) {
            this.pullMessageExecutor.shutdown();
        }

        if (this.replyMessageExecutor != null) {
            this.replyMessageExecutor.shutdown();
        }

        if (this.putMessageFutureExecutor != null) {
            this.putMessageFutureExecutor.shutdown();
        }

        if (this.ackMessageExecutor != null) {
            this.ackMessageExecutor.shutdown();
        }

        if (this.clientManageExecutor != null) {
            this.clientManageExecutor.shutdown();
        }

        if (this.queryMessageExecutor != null) {
            this.queryMessageExecutor.shutdown();
        }

        if (this.heartbeatExecutor != null) {
            this.heartbeatExecutor.shutdown();
        }

        if (this.consumerManageExecutor != null) {
            this.consumerManageExecutor.shutdown();
        }

        if (this.endTransactionExecutor != null) {
            this.endTransactionExecutor.shutdown();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.shutdown();
        }

        if (this.pullRequestHoldThread != null) {
            this.pullRequestHoldThread.shutdown();
        }

        if (this.popServiceManager != null) {
            this.popServiceManager.shutdown();
        }

        if (this.consumerIdsChangeListener != null) {
            this.consumerIdsChangeListener.shutdown();
        }

        if (this.adminBrokerExecutor != null) {
            this.adminBrokerExecutor.shutdown();
        }

        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }

        if (this.remotingServer != null) {
            this.remotingServer.shutdown();
        }

        if (this.fastRemotingServer != null) {
            this.fastRemotingServer.shutdown();
        }

    }

    public void executePullRequest(final Channel channel, final RemotingCommand request) {
        Runnable run = () -> {
            try {
                boolean brokerAllowFlowCtrSuspend = !(request.getExtFields() != null && request.getExtFields().containsKey(ColdDataPullRequestHoldThread.NO_SUSPEND_KEY));
                final RemotingCommand response = BrokerNettyServer.this.getPullMessageProcessor().processRequest(channel, request, false, brokerAllowFlowCtrSuspend);
                writeResponse(channel, request, response);
            } catch (RemotingCommandException e1) {
                LOG.error("executeRequestWhenWakeup run", e1);
            }
        };
        this.broker.getBrokerNettyServer().getPullMessageExecutor().submit(new RequestTask(run, channel, request));
    }

    private void writeResponse(final Channel channel, final RemotingCommand request, RemotingCommand response) {
        if (response == null) {
            return;
        }

        response.setOpaque(request.getOpaque());
        response.markResponseType();
        try {
            NettyRemotingAbstract.writeResponse(channel, request, response, future -> {
                if (future.isSuccess()) {
                    return;
                }

                LOG.error("processRequestWrapper response to {} failed", channel.remoteAddress(), future.cause());
                LOG.error(request.toString());
                LOG.error(response.toString());
            });
        } catch (Throwable e) {
            LOG.error("processRequestWrapper process request over, but response failed", e);
            LOG.error(request.toString());
            LOG.error(response.toString());
        }
    }



    private void registerProcessor() {
        /*
         * SendMessageProcessor
         */
        sendMessageProcessor.registerSendMessageHook(sendMessageHookList);
        sendMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);

        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendMessageProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendMessageProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendMessageProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendMessageProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendMessageProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendMessageProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendMessageProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendMessageProcessor, this.sendMessageExecutor);
        /*
         * PullMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, this.pullMessageProcessor, this.pullMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.LITE_PULL_MESSAGE, this.pullMessageProcessor, this.litePullMessageExecutor);
        this.pullMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);
        /*
         * PeekMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.PEEK_MESSAGE, this.peekMessageProcessor, this.pullMessageExecutor);
        /*
         * PopMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.POP_MESSAGE, this.popMessageProcessor, this.pullMessageExecutor);

        /*
         * AckMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.ACK_MESSAGE, this.ackMessageProcessor, this.ackMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.ACK_MESSAGE, this.ackMessageProcessor, this.ackMessageExecutor);

        this.remotingServer.registerProcessor(RequestCode.BATCH_ACK_MESSAGE, this.ackMessageProcessor, this.ackMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.BATCH_ACK_MESSAGE, this.ackMessageProcessor, this.ackMessageExecutor);
        /*
         * ChangeInvisibleTimeProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, this.changeInvisibleTimeProcessor, this.ackMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, this.changeInvisibleTimeProcessor, this.ackMessageExecutor);
        /*
         * notificationProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.NOTIFICATION, this.notificationProcessor, this.pullMessageExecutor);

        /*
         * pollingInfoProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.POLLING_INFO, this.pollingInfoProcessor, this.pullMessageExecutor);

        /*
         * ReplyMessageProcessor
         */

        replyMessageProcessor.registerSendMessageHook(sendMessageHookList);

        this.remotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE, replyMessageProcessor, replyMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE_V2, replyMessageProcessor, replyMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE, replyMessageProcessor, replyMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE_V2, replyMessageProcessor, replyMessageExecutor);

        /*
         * QueryMessageProcessor
         */
        NettyRequestProcessor queryProcessor = new QueryMessageProcessor(getBrokerController());
        this.remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.queryMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.queryMessageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.queryMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.queryMessageExecutor);

        /*
         * ClientManageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientManageProcessor, this.heartbeatExecutor);
        this.remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientManageProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientManageProcessor, this.clientManageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.HEART_BEAT, clientManageProcessor, this.heartbeatExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientManageProcessor, this.clientManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientManageProcessor, this.clientManageExecutor);

        /*
         * ConsumerManageProcessor
         */
        ConsumerManageProcessor consumerManageProcessor = new ConsumerManageProcessor(getBrokerController());
        this.remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor, this.consumerManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor, this.consumerManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);

        /*
         * QueryAssignmentProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.QUERY_ASSIGNMENT, queryAssignmentProcessor, loadBalanceExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_ASSIGNMENT, queryAssignmentProcessor, loadBalanceExecutor);
        this.remotingServer.registerProcessor(RequestCode.SET_MESSAGE_REQUEST_MODE, queryAssignmentProcessor, loadBalanceExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SET_MESSAGE_REQUEST_MODE, queryAssignmentProcessor, loadBalanceExecutor);

        /*
         * EndTransactionProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.END_TRANSACTION, endTransactionProcessor, this.endTransactionExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.END_TRANSACTION, endTransactionProcessor, this.endTransactionExecutor);

        /*
         * Default
         */
        AdminBrokerProcessor adminProcessor = new AdminBrokerProcessor(getBrokerController());
        this.remotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
        this.fastRemotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);

        /*
         * Initialize the mapping of request codes to request headers.
         */
        RequestHeaderRegistry.getInstance().initialize();
    }

    private void initRemotingServer() throws CloneNotSupportedException {
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);
        NettyServerConfig fastConfig = (NettyServerConfig) this.nettyServerConfig.clone();

        int listeningPort = nettyServerConfig.getListenPort() - 2;
        if (listeningPort < 0) {
            listeningPort = 0;
        }
        fastConfig.setListenPort(listeningPort);

        this.fastRemotingServer = new NettyRemotingServer(fastConfig, this.clientHousekeepingService);
    }

    /**
     * Initialize resources including remoting server and thread executors.
     */
    private void initResources() {
        this.sendMessageExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getSendMessageThreadPoolNums(),
            this.brokerConfig.getSendMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.sendThreadPoolQueue,
            new ThreadFactoryImpl("SendMessageThread_", getBrokerController().getBrokerIdentity()));

        this.pullMessageExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getPullMessageThreadPoolNums(),
            this.brokerConfig.getPullMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.pullThreadPoolQueue,
            new ThreadFactoryImpl("PullMessageThread_", getBrokerController().getBrokerIdentity()));

        this.litePullMessageExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getLitePullMessageThreadPoolNums(),
            this.brokerConfig.getLitePullMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.litePullThreadPoolQueue,
            new ThreadFactoryImpl("LitePullMessageThread_", getBrokerController().getBrokerIdentity()));

        this.putMessageFutureExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getPutMessageFutureThreadPoolNums(),
            this.brokerConfig.getPutMessageFutureThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.putThreadPoolQueue,
            new ThreadFactoryImpl("PutMessageThread_", getBrokerController().getBrokerIdentity()));

        this.ackMessageExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getAckMessageThreadPoolNums(),
            this.brokerConfig.getAckMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.ackThreadPoolQueue,
            new ThreadFactoryImpl("AckMessageThread_", getBrokerController().getBrokerIdentity()));

        this.queryMessageExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getQueryMessageThreadPoolNums(),
            this.brokerConfig.getQueryMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.queryThreadPoolQueue,
            new ThreadFactoryImpl("QueryMessageThread_", getBrokerController().getBrokerIdentity()));

        this.adminBrokerExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getAdminBrokerThreadPoolNums(),
            this.brokerConfig.getAdminBrokerThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.adminBrokerThreadPoolQueue,
            new ThreadFactoryImpl("AdminBrokerThread_", getBrokerController().getBrokerIdentity()));

        this.clientManageExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getClientManageThreadPoolNums(),
            this.brokerConfig.getClientManageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.clientManagerThreadPoolQueue,
            new ThreadFactoryImpl("ClientManageThread_", getBrokerController().getBrokerIdentity()));

        this.heartbeatExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getHeartbeatThreadPoolNums(),
            this.brokerConfig.getHeartbeatThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.heartbeatThreadPoolQueue,
            new ThreadFactoryImpl("HeartbeatThread_", true, getBrokerController().getBrokerIdentity()));

        this.consumerManageExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getConsumerManageThreadPoolNums(),
            this.brokerConfig.getConsumerManageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumerManagerThreadPoolQueue,
            new ThreadFactoryImpl("ConsumerManageThread_", true, getBrokerController().getBrokerIdentity()));

        this.replyMessageExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getProcessReplyMessageThreadPoolNums(),
            this.brokerConfig.getProcessReplyMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.replyThreadPoolQueue,
            new ThreadFactoryImpl("ProcessReplyMessageThread_", getBrokerController().getBrokerIdentity()));

        this.endTransactionExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getEndTransactionThreadPoolNums(),
            this.brokerConfig.getEndTransactionThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.endTransactionThreadPoolQueue,
            new ThreadFactoryImpl("EndTransactionThread_", getBrokerController().getBrokerIdentity()));

        this.loadBalanceExecutor = ThreadUtils.newThreadPoolExecutor(
            this.brokerConfig.getLoadBalanceProcessorThreadPoolNums(),
            this.brokerConfig.getLoadBalanceProcessorThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.loadBalanceThreadPoolQueue,
            new ThreadFactoryImpl("LoadBalanceProcessorThread_", getBrokerController().getBrokerIdentity()));
    }

    private void initServices() {
        this.popServiceManager = new PopServiceManager(broker, popMessageProcessor, notificationProcessor);
        this.clientHousekeepingService = new ClientHousekeepingService(getBrokerController());
        this.pullRequestHoldThread = messageStoreConfig.isEnableLmq() ? new LmqPullRequestHoldThread(getBrokerController()) : new PullRequestHoldThread(getBrokerController());
        this.messageArrivingListener = new NotifyMessageArrivingListener(this.pullRequestHoldThread, this.popServiceManager);
        this.consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(getBrokerController());
    }

    private void initProcessors() {
        this.pullMessageProcessor = new PullMessageProcessor(getBrokerController());
        this.peekMessageProcessor = new PeekMessageProcessor(getBrokerController());
        this.popMessageProcessor = new PopMessageProcessor(getBrokerController());
        this.notificationProcessor = new NotificationProcessor(getBrokerController());
        this.pollingInfoProcessor = new PollingInfoProcessor(getBrokerController());
        this.ackMessageProcessor = new AckMessageProcessor(getBrokerController());
        this.changeInvisibleTimeProcessor = new ChangeInvisibleTimeProcessor(getBrokerController());
        this.sendMessageProcessor = new SendMessageProcessor(getBrokerController());
        this.replyMessageProcessor = new ReplyMessageProcessor(getBrokerController());
        this.queryAssignmentProcessor = new QueryAssignmentProcessor(getBrokerController());
        this.clientManageProcessor = new ClientManageProcessor(getBrokerController());
        this.endTransactionProcessor = new EndTransactionProcessor(getBrokerController());
    }

    private void initThreadPoolQueue() {
        this.sendThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getSendThreadPoolQueueCapacity());
        this.putThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getPutThreadPoolQueueCapacity());
        this.pullThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getPullThreadPoolQueueCapacity());
        this.litePullThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getLitePullThreadPoolQueueCapacity());

        this.ackThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getAckThreadPoolQueueCapacity());
        this.replyThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getReplyThreadPoolQueueCapacity());
        this.queryThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getQueryThreadPoolQueueCapacity());
        this.clientManagerThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getClientManagerThreadPoolQueueCapacity());
        this.consumerManagerThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getConsumerManagerThreadPoolQueueCapacity());
        this.heartbeatThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getHeartbeatThreadPoolQueueCapacity());
        this.endTransactionThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getEndTransactionPoolQueueCapacity());
        this.adminBrokerThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getAdminBrokerThreadPoolQueueCapacity());
        this.loadBalanceThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getLoadBalanceThreadPoolQueueCapacity());

    }

    private void initAcl() {
        if (!this.brokerConfig.isAclEnable()) {
            LOG.info("The broker dose not enable acl");
            return;
        }

        List<AccessValidator> accessValidators = ServiceProvider.load(AccessValidator.class);
        if (accessValidators.isEmpty()) {
            LOG.info("ServiceProvider loaded no AccessValidator, using default org.apache.rocketmq.acl.plain.PlainAccessValidator");
            accessValidators.add(new PlainAccessValidator());
        }

        for (AccessValidator accessValidator : accessValidators) {
            initAcl(accessValidator);
        }
    }

    private void initAcl(AccessValidator accessValidator) {
        final AccessValidator validator = accessValidator;
        accessValidatorMap.put(validator.getClass(), validator);
        this.registerServerRPCHook(new RPCHook() {

            @Override
            public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
                //Do not catch the exception
                validator.validate(validator.parse(request, remoteAddr));
            }

            @Override
            public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
            }

        });
    }

    private void initRpcHooks() {
        List<RPCHook> rpcHooks = ServiceProvider.load(RPCHook.class);
        if (rpcHooks.isEmpty()) {
            return;
        }
        for (RPCHook rpcHook : rpcHooks) {
            this.registerServerRPCHook(rpcHook);
        }
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public Broker getBrokerController() {
        return broker;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public ConsumerIdsChangeListener getConsumerIdsChangeListener() {
        return consumerIdsChangeListener;
    }

    public ExecutorService getPullMessageExecutor() {
        return pullMessageExecutor;
    }

    public ExecutorService getPutMessageFutureExecutor() {
        return putMessageFutureExecutor;
    }

    public BlockingQueue<Runnable> getSendThreadPoolQueue() {
        return sendThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getAckThreadPoolQueue() {
        return ackThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getHeartbeatThreadPoolQueue() {
        return heartbeatThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getEndTransactionThreadPoolQueue() {
        return endTransactionThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getLitePullThreadPoolQueue() {
        return litePullThreadPoolQueue;
    }

    public ExecutorService getSendMessageExecutor() {
        return sendMessageExecutor;
    }

    public SendMessageProcessor getSendMessageProcessor() {
        return sendMessageProcessor;
    }

    public QueryAssignmentProcessor getQueryAssignmentProcessor() {
        return queryAssignmentProcessor;
    }

    public EndTransactionProcessor getEndTransactionProcessor() {
        return endTransactionProcessor;
    }

    public BlockingQueue<Runnable> getClientManagerThreadPoolQueue() {
        return clientManagerThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getConsumerManagerThreadPoolQueue() {
        return consumerManagerThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getAsyncPutThreadPoolQueue() {
        return putThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getReplyThreadPoolQueue() {
        return replyThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getAdminBrokerThreadPoolQueue() {
        return adminBrokerThreadPoolQueue;
    }

    public AckMessageProcessor getAckMessageProcessor() {
        return ackMessageProcessor;
    }

    public PopMessageProcessor getPopMessageProcessor() {
        return popMessageProcessor;
    }

    public PullMessageProcessor getPullMessageProcessor() {
        return pullMessageProcessor;
    }

    public PullRequestHoldThread getPullRequestHoldService() {
        return pullRequestHoldThread;
    }


    public BlockingQueue<Runnable> getPullThreadPoolQueue() {
        return pullThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getQueryThreadPoolQueue() {
        return queryThreadPoolQueue;
    }

    public ChangeInvisibleTimeProcessor getChangeInvisibleTimeProcessor() {
        return changeInvisibleTimeProcessor;
    }

    public MessageArrivingListener getMessageArrivingListener() {
        return messageArrivingListener;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public RemotingServer getFastRemotingServer() {
        return fastRemotingServer;
    }

    public Map<Class, AccessValidator> getAccessValidatorMap() {
        return accessValidatorMap;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public void setFastRemotingServer(RemotingServer fastRemotingServer) {
        this.fastRemotingServer = fastRemotingServer;
    }

    public PopServiceManager getPopServiceManager() {
        return popServiceManager;
    }

    public InetSocketAddress getStoreHost() {
        return storeHost;
    }

    public void setStoreHost(InetSocketAddress storeHost) {
        this.storeHost = storeHost;
    }

    public long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        final Runnable peek = q.peek();
        if (peek != null) {
            RequestTask rt = BrokerFastFailure.castRunnable(peek);
            slowTimeMills = rt == null ? 0 : TimeUtils.now() - rt.getCreateTimestamp();
        }

        if (slowTimeMills < 0) {
            slowTimeMills = 0;
        }

        return slowTimeMills;
    }

    public long headSlowTimeMills4SendThreadPoolQueue() {
        return this.headSlowTimeMills(this.sendThreadPoolQueue);
    }

    public long headSlowTimeMills4PullThreadPoolQueue() {
        return this.headSlowTimeMills(this.pullThreadPoolQueue);
    }

    public long headSlowTimeMills4LitePullThreadPoolQueue() {
        return this.headSlowTimeMills(this.litePullThreadPoolQueue);
    }

    public long headSlowTimeMills4QueryThreadPoolQueue() {
        return this.headSlowTimeMills(this.queryThreadPoolQueue);
    }

    public void printWaterMark() {
        LOG_WATER_MARK.info("[WATERMARK] Send Queue Size: {} SlowTimeMills: {}", this.sendThreadPoolQueue.size(), headSlowTimeMills4SendThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Pull Queue Size: {} SlowTimeMills: {}", this.pullThreadPoolQueue.size(), headSlowTimeMills4PullThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Query Queue Size: {} SlowTimeMills: {}", this.queryThreadPoolQueue.size(), headSlowTimeMills4QueryThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Lite Pull Queue Size: {} SlowTimeMills: {}", this.litePullThreadPoolQueue.size(), headSlowTimeMills4LitePullThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Transaction Queue Size: {} SlowTimeMills: {}", this.endTransactionThreadPoolQueue.size(), headSlowTimeMills(this.endTransactionThreadPoolQueue));
        LOG_WATER_MARK.info("[WATERMARK] ClientManager Queue Size: {} SlowTimeMills: {}", this.clientManagerThreadPoolQueue.size(), this.headSlowTimeMills(this.clientManagerThreadPoolQueue));
        LOG_WATER_MARK.info("[WATERMARK] Heartbeat Queue Size: {} SlowTimeMills: {}", this.heartbeatThreadPoolQueue.size(), this.headSlowTimeMills(this.heartbeatThreadPoolQueue));
        LOG_WATER_MARK.info("[WATERMARK] Ack Queue Size: {} SlowTimeMills: {}", this.ackThreadPoolQueue.size(), headSlowTimeMills(this.ackThreadPoolQueue));
        LOG_WATER_MARK.info("[WATERMARK] Admin Queue Size: {} SlowTimeMills: {}", this.adminBrokerThreadPoolQueue.size(), headSlowTimeMills(this.adminBrokerThreadPoolQueue));
    }

}
