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
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Settings;
import com.google.protobuf.util.Durations;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.function.Consumer;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.domain.constant.ConsumeInitMode;
import org.apache.rocketmq.common.domain.message.MessageConst;
import org.apache.rocketmq.common.domain.message.MessageExt;
import org.apache.rocketmq.common.utils.StringUtils;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.QueueSelector;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueSelector;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

public class ReceiveMessageActivity extends AbstractMessingActivity {
    private static final String ILLEGAL_POLLING_TIME_INTRODUCED_CLIENT_VERSION = "5.0.3";

    public ReceiveMessageActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    /**
     *
     * @param ctx ctx
     * @param request
     *          request.invisible_duration =>
     *          Required if client type is simple consumer.
     *          useless for PushConsumer
     *
     * @param responseObserver responseObserver
     */
    public void receiveMessage(ProxyContext ctx, ReceiveMessageRequest request, StreamObserver<ReceiveMessageResponse> responseObserver) {
        ReceiveMessageResponseStreamWriter writer = createWriter(responseObserver);

        try {
            Settings settings = this.grpcClientSettingsManager.getClientSettings(ctx);
            Long pollingTime = getPollingTime(ctx, request, settings, writer);
            if (pollingTime == null) {
                return;
            }

            validateTopicAndConsumerGroup(request.getMessageQueue().getTopic(), request.getGroup());
            String topic = request.getMessageQueue().getTopic().getName();
            String group = request.getGroup().getName();

            long actualInvisibleTime = getInvisibleTime(request);
            SubscriptionData subscriptionData = getSubscriptionData(ctx, request, topic, writer);
            if (subscriptionData == null) {
                return;
            }

            this.messagingProcessor.popMessage(
                    ctx,
                    new ReceiveMessageQueueSelector(
                        request.getMessageQueue().getBroker().getName()
                    ),
                    group,
                    topic,
                    request.getBatchSize(),
                    actualInvisibleTime,
                    pollingTime,
                    ConsumeInitMode.MAX,
                    subscriptionData,
                    settings.getSubscription().getFifo(),
                    new PopMessageResultFilterImpl(settings.getBackoffPolicy().getMaxAttempts()),
                    request.hasAttemptId() ? request.getAttemptId() : null,
                    ctx.getRemainingMs()
                ).thenAccept(createPopCallback(ctx, request, group, topic, writer))
                .exceptionally(t -> {
                    writer.writeAndComplete(ctx, request, t);
                    return null;
                });

        } catch (Throwable t) {
            writer.writeAndComplete(ctx, request, t);
        }
    }

    protected ReceiveMessageResponseStreamWriter createWriter(StreamObserver<ReceiveMessageResponse> responseObserver) {
        return new ReceiveMessageResponseStreamWriter(
            this.messagingProcessor,
            responseObserver
        );
    }

    protected static class ReceiveMessageQueueSelector implements QueueSelector {

        private final String brokerName;

        public ReceiveMessageQueueSelector(String brokerName) {
            this.brokerName = brokerName;
        }

        @Override
        public AddressableMessageQueue select(ProxyContext ctx, MessageQueueView messageQueueView) {
            try {
                AddressableMessageQueue addressableMessageQueue = null;
                MessageQueueSelector messageQueueSelector = messageQueueView.getReadSelector();

                if (StringUtils.isNotBlank(brokerName)) {
                    addressableMessageQueue = messageQueueSelector.getQueueByBrokerName(brokerName);
                }

                if (addressableMessageQueue == null) {
                    addressableMessageQueue = messageQueueSelector.selectOne(true);
                }
                return addressableMessageQueue;
            } catch (Throwable t) {
                return null;
            }
        }
    }

    private Long getPollingTime(ProxyContext ctx, ReceiveMessageRequest request, Settings settings, ReceiveMessageResponseStreamWriter writer) {
        long pollingTime;
        Long timeRemaining = ctx.getRemainingMs();
        ProxyConfig config = ConfigurationManager.getProxyConfig();

        if (request.hasLongPollingTimeout()) {
            pollingTime = Durations.toMillis(request.getLongPollingTimeout());
        } else {
            pollingTime = timeRemaining - Durations.toMillis(settings.getRequestTimeout()) / 2;
        }
        if (pollingTime < config.getGrpcClientConsumerMinLongPollingTimeoutMillis()) {
            pollingTime = config.getGrpcClientConsumerMinLongPollingTimeoutMillis();
        }
        if (pollingTime > config.getGrpcClientConsumerMaxLongPollingTimeoutMillis()) {
            pollingTime = config.getGrpcClientConsumerMaxLongPollingTimeoutMillis();
        }

        if (pollingTime > timeRemaining) {
            if (timeRemaining >= config.getGrpcClientConsumerMinLongPollingTimeoutMillis()) {
                pollingTime = timeRemaining;
            } else {
                notEnoughDeadLineTime(ctx, writer);
                return null;
            }
        }

        return pollingTime;
    }

    private void notEnoughDeadLineTime(ProxyContext ctx, ReceiveMessageResponseStreamWriter writer) {
        final String clientVersion = ctx.getClientVersion();
        Code code = null == clientVersion || ILLEGAL_POLLING_TIME_INTRODUCED_CLIENT_VERSION.compareTo(clientVersion) > 0
            ? Code.BAD_REQUEST
            : Code.ILLEGAL_POLLING_TIME;

        writer.writeAndComplete(ctx, code, "The deadline time remaining is not enough" +
            " for polling, please check network condition");
    }

    /**
     * get invisibleTime:
     * for PushConsumer: invisibleTime = proxyConfig.defaultInvisibleTimeMills
     *
     * @param request request
     * @return invisibleTime
     */
    private long getInvisibleTime(ReceiveMessageRequest request) {
        long actualInvisibleTime = Durations.toMillis(request.getInvisibleDuration());
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        if (proxyConfig.isEnableProxyAutoRenew() && request.getAutoRenew()) {
            actualInvisibleTime = proxyConfig.getDefaultInvisibleTimeMills();
        } else {
            validateInvisibleTime(actualInvisibleTime, ConfigurationManager.getProxyConfig().getMinInvisibleTimeMillsForRecv());
        }

        return actualInvisibleTime;
    }

    private SubscriptionData getSubscriptionData(ProxyContext ctx, ReceiveMessageRequest request, String topic, ReceiveMessageResponseStreamWriter writer) {
        FilterExpression filterExpression = request.getFilterExpression();

        try {
            return FilterAPI.build(topic, filterExpression.getExpression(),
                GrpcConverter.getInstance().buildExpressionType(filterExpression.getType()));
        } catch (Exception e) {
            writer.writeAndComplete(ctx, Code.ILLEGAL_FILTER_EXPRESSION, e.getMessage());
            return null;
        }

    }

    private Consumer<PopResult> createPopCallback(ProxyContext ctx, ReceiveMessageRequest request, String group, String topic, ReceiveMessageResponseStreamWriter writer) {
        return  popResult -> {
            if (!ConfigurationManager.getProxyConfig().isEnableProxyAutoRenew() || !request.getAutoRenew()) {
                writer.writeAndComplete(ctx, request, popResult);
                return;
            }

            if (!PopStatus.FOUND.equals(popResult.getPopStatus())) {
                writer.writeAndComplete(ctx, request, popResult);
                return;
            }

            List<MessageExt> messageExtList = popResult.getMsgFoundList();
            for (MessageExt messageExt : messageExtList) {
                String receiptHandle = messageExt.getProperty(MessageConst.PROPERTY_POP_CK);
                if (receiptHandle == null) {
                    continue;
                }

                MessageReceiptHandle messageReceiptHandle =
                    new MessageReceiptHandle(group, topic, messageExt.getQueueId(), receiptHandle, messageExt.getMsgId(),
                        messageExt.getQueueOffset(), messageExt.getReconsumeTimes());
                messagingProcessor.addReceiptHandle(ctx, grpcChannelManager.getChannel(ctx.getClientID()), group, messageExt.getMsgId(), messageReceiptHandle);
            }
            writer.writeAndComplete(ctx, request, popResult);
        };
    }
}
