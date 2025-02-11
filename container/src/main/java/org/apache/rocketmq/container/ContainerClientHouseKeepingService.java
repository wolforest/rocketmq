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

import io.netty.channel.Channel;
import java.util.Collection;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.remoting.ChannelEventListener;

public class ContainerClientHouseKeepingService implements ChannelEventListener {
    private final IBrokerContainer brokerContainer;

    public ContainerClientHouseKeepingService(final IBrokerContainer brokerContainer) {
        this.brokerContainer = brokerContainer;
    }

    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {
        onChannelOperation(CallbackCode.CONNECT, remoteAddr, channel);
    }

    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        onChannelOperation(CallbackCode.CLOSE, remoteAddr, channel);
    }

    @Override
    public void onChannelException(String remoteAddr, Channel channel) {
        onChannelOperation(CallbackCode.EXCEPTION, remoteAddr, channel);
    }

    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {
        onChannelOperation(CallbackCode.IDLE, remoteAddr, channel);
    }

    @Override
    public void onChannelActive(String remoteAddr, Channel channel) {
        onChannelOperation(CallbackCode.ACTIVE, remoteAddr, channel);
    }

    private void onChannelOperation(CallbackCode callbackCode, String remoteAddr, Channel channel) {
        Collection<InnerBrokerController> masterBrokers = this.brokerContainer.getMasterBrokers();
        Collection<InnerSalveBrokerController> slaveBrokers = this.brokerContainer.getSlaveBrokers();

        for (Broker masterBroker : masterBrokers) {
            brokerOperation(masterBroker, callbackCode, remoteAddr, channel);
        }

        for (InnerSalveBrokerController slaveBroker : slaveBrokers) {
            brokerOperation(slaveBroker, callbackCode, remoteAddr, channel);
        }
    }

    private void brokerOperation(Broker broker, CallbackCode callbackCode, String remoteAddr,
        Channel channel) {
        if (callbackCode == CallbackCode.CONNECT) {
            broker.getBrokerStatsManager().incChannelConnectNum();
            return;
        }
        boolean removed = broker.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        removed &= broker.getConsumerManager().doChannelCloseEvent(remoteAddr, channel);
        if (removed) {
            switch (callbackCode) {
                case CLOSE:
                    broker.getBrokerStatsManager().incChannelCloseNum();
                    break;
                case EXCEPTION:
                    broker.getBrokerStatsManager().incChannelExceptionNum();
                    break;
                case IDLE:
                    broker.getBrokerStatsManager().incChannelIdleNum();
                    break;
                default:
                    break;
            }
        }
    }

    public enum CallbackCode {
        /**
         * onChannelConnect
         */
        CONNECT,
        /**
         * onChannelClose
         */
        CLOSE,
        /**
         * onChannelException
         */
        EXCEPTION,
        /**
         * onChannelIdle
         */
        IDLE,
        /**
         * onChannelActive
         */
        ACTIVE
    }
}
