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

package org.apache.rocketmq.remoting.protocol.header.controller.register;

import org.apache.rocketmq.common.domain.action.Action;
import org.apache.rocketmq.common.domain.action.RocketMQAction;
import org.apache.rocketmq.common.domain.resource.ResourceType;
import org.apache.rocketmq.common.domain.resource.RocketMQResource;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@RocketMQAction(value = RequestCode.CONTROLLER_REGISTER_BROKER, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class RegisterBrokerToControllerRequestHeader implements CommandCustomHeader {

    @RocketMQResource(ResourceType.CLUSTER)
    private String clusterName;

    private String brokerName;

    private Long brokerId;

    private String brokerAddress;

    private long invokeTime;

    public RegisterBrokerToControllerRequestHeader() {
    }

    public RegisterBrokerToControllerRequestHeader(String clusterName, String brokerName, Long brokerId, String brokerAddress) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerId = brokerId;
        this.brokerAddress = brokerAddress;
        this.invokeTime = System.currentTimeMillis();
    }

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public long getInvokeTime() {
        return invokeTime;
    }

    public void setInvokeTime(long invokeTime) {
        this.invokeTime = invokeTime;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public Long getBrokerId() {
        return brokerId;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public void setBrokerId(Long brokerId) {
        this.brokerId = brokerId;
    }

    public void setBrokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
    }
}
