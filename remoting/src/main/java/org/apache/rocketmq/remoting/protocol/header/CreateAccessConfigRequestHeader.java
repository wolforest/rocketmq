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

package org.apache.rocketmq.remoting.protocol.header;

import com.google.common.base.MoreObjects;
import org.apache.rocketmq.common.domain.action.Action;
import org.apache.rocketmq.common.domain.action.RocketMQAction;
import org.apache.rocketmq.common.domain.resource.ResourceType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@RocketMQAction(value = RequestCode.UPDATE_AND_CREATE_ACL_CONFIG, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class CreateAccessConfigRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String accessKey;

    private String secretKey;

    private String whiteRemoteAddress;

    private boolean admin;

    private String defaultTopicPerm;

    private String defaultGroupPerm;

    // list string,eg: topicA=DENY,topicD=SUB
    private String topicPerms;

    // list string,eg: groupD=DENY,groupD=SUB
    private String groupPerms;


    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getWhiteRemoteAddress() {
        return whiteRemoteAddress;
    }

    public void setWhiteRemoteAddress(String whiteRemoteAddress) {
        this.whiteRemoteAddress = whiteRemoteAddress;
    }

    public boolean isAdmin() {
        return admin;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    public String getDefaultTopicPerm() {
        return defaultTopicPerm;
    }

    public void setDefaultTopicPerm(String defaultTopicPerm) {
        this.defaultTopicPerm = defaultTopicPerm;
    }

    public String getDefaultGroupPerm() {
        return defaultGroupPerm;
    }

    public void setDefaultGroupPerm(String defaultGroupPerm) {
        this.defaultGroupPerm = defaultGroupPerm;
    }

    public String getTopicPerms() {
        return topicPerms;
    }

    public void setTopicPerms(String topicPerms) {
        this.topicPerms = topicPerms;
    }

    public String getGroupPerms() {
        return groupPerms;
    }

    public void setGroupPerms(String groupPerms) {
        this.groupPerms = groupPerms;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("accessKey", accessKey)
            .add("secretKey", secretKey)
            .add("whiteRemoteAddress", whiteRemoteAddress)
            .add("admin", admin)
            .add("defaultTopicPerm", defaultTopicPerm)
            .add("defaultGroupPerm", defaultGroupPerm)
            .add("topicPerms", topicPerms)
            .add("groupPerms", groupPerms)
            .toString();
    }
}
