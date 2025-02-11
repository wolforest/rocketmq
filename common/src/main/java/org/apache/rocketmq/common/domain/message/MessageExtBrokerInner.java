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
package org.apache.rocketmq.common.domain.message;

import java.nio.ByteBuffer;

import org.apache.rocketmq.common.domain.topic.TopicFilterType;
import org.apache.rocketmq.common.utils.MessageUtils;

public class MessageExtBrokerInner extends MessageExt {
    private static final long serialVersionUID = 7256001576878700634L;

    /**
     * formatted properties string:
     * MessageDecoder.messageProperties2String(msgExt.getProperties())
     */
    private String propertiesString;

    /**
     * the hash code of tags
     */
    private long tagsCode;

    /**
     * ByteBuffer of encoded message,
     *   used by DefaultAppendMessageCallback
     * set by:
     *  - CommitLog.putMessageThreadLocal
     */
    private ByteBuffer encodedBuff;

    private volatile boolean encodeCompleted;

    /**
     * if topic.length() > Byte.MAX_VALUE,
     *      the message will be encoded to v2
     */
    private MessageVersion version = MessageVersion.MESSAGE_VERSION_V1;

    public ByteBuffer getEncodedBuff() {
        return encodedBuff;
    }

    public void setEncodedBuff(ByteBuffer encodedBuff) {
        this.encodedBuff = encodedBuff;
    }

    public static long tagsString2tagsCode(final TopicFilterType filter, final String tags) {
        if (null == tags || tags.isEmpty()) { return 0; }

        return tags.hashCode();
    }

    public static long tagsString2tagsCode(final String tags) {
        return tagsString2tagsCode(null, tags);
    }

    public String getPropertiesString() {
        return propertiesString;
    }

    public void setPropertiesString(String propertiesString) {
        this.propertiesString = propertiesString;
    }


    public void deleteProperty(String name) {
        super.clearProperty(name);
        if (propertiesString != null) {
            this.setPropertiesString(MessageUtils.deleteProperty(propertiesString, name));
        }
    }

    public long getTagsCode() {
        return tagsCode;
    }

    public void setTagsCode(long tagsCode) {
        this.tagsCode = tagsCode;
    }

    public MessageVersion getVersion() {
        return version;
    }

    public void setVersion(MessageVersion version) {
        this.version = version;
    }

    public void removeWaitStorePropertyString() {
        if (this.getProperties().containsKey(MessageConst.PROPERTY_WAIT_STORE_MSG_OK)) {
            // There is no need to store "WAIT=true", remove it from propertiesString to save 9 bytes for each message.
            // It works for most case. In some cases msgInner.setPropertiesString invoked later and replace it.
            String waitStoreMsgOKValue = this.getProperties().remove(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
            this.setPropertiesString(MessageDecoder.messageProperties2String(this.getProperties()));
            // Reput to properties, since msgInner.isWaitStoreMsgOK() will be invoked later
            this.getProperties().put(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, waitStoreMsgOKValue);
        } else {
            this.setPropertiesString(MessageDecoder.messageProperties2String(this.getProperties()));
        }
    }

    public boolean isEncodeCompleted() {
        return encodeCompleted;
    }

    public void setEncodeCompleted(boolean encodeCompleted) {
        this.encodeCompleted = encodeCompleted;
    }
}
