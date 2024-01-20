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
package org.apache.rocketmq.store.server;

import org.apache.rocketmq.store.api.service.KVService;
import org.apache.rocketmq.store.api.service.MessageService;
import org.apache.rocketmq.store.api.service.MonitorService;
import org.apache.rocketmq.store.api.service.QueueService;
import org.apache.rocketmq.store.server.config.StoreOption;

public class Store {
    private final StoreOption option;

    public Store(StoreOption option) {
        this.option = option;
    }

    /******************** server api  ************************/

    public boolean load() {
        return false;
    }

    public void start() {

    }

    public void shutdown() {

    }

    /******************** api service  ************************/

    public KVService getKVService() {
        return null;
    }

    public MessageService getMessageService() {
        return null;
    }

    public MonitorService getMonitorService() {
        return null;
    }

    public QueueService getQueueService() {
        return null;
    }
}
