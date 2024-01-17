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
package org.apache.rocketmq.store.api.broker.pop;

import java.util.concurrent.LinkedBlockingDeque;

public class QueueWithTime<T> {
    private final LinkedBlockingDeque<T> queue;
    private long time;

    public QueueWithTime() {
        this.queue = new LinkedBlockingDeque<>();
        this.time = System.currentTimeMillis();
    }

    public void setTime(long popTime) {
        this.time = popTime;
    }

    public long getTime() {
        return time;
    }

    public LinkedBlockingDeque<T> get() {
        return queue;
    }
}
