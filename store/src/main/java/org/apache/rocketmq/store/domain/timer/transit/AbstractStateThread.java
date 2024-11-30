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
package org.apache.rocketmq.store.domain.timer.transit;

import org.apache.rocketmq.common.lang.thread.ServiceThread;

/**
 * @renamed from AbstractStateService to AbstractStateThread
 */
public abstract class AbstractStateThread extends ServiceThread {
    public static final int INITIAL = -1, START = 0, WAITING = 1, RUNNING = 2, END = 3;
    protected int state = INITIAL;

    public void setState(int state) {
        this.state = state;
    }

    public boolean isState(int state) {
        return this.state == state;
    }
}
