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
package org.apache.rocketmq.store.domain.timer.persistence;

import java.util.LinkedList;
import org.apache.rocketmq.store.domain.timer.model.TimerEvent;

public class ScanResult {
    LinkedList<TimerEvent> normalMsgStack = new LinkedList<>();
    LinkedList<TimerEvent> deleteMsgStack = new LinkedList<>();
    int code = 0;

    public LinkedList<TimerEvent> getNormalMsgStack() {
        return normalMsgStack;
    }

    public LinkedList<TimerEvent> getDeleteMsgStack() {
        return deleteMsgStack;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public void addDeleteMsgStack(TimerEvent timerEvent) {
        deleteMsgStack.add(timerEvent);
    }

    public void addNormalMsgStack(TimerEvent timerEvent) {
        normalMsgStack.addFirst(timerEvent);
    }

    public int sizeOfDeleteMsgStack() {
        return deleteMsgStack.size();
    }

    public int sizeOfNormalMsgStack() {
        return normalMsgStack.size();
    }
}
