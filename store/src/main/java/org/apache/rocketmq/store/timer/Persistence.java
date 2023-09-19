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
package org.apache.rocketmq.store.timer;

import java.util.LinkedList;

public interface Persistence {
    class ScannResult {
        LinkedList<TimerRequest> normalMsgStack = new LinkedList<>();
        LinkedList<TimerRequest> deleteMsgStack = new LinkedList<>();
        int code = 0;

        public LinkedList<TimerRequest> getNormalMsgStack() {
            return normalMsgStack;
        }

        public LinkedList<TimerRequest> getDeleteMsgStack() {
            return deleteMsgStack;
        }

        public void setCode(int code){
            this.code = code;
        }
        public int getCode() {
            return code;
        }

        public void addDeleteMsgStack(TimerRequest timerRequest) {
            deleteMsgStack.add(timerRequest);
        }

        public void addNormalMsgStack(TimerRequest timerRequest) {
            normalMsgStack.addFirst(timerRequest);
        }

        public int sizeOfDeleteMsgStack() {
            return deleteMsgStack.size();
        }

        public int sizeOfNormalMsgStack() {
            return normalMsgStack.size();
        }
    }

    boolean save(TimerRequest timerRequest);
    ScannResult scan();
}
