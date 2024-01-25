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
package org.apache.rocketmq.broker.domain.coldctr;

public class SimpleColdCtrStrategy implements ColdCtrStrategy {
    private final ColdDataCgCtrThread coldDataCgCtrThread;

    public SimpleColdCtrStrategy(ColdDataCgCtrThread coldDataCgCtrThread) {
        this.coldDataCgCtrThread = coldDataCgCtrThread;
    }

    @Override
    public Double decisionFactor() {
        return null;
    }

    @Override
    public void promote(String consumerGroup, Long currentThreshold) {
        coldDataCgCtrThread.addOrUpdateGroupConfig(consumerGroup, (long)(currentThreshold * 1.5));
    }

    @Override
    public void decelerate(String consumerGroup, Long currentThreshold) {
        if (!coldDataCgCtrThread.isGlobalColdCtr()) {
            return;
        }
        long changedThresholdVal = (long)(currentThreshold * 0.8);
        if (changedThresholdVal < coldDataCgCtrThread.getBrokerConfig().getCgColdReadThreshold()) {
            changedThresholdVal = coldDataCgCtrThread.getBrokerConfig().getCgColdReadThreshold();
        }
        coldDataCgCtrThread.addOrUpdateGroupConfig(consumerGroup, changedThresholdVal);
    }

    @Override
    public void collect(Long globalAcc) {
    }
}
