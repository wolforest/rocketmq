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

public interface ColdCtrStrategy {
    /**
     * Calculate the determining factor about whether to accelerate or decelerate
     * @return factor
     */
    Double decisionFactor();
    /**
     * Promote the speed for consumerGroup to read cold data
     * @param consumerGroup consumerGroup
     * @param currentThreshold currentThreshold
     */
    void promote(String consumerGroup, Long currentThreshold);
    /**
     * Decelerate the speed for consumerGroup to read cold data
     * @param consumerGroup consumerGroup
     * @param currentThreshold currentThreshold
     */
    void decelerate(String consumerGroup, Long currentThreshold);
    /**
     * Collect the total number of cold read data in the system
     * @param globalAcc globalAcc
     */
    void collect(Long globalAcc);
}
