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

package org.apache.rocketmq.common.domain;

import org.apache.rocketmq.common.domain.constant.MQConstants;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MQConstantsTest {

    @Test
    public void testIsLmq() {
        String testLmq = null;
        assertThat(MQConstants.isLmq(testLmq)).isFalse();
        testLmq = "lmq";
        assertThat(MQConstants.isLmq(testLmq)).isFalse();
        testLmq = "%LMQ%queue123";
        assertThat(MQConstants.isLmq(testLmq)).isTrue();
        testLmq = "%LMQ%GID_TEST";
        assertThat(MQConstants.isLmq(testLmq)).isTrue();
    }
}
