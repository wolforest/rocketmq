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
package org.apache.rocketmq.common.utils;

import java.lang.management.ManagementFactory;
import org.apache.commons.lang3.StringUtils;

public class ProcessUtils {
    public static final long CURRENT_JVM_PID = getPID();

    public static long getPID() {
        String processName = ManagementFactory.getRuntimeMXBean().getName();
        if (StringUtils.isEmpty(processName)) {
            return 0;
        }

        try {
            return Long.parseLong(processName.split("@")[0]);
        } catch (Exception e) {
            return 0;
        }
    }

}
