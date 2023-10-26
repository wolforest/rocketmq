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
import java.lang.management.RuntimeMXBean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class NetworkPortUtils {

    private final static int PID;
    static {
        Supplier<Integer> supplier = () -> {
            // format: "pid@hostname"
            String currentJVM = ManagementFactory.getRuntimeMXBean().getName();
            try {
                return Integer.parseInt(currentJVM.substring(0, currentJVM.indexOf('@')));
            } catch (Exception e) {
                return -1;
            }
        };
        PID = supplier.get();
    }
    private static AtomicInteger port = new AtomicInteger(uniquePort());

    public static int nextPort() {
        return port.addAndGet(5);
    }

    private static int uniquePort() {
        int minPort = 5000;
        int step = 500;
        int forkNumber = getForkNumber();
        if (forkNumber != 0) {
            return minPort + forkNumber * step;
        }
        int processId = getProcessID();
        // it's unreliable,Just for single run
        int firstNumber = processId;
        while (firstNumber >= 10) {
            firstNumber /= 10;
        }
        int lastNumber = processId % 10;
        return minPort + (firstNumber * 10 + lastNumber) * step;
    }

    private static int getForkNumber() {
        String forkNumberString = System.getProperty("forkNumber");
        if (forkNumberString == null) {
            return 0;
        }
        try {
            return Integer.parseInt(forkNumberString);
        } catch (Exception e) {
            return 0;
        }
    }

    public static int getProcessID() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        String strID = runtimeMXBean.getName().split("@")[0];
        return Integer.parseInt(strID);
    }

    public static int getPid() {
        return PID;
    }

}
