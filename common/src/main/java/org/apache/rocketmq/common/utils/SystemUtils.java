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
import java.util.function.Supplier;

public class SystemUtils {
    public static final String OS_NAME = System.getProperty("os.name");
    private static final String OS = System.getProperty("os.name").toLowerCase();
    private static int pid;
    static boolean isLinuxPlatform = false;
    static boolean isWindowsPlatform = false;

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
        SystemUtils.pid = supplier.get();
    }

    public static boolean isWindows() {
        return OS.contains("win");
    }

    public static boolean isMac() {
        return OS.contains("mac");
    }

    public static boolean isUnix() {
        return OS.contains("nix")
                || OS.contains("nux")
                || OS.indexOf("aix") > 0;
    }

    public static boolean isSolaris() {
        return OS.contains("sunos");
    }

    public static int getPid() {
        return pid;
    }

    public static boolean isWindowsPlatform() {
        return isWindowsPlatform;
    }

    public static boolean isLinuxPlatform() {
        return isLinuxPlatform;
    }

}
