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

package org.apache.rocketmq.proxy;

public enum ProxyMode {
    LOCAL("LOCAL"),
    /**
     * CLUSTER mode is not recommended
     * - it's hard to add proxy instance
     * - ... ...
     */
    CLUSTER("CLUSTER");

    private final String mode;

    ProxyMode(String mode) {
        this.mode = mode;
    }

    public static boolean isValid(String mode) {
        return isClusterMode(mode) || isLocalMode(mode);
    }

    public static boolean isClusterMode(String mode) {
        if (mode == null) {
            return false;
        }
        return CLUSTER.mode.equals(mode.toUpperCase());
    }

    public static boolean isClusterMode(ProxyMode mode) {
        if (mode == null) {
            return false;
        }
        return CLUSTER.equals(mode);
    }

    public static boolean isLocalMode(String mode) {
        if (mode == null) {
            return false;
        }
        return LOCAL.mode.equals(mode.toUpperCase());
    }

    public static boolean isLocalMode(ProxyMode mode) {
        if (mode == null) {
            return false;
        }
        return LOCAL.equals(mode);
    }
}
