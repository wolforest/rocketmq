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

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * @renamed from ConcurrentHashMapUtils to MapUtils
 */
public abstract class MapUtils {

    private static boolean isJdk8;

    static {
        // Java 8
        // Java 9+: 9,11,17
        try {
            isJdk8 = System.getProperty("java.version").startsWith("1.8.");
        } catch (Exception ignore) {
            isJdk8 = true;
        }
    }

    /**
     * A temporary workaround for Java 8 specific performance issue JDK-8161372 .<br> Use implementation of
     * ConcurrentMap.computeIfAbsent instead.
     *
     * Requirement: <strong>The mapping function should not modify this map during computation.</strong>
     *
     * @see <a href="https://bugs.openjdk.java.net/browse/JDK-8161372">https://bugs.openjdk.java.net/browse/JDK-8161372</a>
     */
    public static <K, V> V computeIfAbsent(ConcurrentMap<K, V> map, K key, Function<? super K, ? extends V> func) {
        Objects.requireNonNull(func);
        if (!isJdk8) {
            return map.computeIfAbsent(key, func);
        }

        V v = map.get(key);
        if (null != v) {
            return v;
        }

        // this bug fix methods maybe cause `func.apply` multiple calls.
        v = func.apply(key);
        if (null == v) {
            return null;
        }
        final V res = map.putIfAbsent(key, v);
        if (null != res) {
            // if pre value present, means other thread put value already, and putIfAbsent not effect
            // return exist value
            return res;
        }

        return v;

    }
}
