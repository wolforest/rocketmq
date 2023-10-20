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
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class StringUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.TOOLS_LOGGER_NAME);

    private final static char[] HEX_ARRAY;
    private final static int PID;

    static {
        HEX_ARRAY = "0123456789ABCDEF".toCharArray();
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

    public static List<String> split(String str, String splitter) {
        if (str == null) {
            return null;
        }

        String[] addrArray = str.split(splitter);
        return Arrays.asList(addrArray);
    }

    public static String bytes2string(byte[] src) {
        char[] hexChars = new char[src.length * 2];
        for (int j = 0; j < src.length; j++) {
            int v = src[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static byte[] string2bytes(String hexString) {
        if (hexString == null || hexString.equals("")) {
            return null;
        }
        hexString = hexString.toUpperCase();
        int length = hexString.length() / 2;
        char[] hexChars = hexString.toCharArray();
        byte[] d = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return d;
    }

    private static byte charToByte(char c) {
        return (byte) "0123456789ABCDEF".indexOf(c);
    }


    public static Long getLong(String s) {
        return getLong(s, 0);
    }

    public static Long getLong(String s, long defaultValue) {
        long v = defaultValue;
        try {
            v = Long.parseLong(s);
        } catch (Exception e) {
            LOGGER.error("GetLong error", e);
        }
        return v;

    }

    public static Integer getInt(String s) {
        return getInt(s, 0);
    }

    public static Integer getInt(String s, int defaultValue) {
        int v = defaultValue;
        try {
            v = Integer.parseInt(s);
        } catch (Exception e) {
            LOGGER.error("GetInt error", e);
        }
        return v;
    }

    public static String responseCode2String(final int code) {
        return Integer.toString(code);
    }

    public static String frontStringAtLeast(final String str, final int size) {
        if (str != null) {
            if (str.length() > size) {
                return str.substring(0, size);
            }
        }

        return str;
    }

    public static boolean isBlank(String str) {
        return org.apache.commons.lang3.StringUtils.isBlank(str);
    }

    public static String join(List<String> list, String splitter) {
        if (list == null) {
            return null;
        }

        StringBuilder str = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            str.append(list.get(i));
            if (i == list.size() - 1) {
                break;
            }
            str.append(splitter);
        }
        return str.toString();
    }

    public static void writeInt(char[] buffer, int pos, int value) {
        char[] hexArray = HEX_ARRAY;
        for (int moveBits = 28; moveBits >= 0; moveBits -= 4) {
            buffer[pos++] = hexArray[(value >>> moveBits) & 0x0F];
        }
    }

    public static void writeShort(char[] buffer, int pos, int value) {
        char[] hexArray = HEX_ARRAY;
        for (int moveBits = 12; moveBits >= 0; moveBits -= 4) {
            buffer[pos++] = hexArray[(value >>> moveBits) & 0x0F];
        }
    }

}
