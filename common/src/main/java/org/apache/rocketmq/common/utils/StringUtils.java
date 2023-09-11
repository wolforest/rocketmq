package org.apache.rocketmq.common.utils;

import java.util.Arrays;
import java.util.List;

public class StringUtils {

    public static List<String> split(String str, String splitter) {
        if (str == null) {
            return null;
        }

        String[] addrArray = str.split(splitter);
        return Arrays.asList(addrArray);
    }
}
