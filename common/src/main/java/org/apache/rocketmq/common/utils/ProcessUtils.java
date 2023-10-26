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
