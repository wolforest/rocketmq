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
package org.apache.rocketmq.common.app.config;

import java.io.IOException;
import java.util.Map;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.utils.IOUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.Statistics;

public abstract class ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    protected RocksDBConfigManager rocksDBConfigManager;

    public boolean load() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = IOUtils.file2String(fileName);

            if (null == jsonString || jsonString.length() == 0) {
                return this.loadBak();
            } else {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " failed, and try to load backup file", e);
            return this.loadBak();
        }
    }

    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = IOUtils.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " Failed", e);
            return false;
        }

        return true;
    }

    public synchronized <T> void persist(String topicName, T t) {
        // stub for future
        this.persist();
    }

    public synchronized <T> void persist(Map<String, T> m) {
        // stub for future
        this.persist();
    }

    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString == null) {
            return;
        }

        String fileName = this.configFilePath();
        try {
            IOUtils.string2File(jsonString, fileName);
        } catch (IOException e) {
            log.error("persist file " + fileName + " exception", e);
        }
    }

    protected void decode0(final byte[] key, final byte[] body) {

    }

    public boolean stop() {
        return true;
    }

    public abstract String configFilePath();

    public abstract String encode();

    public abstract String encode(final boolean prettyFormat);

    public abstract void decode(final String jsonString);

    public Statistics getStatistics() {
        return rocksDBConfigManager == null ? null : rocksDBConfigManager.getStatistics();
    }
}
