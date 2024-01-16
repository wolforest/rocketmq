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
package org.apache.rocketmq.broker.server.daemon;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class SystemConfigFileHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private String file;

    public SystemConfigFileHelper() {
    }

    public Properties loadConfig() throws Exception {
        InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(file)));
        Properties properties = new Properties();
        properties.load(in);
        in.close();
        return properties;
    }

    public void update(Properties properties) throws Exception {
        LOGGER.error("[SystemConfigFileHelper] update no thing.");
    }

    public void setFile(String file) {
        this.file = file;
    }

    public String getFile() {
        return file;
    }
}