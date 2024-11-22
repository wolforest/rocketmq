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

package org.apache.rocketmq.proxy.config;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.utils.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.auth.config.AuthConfig;

public class Configuration {
    private final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final AtomicReference<ProxyConfig> proxyConfigReference = new AtomicReference<>();
    private final AtomicReference<AuthConfig> authConfigReference = new AtomicReference<>();
    public static final String CONFIG_PATH_PROPERTY = "com.rocketmq.proxy.configPath";

    public void init() throws Exception {
        String proxyConfigData = loadJsonConfig();

        ProxyConfig proxyConfig = JSON.parseObject(proxyConfigData, ProxyConfig.class);
        proxyConfig.initData();
        setProxyConfig(proxyConfig);

        AuthConfig authConfig = JSON.parseObject(proxyConfigData, AuthConfig.class);
        setAuthConfig(authConfig);
        authConfig.setConfigName(proxyConfig.getProxyName());
        authConfig.setClusterName(proxyConfig.getRocketMQClusterName());
    }

    private static String loadJsonConfig() throws Exception {
        String configFileName = ProxyConfig.DEFAULT_CONFIG_FILE_NAME;
        String filePath = System.getProperty(CONFIG_PATH_PROPERTY);

        if (StringUtils.isBlank(filePath)) {
            String testConfig = loadTestConfig(configFileName);
            if (testConfig != null) {
                return testConfig;
            }
        }

        if (StringUtils.isBlank(filePath)) {
            filePath = new File(ConfigurationManager.getProxyHome() + File.separator + "conf", configFileName).toString();
        }

        return loadFileConfig(filePath);
    }

    private static String loadTestConfig(String configFileName) throws IOException {
        final String testResource = "rmq-proxy-home/conf/" + configFileName;
        try (InputStream inputStream = Configuration.class.getClassLoader().getResourceAsStream(testResource)) {
            if (null != inputStream) {
                return CharStreams.toString(new InputStreamReader(inputStream, Charsets.UTF_8));
            }
        }

        return null;
    }

    private static String loadFileConfig(String filePath) throws IOException {
        File file = new File(filePath);
        log.info("The current configuration file path is {}", filePath);

        if (!file.exists()) {
            log.warn("the config file {} not exist", filePath);
            throw new RuntimeException(String.format("the config file %s not exist", filePath));
        }

        long fileLength = file.length();
        if (fileLength <= 0) {
            log.warn("the config file {} length is zero", filePath);
            throw new RuntimeException(String.format("the config file %s length is zero", filePath));
        }

        return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
    }

    public ProxyConfig getProxyConfig() {
        return proxyConfigReference.get();
    }

    public void setProxyConfig(ProxyConfig proxyConfig) {
        proxyConfigReference.set(proxyConfig);
    }

    public AuthConfig getAuthConfig() {
        return authConfigReference.get();
    }

    public void setAuthConfig(AuthConfig authConfig) {
        authConfigReference.set(authConfig);
    }
}
