package org.apache.rocketmq.apitest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.common.utils.StringUtils;

public class ConfigManager {
    protected static final String BASE_CONFIG = "api-test.json";
    private static final String DEFAULT_ACCOUNT = "default";
    private static JSONObject config;

    public static void init() throws Exception {
        config = loadConfig(BASE_CONFIG);
    }

    public static JSONObject getConfig() {
        return config;
    }

    public static ClientConfiguration buildClientConfig() {
        return buildClientConfig(DEFAULT_ACCOUNT);
    }

    public static ClientConfiguration buildClientConfig(String accountName) {
        JSONObject account = config.getJSONObject("accounts").getJSONObject(accountName);
        if (account == null) {
            throw new IllegalArgumentException("can't found account info: " + accountName);
        }

        SessionCredentialsProvider sessionCredentialsProvider =
            new StaticSessionCredentialsProvider(account.getString("accessKey"), account.getString("secretKey"));

        return ClientConfiguration.newBuilder()
            .setEndpoints(config.getString("endpoint"))
            .enableSsl(false)
            .setCredentialProvider(sessionCredentialsProvider)
            .build();
    }

    public static JSONObject loadConfig(String fileName) throws Exception {
        if (StringUtils.isBlank(fileName)) {
            throw new IllegalArgumentException("fileName can't be blank");
        }

        try (InputStream inputStream = ApiBaseTest.class.getClassLoader().getResourceAsStream(fileName)) {
            if (inputStream == null) {
                throw new IOException("config file not exists: " + fileName);
            }

            String fileData = CharStreams.toString(new InputStreamReader(inputStream, Charsets.UTF_8));
            return JSON.parseObject(fileData);
        }
    }

}
