package org.apache.rocketmq.apitest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.rocketmq.common.utils.StringUtils;
import org.junit.After;
import org.junit.Before;

public class ApiBaseTest {
    protected static final String BASE_CONFIG = "api-test.json";

    protected JSONObject config;

    @Before
    public void before() throws Throwable {
        config = loadConfig(BASE_CONFIG);
    }

    @After
    public void after() {

    }

    protected JSONObject loadConfig(String fileName) throws Exception {
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
