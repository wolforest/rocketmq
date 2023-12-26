package org.apache.rocketmq.apitest;

import org.junit.After;
import org.junit.Before;

public class ApiBaseTest {
    @Before
    public void before() throws Throwable {
        ConfigManager.init();
    }

    @After
    public void after() {

    }
}
