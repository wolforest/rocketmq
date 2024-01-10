package org.apache.rocketmq.apitest.test;

import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;


public class Retry implements IRetryAnalyzer {
    private int retryCount = 0;
    private int retryLimit = 1;

    public boolean retry(ITestResult result) {
        if (retryCount < retryLimit) {
            retryCount++;
            return true;
        }
        return false;
    }

    public void reset() {
        retryCount = 0;
    }
}
