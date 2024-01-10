package org.apache.rocketmq.apitest.test;

import java.util.Iterator;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

public class TestNGListener extends TestListenerAdapter {

    @Override
    public void onTestSuccess(ITestResult iTestResult) {
        super.onTestSuccess(iTestResult);

        //对于dataProvider的用例，每次成功后，重置Retry次数
//        Retry retry = (Retry) iTestResult.getMethod().getRetryAnalyzer(iTestResult);
//        retry.reset();
    }

    @Override
    public void onTestFailure(ITestResult iTestResult) {
        super.onTestFailure(iTestResult);

        //对于dataProvider的用例，每次成功后，重置Retry次数
//        Retry retry = (Retry) iTestResult.getMethod().getRetryAnalyzer(iTestResult);
//        retry.reset();
    }

    @Override
    public void onFinish(ITestContext iTestContext) {
        super.onFinish(iTestContext);
        Iterator<ITestResult> skippTestResults = iTestContext.getSkippedTests().getAllResults().iterator();
        while (skippTestResults.hasNext()) {
            ITestResult skippResult = skippTestResults.next();
            ITestNGMethod method = skippResult.getMethod();
            if (iTestContext.getFailedTests().getResults(method).size() > 0 || iTestContext.getPassedTests().getResults(method).size() > 0) {
                skippTestResults.remove();
            }
        }
    }
}
