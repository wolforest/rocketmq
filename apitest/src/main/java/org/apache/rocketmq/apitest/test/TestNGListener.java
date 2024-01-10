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
