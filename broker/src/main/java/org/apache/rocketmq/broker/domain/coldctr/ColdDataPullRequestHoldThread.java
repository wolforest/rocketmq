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
package org.apache.rocketmq.broker.domain.coldctr;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.rocketmq.broker.api.controller.PullMessageProcessor;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.connection.longpolling.PullRequest;
import org.apache.rocketmq.common.lang.thread.ServiceThread;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.utils.TimeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @renamed from ColdDataPullRequestHoldService to ColdDataPullRequestHoldThread
 * @see PullMessageProcessor just for PullMessageProcessor
 *
 * just requests are type of pull have the qualification to be put into this hold queue.
 * if the pull request is reading cold data and that request will be cold at the first time,
 * then the pull request will be cold in this @code pullRequestLinkedBlockingQueue,
 * in @code coldTimeoutMillis later the pull request will be warm and marked hold
 */
public class ColdDataPullRequestHoldThread extends ServiceThread {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_COLDCTR_LOGGER_NAME);
    public static final String NO_SUSPEND_KEY = "_noSuspend_";

    private final long coldHoldTimeoutMillis = 3000;
    private final Broker broker;
    private final LinkedBlockingQueue<PullRequest> pullRequestColdHoldQueue = new LinkedBlockingQueue<>(10000);

    public void suspendColdDataReadRequest(PullRequest pullRequest) {
        if (this.broker.getMessageStoreConfig().isColdDataFlowControlEnable()) {
            pullRequestColdHoldQueue.offer(pullRequest);
        }
    }

    public ColdDataPullRequestHoldThread(Broker broker) {
        this.broker = broker;
    }

    @Override
    public String getServiceName() {
        return ColdDataPullRequestHoldThread.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                if (!this.broker.getMessageStoreConfig().isColdDataFlowControlEnable()) {
                    this.waitForRunning(20 * 1000);
                } else {
                    this.waitForRunning(5 * 1000);
                }

                long beginClockTimestamp = TimeUtils.now();
                this.checkColdDataPullRequest();
                long costTime = TimeUtils.now() - beginClockTimestamp;
                log.info("[{}] checkColdDataPullRequest-cost {} ms.", costTime > 5 * 1000 ? "NOTIFYME" : "OK", costTime);
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception", e);
            }
        }
        log.info("{} service end", this.getServiceName());
    }

    private void checkColdDataPullRequest() {
        int succTotal = 0, errorTotal = 0, queueSize = pullRequestColdHoldQueue.size() ;
        Iterator<PullRequest> iterator = pullRequestColdHoldQueue.iterator();
        while (iterator.hasNext()) {
            PullRequest pullRequest = iterator.next();
            if (System.currentTimeMillis() < pullRequest.getSuspendTimestamp() + coldHoldTimeoutMillis) {
                continue;
            }

            try {
                pullRequest.getRequestCommand().addExtField(NO_SUSPEND_KEY, "1");
                this.broker.getBrokerNettyServer().executePullRequest(pullRequest.getClientChannel(), pullRequest.getRequestCommand());
                succTotal++;
            } catch (Exception e) {
                log.error("PullRequestColdHoldService checkColdDataPullRequest error", e);
                errorTotal++;
            }
            //remove the timeout request from the iterator
            iterator.remove();
        }

        log.info("checkColdPullRequest-info-finish, queueSize: {} successTotal: {} errorTotal: {}", queueSize, succTotal, errorTotal);
    }

}