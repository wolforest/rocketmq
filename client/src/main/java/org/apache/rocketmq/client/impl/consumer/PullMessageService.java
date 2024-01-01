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
package org.apache.rocketmq.client.impl.consumer;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.lang.thread.ServiceThread;
import org.apache.rocketmq.common.lang.thread.ThreadFactoryImpl;
import org.apache.rocketmq.common.domain.message.MessageRequestMode;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class PullMessageService extends ServiceThread {
    private final Logger logger = LoggerFactory.getLogger(PullMessageService.class);
    private final LinkedBlockingQueue<MessageRequest> messageRequestQueue = new LinkedBlockingQueue<>();

    private final MQClientInstance mQClientFactory;
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("PullMessageServiceScheduledThread"));

    public PullMessageService(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        if (isStopped()) {
            logger.warn("PullMessageServiceScheduledThread has shutdown");
            return;
        }

        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                PullMessageService.this.executePullRequestImmediately(pullRequest);
            }
        }, timeDelay, TimeUnit.MILLISECONDS);
    }

    public void executePullRequestImmediately(final PullRequest pullRequest) {
        try {
            this.messageRequestQueue.put(pullRequest);
        } catch (InterruptedException e) {
            logger.error("executePullRequestImmediately pullRequestQueue.put", e);
        }
    }

    public void executePopPullRequestLater(final PopRequest popRequest, final long timeDelay) {
        if (isStopped()) {
            logger.warn("PullMessageServiceScheduledThread has shutdown");
            return;
        }

        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                PullMessageService.this.executePopPullRequestImmediately(popRequest);
            }
        }, timeDelay, TimeUnit.MILLISECONDS);
    }

    public void executePopPullRequestImmediately(final PopRequest popRequest) {
        try {
            this.messageRequestQueue.put(popRequest);
        } catch (InterruptedException e) {
            logger.error("executePullRequestImmediately pullRequestQueue.put", e);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        if (isStopped()) {
            logger.warn("PullMessageServiceScheduledThread has shutdown");
            return;
        }

        this.scheduledExecutorService.schedule(r, timeDelay, TimeUnit.MILLISECONDS);
    }

    public void executeTask(final Runnable r) {
        if (!isStopped()) {
            this.scheduledExecutorService.execute(r);
        } else {
            logger.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    private void pullMessage(final PullRequest pullRequest) {
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(pullRequest.getConsumerGroup());
        if (consumer == null) {
            logger.warn("No matched consumer for the PullRequest {}, drop it", pullRequest);
            return;
        }

        DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
        impl.pullMessage(pullRequest);
    }

    private void popMessage(final PopRequest popRequest) {
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(popRequest.getConsumerGroup());
        if (consumer == null) {
            logger.warn("No matched consumer for the PopRequest {}, drop it", popRequest);
            return;
        }

        DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
        impl.popMessage(popRequest);
    }

    @Override
    public void run() {
        logger.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            pullOrPopMessage();
        }

        logger.info(this.getServiceName() + " service end");
    }

    private void pullOrPopMessage() {
        try {
            MessageRequest messageRequest = this.messageRequestQueue.take();
            if (messageRequest.getMessageRequestMode() == MessageRequestMode.POP) {
                this.popMessage((PopRequest) messageRequest);
            } else {
                this.pullMessage((PullRequest) messageRequest);
            }
        } catch (InterruptedException ignored) {
        } catch (Exception e) {
            logger.error("Pull Message Service Run Method exception", e);
        }
    }

    @Override
    public void shutdown(boolean interrupt) {
        super.shutdown(interrupt);
        ThreadUtils.shutdownGracefully(this.scheduledExecutorService, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }

}
