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
package org.apache.rocketmq.broker.server.daemon.schedule;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class HandlePutResultTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long DELAY_FOR_A_SLEEP = 10L;

    private final int delayLevel;

    private final ScheduleMessageService scheduleMessageService;

    public HandlePutResultTask(ScheduleMessageService scheduleMessageService, int delayLevel) {
        this.scheduleMessageService = scheduleMessageService;
        this.delayLevel = delayLevel;
    }

    @Override
    public void run() {
        LinkedBlockingQueue<PutResultProcess> pendingQueue =
            scheduleMessageService.getDeliverPendingTable().get(this.delayLevel);

        PutResultProcess putResultProcess;
        while ((putResultProcess = pendingQueue.peek()) != null) {
            try {
                switch (putResultProcess.getStatus()) {
                    case SUCCESS:
                        scheduleMessageService.updateOffset(this.delayLevel, putResultProcess.getNextOffset());
                        pendingQueue.remove();
                        break;
                    case RUNNING:
                        scheduleNextTask();
                        return;
                    case EXCEPTION:
                        if (!scheduleMessageService.isStarted()) {
                            log.warn("HandlePutResultTask shutdown, info={}", putResultProcess.toString());
                            return;
                        }
                        log.warn("putResultProcess error, info={}", putResultProcess.toString());
                        putResultProcess.doResend();
                        break;
                    case SKIP:
                        log.warn("putResultProcess skip, info={}", putResultProcess.toString());
                        pendingQueue.remove();
                        break;
                }
            } catch (Exception e) {
                log.error("HandlePutResultTask exception. info={}", putResultProcess.toString(), e);
                putResultProcess.doResend();
            }
        }

        scheduleNextTask();
    }

    private void scheduleNextTask() {
        if (scheduleMessageService.isStarted()) {
            scheduleMessageService.getHandleExecutorService()
                .schedule(new HandlePutResultTask(this.scheduleMessageService, this.delayLevel), DELAY_FOR_A_SLEEP, TimeUnit.MILLISECONDS);
        }
    }
}

