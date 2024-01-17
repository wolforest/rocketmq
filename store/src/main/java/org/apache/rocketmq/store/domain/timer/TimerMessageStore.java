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
package org.apache.rocketmq.store.domain.timer;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.rocketmq.common.lang.thread.ThreadFactoryImpl;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.common.domain.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.common.utils.TimeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.server.DefaultMessageStore;
import org.apache.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.store.api.dto.PutMessageResult;
import org.apache.rocketmq.store.server.config.MessageStoreConfig;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.Slot;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerLog;
import org.apache.rocketmq.store.domain.timer.persistence.wheel.TimerWheel;
import org.apache.rocketmq.store.domain.timer.transit.TimerDequeueWarmService;
import org.apache.rocketmq.store.domain.timer.transit.TimerFlushService;
import org.apache.rocketmq.store.domain.timer.transit.TimerMessageAccepter;
import org.apache.rocketmq.store.domain.timer.transit.TimerMessageDeliver;
import org.apache.rocketmq.store.domain.timer.transit.TimerMessageQuery;
import org.apache.rocketmq.store.domain.timer.transit.TimerMessageRecover;
import org.apache.rocketmq.store.domain.timer.transit.TimerMessageSaver;
import org.apache.rocketmq.store.domain.timer.transit.TimerMessageScanner;
import org.apache.rocketmq.store.infra.file.SelectMappedBufferResult;
import org.apache.rocketmq.store.domain.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.api.broker.stats.BrokerStatsManager;
import org.apache.rocketmq.store.server.metrics.PerfCounter;

/**
 * timer server
 * 1. manager timer server start/shutdown
 * 2. timer context <- to move to TimerContext
 * 3. timer matrix gateway <- used by external services
 *
 * @renamed from TimerMessageStore to TimerServer
 */
public class TimerMessageStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public static final int DEFAULT_CAPACITY = 1024;
    public static final String ENQUEUE_PUT = "enqueue_put";
    public static final String DEQUEUE_PUT = "dequeue_put";
    // The total days in the timer wheel when precision is 1000ms.
    // If the broker shutdown last more than the configured days, will cause message loss
    public static final int TIMER_WHEEL_TTL_DAY = 7;
    public static final int DAY_SECS = 24 * 3600;

    public static final Random RANDOM = new Random();
    public boolean debug = false;
    protected final PerfCounter.Ticks perfCounterTicks = new PerfCounter.Ticks(LOGGER);

    /**
     * wait to schedule task queue
     * the task will schedule by timer wheel
     * or put into timerMessageDeliverQueue
     */
    protected BlockingQueue<TimerRequest> fetchedTimerMessageQueue;
    /**
     * wait to execute message queue
     * the message in queue will put back to commitLog
     */
    protected BlockingQueue<TimerRequest> timerMessageDeliverQueue;
    /**
     *
     */
    protected BlockingQueue<List<TimerRequest>> timerMessageQueryQueue;

    private ScheduledExecutorService scheduler;
    private final TimerState timerState;
    private final MessageStore messageStore;
    private final TimerWheel timerWheel;
    private final TimerLog timerLog;
    private final TimerCheckpoint timerCheckpoint;
    private TimerMessageAccepter timerMessageFetcher;
    private TimerMessageSaver timerMessageLocator;
    private TimerDequeueWarmService dequeueWarmService;
    private TimerMessageScanner timerMessageScanner;
    private TimerMessageDeliver[] timerMessageDelivers;
    private TimerMessageQuery[] timerMessageQueries;
    private TimerFlushService timerFlushService;
    private final int commitLogFileSize;
    private final int timerLogFileSize;
    protected final int precisionMs;
    protected final MessageStoreConfig storeConfig;
    protected TimerMetrics timerMetrics;
    protected long lastTimeOfCheckMetrics = System.currentTimeMillis();
    private final BrokerStatsManager brokerStatsManager;
    private Function<MessageExtBrokerInner, PutMessageResult> escapeBridgeHook;
    private final MessageOperator messageOperator;
    private final TimerMetricManager timerMetricManager;
    private TimerMessageRecover recover;

    public TimerMessageStore(final MessageStore messageStore, final MessageStoreConfig storeConfig,
                             TimerCheckpoint timerCheckpoint, TimerMetrics timerMetrics,
                             final BrokerStatsManager brokerStatsManager) throws IOException {
        this.messageStore = messageStore;
        this.storeConfig = storeConfig;
        this.timerMetrics = timerMetrics;
        this.timerCheckpoint = timerCheckpoint;
        this.brokerStatsManager = brokerStatsManager;
        this.commitLogFileSize = storeConfig.getMappedFileSizeCommitLog();
        this.timerLogFileSize = storeConfig.getMappedFileSizeTimerLog();
        this.precisionMs = storeConfig.getTimerPrecisionMs();

        initQueues(storeConfig);
        initScheduler(messageStore);

        this.timerLog = new TimerLog(getTimerLogPath(storeConfig.getStorePathRootDir()), timerLogFileSize);
        final int slotsTotal = TIMER_WHEEL_TTL_DAY * DAY_SECS;
        final String timeWheelFileName = getTimerWheelFileFullName(storeConfig.getStorePathRootDir());
        this.timerWheel = new TimerWheel(timeWheelFileName, slotsTotal, precisionMs);
        this.timerState = new TimerState(timerCheckpoint, storeConfig, timerLog, slotsTotal, timerWheel, messageStore);
        this.messageOperator = new MessageOperator(messageStore, storeConfig);
        this.timerMetricManager = new TimerMetricManager(timerState, storeConfig, timerWheel, timerLog, messageOperator, timerMetrics);
    }

    public boolean load() {
        this.initService();
        boolean load = timerLog.load();
        load = load && this.timerMetrics.load();
        recover.recover();
        calcTimerDistribution();
        return load;
    }

    public void start(boolean shouldRunningDequeue) {
        this.timerState.setShouldRunningDequeue(shouldRunningDequeue);
        this.start();
    }

    public void start() {
        final long shouldStartTime = storeConfig.getDisappearTimeAfterStart() + System.currentTimeMillis();
        timerState.maybeMoveWriteTime();
        timerMessageFetcher.start();
        timerMessageLocator.start();
        dequeueWarmService.start();
        timerMessageScanner.start(shouldStartTime);

        startTimerMessageQueries();
        startTimerMessageDelivers();
        timerFlushService.start();
        scheduleCleanLogService();
        scheduleCheckAndReviseService();

        timerState.flagRunning();
        LOGGER.info("Timer start ok currReadTimerMs:[{}] queueOffset:[{}]", new Timestamp(timerState.currReadTimeMs), timerState.currQueueOffset);
    }

    public void shutdown() {
        if (timerState.isShutdown()) {
            return;
        }
        timerState.flagShutdown();
        //first save checkpoint
        timerState.prepareTimerCheckPoint();
        timerFlushService.shutdown();
        timerLog.shutdown();
        timerCheckpoint.shutdown();

        fetchedTimerMessageQueue.clear(); //avoid blocking
        timerMessageQueryQueue.clear(); //avoid blocking
        timerMessageDeliverQueue.clear(); //avoid blocking

        timerMessageFetcher.shutdown();
        timerMessageLocator.shutdown();
        dequeueWarmService.shutdown();
        timerMessageScanner.shutdown();
        shutdownTimerMessageQueries();
        shutdownTimerMessageDelivers();

        timerWheel.shutdown(false);
        this.scheduler.shutdown();
    }

    public long getCongestNum(long deliverTimeMs) {
        return timerWheel.getNum(deliverTimeMs);
    }

    public boolean isReject(long deliverTimeMs) {
        long congestNum = timerWheel.getNum(deliverTimeMs);
        if (congestNum <= storeConfig.getTimerCongestNumEachSlot()) {
            return false;
        }
        if (congestNum >= storeConfig.getTimerCongestNumEachSlot() * 2L) {
            return true;
        }
        if (RANDOM.nextInt(1000) > 1000 * (congestNum - storeConfig.getTimerCongestNumEachSlot()) / (storeConfig.getTimerCongestNumEachSlot() + 0.1)) {
            return true;
        }
        return false;
    }

    public long getEnqueueBehindMessages() {
        long tmpQueueOffset = timerState.currQueueOffset;
        ConsumeQueueInterface cq = messageStore.getConsumeQueue(TimerState.TIMER_TOPIC, 0);
        long maxOffsetInQueue = cq == null ? 0 : cq.getMaxOffsetInQueue();
        return maxOffsetInQueue - tmpQueueOffset;
    }

    public long getEnqueueBehindMillis() {
        long ts = System.currentTimeMillis();
        if (ts - timerState.lastEnqueueButExpiredTime < 2000) {
            return (ts - timerState.lastEnqueueButExpiredStoreTime) / 1000;
        }
        return 0;
    }

    public long getEnqueueBehind() {
        return getEnqueueBehindMillis() / 1000;
    }

    public long getDequeueBehindMessages() {
        return timerWheel.getAllNum(timerState.currReadTimeMs);
    }

    public float getEnqueueTps() {
        return perfCounterTicks.getCounter(ENQUEUE_PUT).getLastTps();
    }

    public float getDequeueTps() {
        return perfCounterTicks.getCounter("dequeue_put").getLastTps();
    }

    public void registerEscapeBridgeHook(Function<MessageExtBrokerInner, PutMessageResult> escapeBridgeHook) {
        this.escapeBridgeHook = escapeBridgeHook;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public int warmDequeue() {
        if (!timerState.isRunningDequeue()) {
            return -1;
        }
        if (!storeConfig.isTimerWarmEnable()) {
            return -1;
        }
        if (timerState.preReadTimeMs <= timerState.currReadTimeMs) {
            timerState.preReadTimeMs = timerState.currReadTimeMs + precisionMs;
        }
        if (timerState.preReadTimeMs >= timerState.currWriteTimeMs) {
            return -1;
        }
        if (timerState.preReadTimeMs >= timerState.currReadTimeMs + 3L * precisionMs) {
            return -1;
        }
        Slot slot = timerWheel.getSlot(timerState.preReadTimeMs);
        if (-1 == slot.timeMs) {
            timerState.preReadTimeMs = timerState.preReadTimeMs + precisionMs;
            return 0;
        }
        long currOffsetPy = slot.lastPos;
        LinkedList<SelectMappedBufferResult> sbrs = new LinkedList<>();
        SelectMappedBufferResult timeSbr = null;
        SelectMappedBufferResult msgSbr = null;
        try {
            //read the msg one by one
            while (currOffsetPy != -1) {
                if (!timerState.isRunning()) {
                    break;
                }
                perfCounterTicks.startTick("warm_dequeue");
                if (null == timeSbr || timeSbr.getStartOffset() > currOffsetPy) {
                    timeSbr = timerLog.getWholeBuffer(currOffsetPy);
                    if (null != timeSbr) {
                        sbrs.add(timeSbr);
                    }
                }
                if (null == timeSbr) {
                    break;
                }
                long prevPos = -1;
                try {
                    int position = (int) (currOffsetPy % timerLogFileSize);
                    timeSbr.getByteBuffer().position(position);
                    timeSbr.getByteBuffer().getInt(); //size
                    prevPos = timeSbr.getByteBuffer().getLong();
                    timeSbr.getByteBuffer().position(position + TimerLog.UNIT_PRE_SIZE_FOR_MSG);
                    long offsetPy = timeSbr.getByteBuffer().getLong();
                    int sizePy = timeSbr.getByteBuffer().getInt();
                    if (null == msgSbr || msgSbr.getStartOffset() > offsetPy) {
                        msgSbr = messageStore.getCommitLogData(offsetPy - offsetPy % commitLogFileSize);
                        if (null != msgSbr) {
                            sbrs.add(msgSbr);
                        }
                    }
                    if (null != msgSbr) {
                        ByteBuffer bf = msgSbr.getByteBuffer();
                        int firstPos = (int) (offsetPy % commitLogFileSize);
                        for (int pos = firstPos; pos < firstPos + sizePy; pos += 4096) {
                            bf.position(pos);
                            bf.get();
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Unexpected error in warm", e);
                } finally {
                    currOffsetPy = prevPos;
                    perfCounterTicks.endTick("warm_dequeue");
                }
            }
            for (SelectMappedBufferResult sbr : sbrs) {
                if (null != sbr) {
                    sbr.release();
                }
            }
        } finally {
            timerState.preReadTimeMs = timerState.preReadTimeMs + precisionMs;
        }
        return 1;
    }

    private void startTimerMessageQueries() {
        for (TimerMessageQuery query : timerMessageQueries) {
            query.start();
        }
    }

    private void startTimerMessageDelivers() {
        for (TimerMessageDeliver deliver : timerMessageDelivers) {
            deliver.start();
        }
    }

    private void scheduleCleanLogService() {
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    long minPy = messageStore.getMinPhyOffset();
                    int checkOffset = timerLog.getOffsetForLastUnit();
                    timerLog.getMappedFileQueue()
                        .deleteExpiredFileByOffsetForTimerLog(minPy, checkOffset, TimerLog.UNIT_SIZE);
                } catch (Exception e) {
                    LOGGER.error("Error in cleaning timerLog", e);
                }
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    private void scheduleCheckAndReviseService() {
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (!storeConfig.isTimerEnableCheckMetrics()) {
                        return;
                    }

                    String when = storeConfig.getTimerCheckMetricsWhen();
                    if (!TimeUtils.isItTimeToDo(when)) {
                        return;
                    }

                    long curr = System.currentTimeMillis();
                    if (curr - lastTimeOfCheckMetrics > 70 * 60 * 1000) {
                        lastTimeOfCheckMetrics = curr;
                        timerMetricManager.checkAndReviseMetrics();
                        LOGGER.info("[CheckAndReviseMetrics]Timer do check timer metrics cost {} ms",
                            System.currentTimeMillis() - curr);
                    }
                } catch (Exception e) {
                    LOGGER.error("Error in cleaning timerLog", e);
                }
            }
        }, 45, 45, TimeUnit.MINUTES);
    }

    private void shutdownTimerMessageQueries() {
        for (TimerMessageQuery query : timerMessageQueries) {
            query.shutdown();
        }
    }

    private void shutdownTimerMessageDelivers() {
        for (TimerMessageDeliver deliver : timerMessageDelivers) {
            deliver.shutdown();
        }
    }

    private void initService() {
        initTimerMessageQuery();
        initTimerMessageDeliver();

        timerMessageFetcher = new TimerMessageAccepter(timerState, storeConfig, messageOperator,
            fetchedTimerMessageQueue, perfCounterTicks);

        timerMessageLocator = new TimerMessageSaver(
                timerState,
                storeConfig,
                timerWheel,
                timerLog,
                messageOperator,
                fetchedTimerMessageQueue,
                timerMessageDeliverQueue,
                timerMessageDelivers,
                timerMessageQueries,
                timerMetricManager,
                perfCounterTicks);
        dequeueWarmService = new TimerDequeueWarmService(timerState);
        timerMessageScanner = new TimerMessageScanner(
                timerState,
                storeConfig,
                timerWheel,
                timerLog,
                timerMessageQueryQueue,
                timerMessageDeliverQueue,
                timerMessageDelivers,
                timerMessageQueries,
                timerMetricManager,
                perfCounterTicks
        );
        timerFlushService = new TimerFlushService(
                timerState,
                storeConfig,
                messageStore,
                timerWheel,
                timerLog,
                fetchedTimerMessageQueue,
                timerMessageQueryQueue,
                timerMessageDeliverQueue,
                timerMetrics
        );
        recover = new TimerMessageRecover(
                timerState,
                timerWheel,
                timerLog,
                messageOperator,
                timerCheckpoint);

    }

    private void initTimerMessageQuery() {
        int getThreadNum = Math.max(storeConfig.getTimerGetMessageThreadNum(), 1);
        timerMessageQueries = new TimerMessageQuery[getThreadNum];
        for (int i = 0; i < timerMessageQueries.length; i++) {
            timerMessageQueries[i] = new TimerMessageQuery(
                timerState,
                storeConfig,
                messageOperator,
                timerMessageDeliverQueue,
                timerMessageQueryQueue,
                perfCounterTicks);
        }
    }

    private void initTimerMessageDeliver() {
        int putThreadNum = Math.max(storeConfig.getTimerPutMessageThreadNum(), 1);
        timerMessageDelivers = new TimerMessageDeliver[putThreadNum];
        for (int i = 0; i < timerMessageDelivers.length; i++) {
            timerMessageDelivers[i] = new TimerMessageDeliver(
                timerState,
                storeConfig,
                messageOperator,
                timerMessageDeliverQueue,
                brokerStatsManager,
                timerMetricManager,
                escapeBridgeHook,
                perfCounterTicks);
        }
    }

    private void initScheduler(MessageStore messageStore) {
        if (messageStore instanceof DefaultMessageStore) {
            this.scheduler = ThreadUtils.newSingleThreadScheduledExecutor(
                    new ThreadFactoryImpl("TimerScheduledThread",
                            ((DefaultMessageStore) messageStore).getBrokerIdentity()));
        } else {
            scheduler = ThreadUtils.newSingleThreadScheduledExecutor(
                    new ThreadFactoryImpl("TimerScheduledThread"));
        }
    }

    private void initQueues(MessageStoreConfig storeConfig) {
        if (storeConfig.isTimerEnableDisruptor()) {
            fetchedTimerMessageQueue = new DisruptorBlockingQueue<>(DEFAULT_CAPACITY);
            timerMessageQueryQueue = new DisruptorBlockingQueue<>(DEFAULT_CAPACITY);
            timerMessageDeliverQueue = new DisruptorBlockingQueue<>(DEFAULT_CAPACITY);
        } else {
            fetchedTimerMessageQueue = new LinkedBlockingDeque<>(DEFAULT_CAPACITY);
            timerMessageQueryQueue = new LinkedBlockingDeque<>(DEFAULT_CAPACITY);
            timerMessageDeliverQueue = new LinkedBlockingDeque<>(DEFAULT_CAPACITY);
        }
    }

    private void calcTimerDistribution() {
        long startTime = System.currentTimeMillis();
        List<Integer> timerDist = this.timerMetrics.getTimerDistList();
        long currTime = System.currentTimeMillis() / precisionMs * precisionMs;
        for (int i = 0; i < timerDist.size(); i++) {
            int slotBeforeNum = i == 0 ? 0 : timerDist.get(i - 1) * 1000 / precisionMs;
            int slotTotalNum = timerDist.get(i) * 1000 / precisionMs;
            int periodTotal = 0;
            for (int j = slotBeforeNum; j < slotTotalNum; j++) {
                Slot slotEach = timerWheel.getSlot(currTime + (long) j * precisionMs);
                periodTotal += slotEach.num;
            }
            LOGGER.debug("{} period's total num: {}", timerDist.get(i), periodTotal);
            this.timerMetrics.updateDistPair(timerDist.get(i), periodTotal);
        }
        long endTime = System.currentTimeMillis();
        LOGGER.debug("Total cost Time: {}", endTime - startTime);
    }

    private String getTimerLogPath(final String rootDir) {
        return rootDir + File.separator + "timerlog";
    }

    private String getTimerWheelFileFullName(final String rootDir) {
        return rootDir + File.separator + "timerwheel";
    }

    public TimerState getTimerState() {
        return this.timerState;
    }

    public long getCurrReadTimeMs() {
        return this.timerState.currReadTimeMs;
    }

    public long getQueueOffset() {
        return timerState.currQueueOffset;
    }

    public long getCommitQueueOffset() {
        return this.timerState.commitQueueOffset;
    }

    public long getCommitReadTimeMs() {
        return this.timerState.commitReadTimeMs;
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public TimerWheel getTimerWheel() {
        return timerWheel;
    }

    public TimerLog getTimerLog() {
        return timerLog;
    }

    public TimerMetrics getTimerMetrics() {
        return this.timerMetrics;
    }
}
