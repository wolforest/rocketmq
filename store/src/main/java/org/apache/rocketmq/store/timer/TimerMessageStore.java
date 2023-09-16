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
package org.apache.rocketmq.store.timer;

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

import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.TimeUtils;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.queue.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.logfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.timer.service.MessageReader;
import org.apache.rocketmq.store.timer.service.TimerMessageQuery;
import org.apache.rocketmq.store.timer.service.TimerMetricManager;
import org.apache.rocketmq.store.timer.service.TimerWheelFetcher;
import org.apache.rocketmq.store.timer.service.TimerMessageDeliver;
import org.apache.rocketmq.store.timer.service.TimerDequeueWarmService;
import org.apache.rocketmq.store.timer.service.TimerMessageFetcher;
import org.apache.rocketmq.store.timer.service.TimerWheelLocator;
import org.apache.rocketmq.store.timer.service.TimerFlushService;
import org.apache.rocketmq.store.util.PerfCounter;

public class TimerMessageStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);


    public final TimerState timerState;

    public static final String TIMER_TOPIC = TopicValidator.SYSTEM_TOPIC_PREFIX + "wheel_timer";


    public static final Random RANDOM = new Random();

    public static final int DEFAULT_CAPACITY = 1024;


    public boolean debug = false;

    public static final String ENQUEUE_PUT = "enqueue_put";
    public static final String DEQUEUE_PUT = "dequeue_put";
    protected final PerfCounter.Ticks perfCounterTicks = new PerfCounter.Ticks(LOGGER);


    protected BlockingQueue<TimerRequest> fetchedTimerMessageQueue;
    protected BlockingQueue<List<TimerRequest>> timerMessageQueryQueue;
    protected BlockingQueue<TimerRequest> timerMessageDeliverQueue;


    private final ScheduledExecutorService scheduler;

    private final MessageStore messageStore;
    private final TimerWheel timerWheel;
    private final TimerLog timerLog;
    private final TimerCheckpoint timerCheckpoint;

    private TimerMessageFetcher timerMessageFetcher;
    private TimerWheelLocator timerWheelLocator;
    private TimerDequeueWarmService dequeueWarmService;
    private TimerWheelFetcher timerWheelFetcher;
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
    private MessageReader messageReader;
    private TimerMetricManager timerMetricManager;

    public TimerMessageStore(final MessageStore messageStore, final MessageStoreConfig storeConfig,
                             TimerCheckpoint timerCheckpoint, TimerMetrics timerMetrics,
                             final BrokerStatsManager brokerStatsManager) throws IOException {
        initQueues(storeConfig);

        this.messageStore = messageStore;
        this.storeConfig = storeConfig;
        this.commitLogFileSize = storeConfig.getMappedFileSizeCommitLog();
        this.timerLogFileSize = storeConfig.getMappedFileSizeTimerLog();
        this.precisionMs = storeConfig.getTimerPrecisionMs();


        this.timerLog = new TimerLog(getTimerLogPath(storeConfig.getStorePathRootDir()), timerLogFileSize);
        this.timerMetrics = timerMetrics;

        this.timerCheckpoint = timerCheckpoint;
        this.timerState = new TimerState(timerCheckpoint, storeConfig, timerLog, messageStore);
        this.timerWheel = new TimerWheel(
                getTimerWheelFileFullName(storeConfig.getStorePathRootDir()), timerState.slotsTotal, precisionMs);
        timerMetricManager = new TimerMetricManager(timerMetrics, storeConfig, messageReader, timerWheel, timerLog, timerState);
        if (messageStore instanceof DefaultMessageStore) {
            scheduler = ThreadUtils.newSingleThreadScheduledExecutor(
                    new ThreadFactoryImpl("TimerScheduledThread",
                            ((DefaultMessageStore) messageStore).getBrokerIdentity()));
        } else {
            scheduler = ThreadUtils.newSingleThreadScheduledExecutor(
                    new ThreadFactoryImpl("TimerScheduledThread"));
        }


        this.messageReader = new MessageReader(messageStore, storeConfig);


        this.brokerStatsManager = brokerStatsManager;
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

    public void initService() {
        int getThreadNum = Math.max(storeConfig.getTimerGetMessageThreadNum(), 1);
        timerMessageQueries = new TimerMessageQuery[getThreadNum];
        for (int i = 0; i < timerMessageQueries.length; i++) {
            timerMessageQueries[i] = new TimerMessageQuery(timerState, timerCheckpoint, messageReader, perfCounterTicks, storeConfig, timerMessageDeliverQueue, timerMessageQueryQueue, getServiceThreadName());
        }

        int putThreadNum = Math.max(storeConfig.getTimerPutMessageThreadNum(), 1);
        timerMessageDelivers = new TimerMessageDeliver[putThreadNum];
        for (int i = 0; i < timerMessageDelivers.length; i++) {
            timerMessageDelivers[i] = new TimerMessageDeliver(timerMetricManager, timerMessageDeliverQueue, perfCounterTicks, timerState,
                    storeConfig, getServiceThreadName(), brokerStatsManager, escapeBridgeHook, messageStore);
        }
        timerMessageFetcher = new TimerMessageFetcher(fetchedTimerMessageQueue, storeConfig, perfCounterTicks, messageReader, messageStore, timerState, timerCheckpoint, getServiceThreadName());
        timerWheelLocator = new TimerWheelLocator(storeConfig, timerWheel, timerLog, timerMetricManager,
                fetchedTimerMessageQueue, timerMessageDeliverQueue,
                timerMessageDelivers, timerMessageQueries, timerState, perfCounterTicks, getServiceThreadName());
        dequeueWarmService = new TimerDequeueWarmService(this);
        timerWheelFetcher = new TimerWheelFetcher(storeConfig, timerState, timerWheel, timerLog, perfCounterTicks, getServiceThreadName(),
                timerMessageQueryQueue, timerMessageDeliverQueue,
                timerMessageDelivers, timerMessageQueries);
        timerFlushService = new TimerFlushService(this,messageStore,fetchedTimerMessageQueue,timerMessageQueryQueue,timerMessageDeliverQueue,
                storeConfig,timerState,timerMetrics,timerCheckpoint,timerLog,timerWheel);


    }

    public boolean load() {
        this.initService();
        boolean load = timerLog.load();
        load = load && this.timerMetrics.load();
        recover();
        calcTimerDistribution();
        return load;
    }

    public static String getTimerWheelFileFullName(final String rootDir) {
        return rootDir + File.separator + "timerwheel";
    }

    public static String getTimerLogPath(final String rootDir) {
        return rootDir + File.separator + "timerlog";
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

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public void recover() {
        //recover timerLog
        long lastFlushPos = timerCheckpoint.getLastTimerLogFlushPos();
        MappedFile lastFile = timerLog.getMappedFileQueue().getLastMappedFile();
        if (null != lastFile) {
            lastFlushPos = lastFlushPos - lastFile.getFileSize();
        }
        if (lastFlushPos < 0) {
            lastFlushPos = 0;
        }
        long processOffset = recoverAndRevise(lastFlushPos, true);

        timerLog.getMappedFileQueue().setFlushedWhere(processOffset);
        //revise queue offset
        long queueOffset = reviseQueueOffset(processOffset);
        if (-1 == queueOffset) {
            timerState.currQueueOffset = timerCheckpoint.getLastTimerQueueOffset();
        } else {
            timerState.currQueueOffset = queueOffset + 1;
        }
        timerState.currQueueOffset = Math.min(timerState.currQueueOffset, timerCheckpoint.getMasterTimerQueueOffset());

        //check timer wheel
        timerState.currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
        long nextReadTimeMs = formatTimeMs(
                System.currentTimeMillis()) - (long) timerState.slotsTotal * precisionMs + (long) TimerState.TIMER_BLANK_SLOTS * precisionMs;
        if (timerState.currReadTimeMs < nextReadTimeMs) {
            timerState.currReadTimeMs = nextReadTimeMs;
        }
        //the timer wheel may contain physical offset bigger than timerLog
        //This will only happen when the timerLog is damaged
        //hard to test
        long minFirst = timerWheel.checkPhyPos(timerState.currReadTimeMs, processOffset);
        if (debug) {
            minFirst = 0;
        }
        if (minFirst < processOffset) {
            LOGGER.warn("Timer recheck because of minFirst:{} processOffset:{}", minFirst, processOffset);
            recoverAndRevise(minFirst, false);
        }
        LOGGER.info("Timer recover ok currReadTimerMs:{} currQueueOffset:{} checkQueueOffset:{} processOffset:{}",
                timerState.currReadTimeMs, timerState.currQueueOffset, timerCheckpoint.getLastTimerQueueOffset(), processOffset);

        timerState.commitReadTimeMs = timerState.currReadTimeMs;
        timerState.commitQueueOffset = timerState.currQueueOffset;

        timerState.prepareTimerCheckPoint();
    }

    public long reviseQueueOffset(long processOffset) {
        SelectMappedBufferResult selectRes = timerLog.getTimerMessage(processOffset - (TimerLog.UNIT_SIZE - TimerLog.UNIT_PRE_SIZE_FOR_MSG));
        if (null == selectRes) {
            return -1;
        }
        try {
            long offsetPy = selectRes.getByteBuffer().getLong();
            int sizePy = selectRes.getByteBuffer().getInt();
            MessageExt messageExt = messageReader.getMessageByCommitOffset(offsetPy, sizePy);
            if (null == messageExt) {
                return -1;
            }

            // check offset in msg is equal to offset of cq.
            // if not, use cq offset.
            long msgQueueOffset = messageExt.getQueueOffset();
            int queueId = messageExt.getQueueId();
            ConsumeQueue cq = (ConsumeQueue) this.messageStore.getConsumeQueue(TIMER_TOPIC, queueId);
            if (null == cq) {
                return msgQueueOffset;
            }
            long cqOffset = msgQueueOffset;
            long tmpOffset = msgQueueOffset;
            int maxCount = 20000;
            while (maxCount-- > 0) {
                if (tmpOffset < 0) {
                    LOGGER.warn("reviseQueueOffset check cq offset fail, msg in cq is not found.{}, {}",
                            offsetPy, sizePy);
                    break;
                }
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(tmpOffset);
                if (null == bufferCQ) {
                    // offset in msg may be greater than offset of cq.
                    tmpOffset -= 1;
                    continue;
                }
                try {
                    long offsetPyTemp = bufferCQ.getByteBuffer().getLong();
                    int sizePyTemp = bufferCQ.getByteBuffer().getInt();
                    if (offsetPyTemp == offsetPy && sizePyTemp == sizePy) {
                        LOGGER.info("reviseQueueOffset check cq offset ok. {}, {}, {}",
                                tmpOffset, offsetPyTemp, sizePyTemp);
                        cqOffset = tmpOffset;
                        break;
                    }
                    tmpOffset -= 1;
                } catch (Throwable e) {
                    LOGGER.error("reviseQueueOffset check cq offset error.", e);
                } finally {
                    bufferCQ.release();
                }
            }

            return cqOffset;
        } finally {
            selectRes.release();
        }
    }

    //recover timerLog and revise timerWheel
    //return process offset
    private long recoverAndRevise(long beginOffset, boolean checkTimerLog) {
        LOGGER.info("Begin to recover timerLog offset:{} check:{}", beginOffset, checkTimerLog);
        MappedFile lastFile = timerLog.getMappedFileQueue().getLastMappedFile();
        if (null == lastFile) {
            return 0;
        }

        List<MappedFile> mappedFiles = timerLog.getMappedFileQueue().getMappedFiles();
        int index = mappedFiles.size() - 1;
        for (; index >= 0; index--) {
            MappedFile mappedFile = mappedFiles.get(index);
            if (beginOffset >= mappedFile.getFileFromOffset()) {
                break;
            }
        }
        if (index < 0) {
            index = 0;
        }
        long checkOffset = mappedFiles.get(index).getFileFromOffset();
        for (; index < mappedFiles.size(); index++) {
            MappedFile mappedFile = mappedFiles.get(index);
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0, checkTimerLog ? mappedFiles.get(index).getFileSize() : mappedFile.getReadPosition());
            ByteBuffer bf = sbr.getByteBuffer();
            int position = 0;
            boolean stopCheck = false;
            for (; position < sbr.getSize(); position += TimerLog.UNIT_SIZE) {
                try {
                    bf.position(position);
                    int size = bf.getInt();//size
                    bf.getLong();//prev pos
                    int magic = bf.getInt();
                    if (magic == TimerLog.BLANK_MAGIC_CODE) {
                        break;
                    }
                    if (checkTimerLog && (!TimerState.isMagicOK(magic) || TimerLog.UNIT_SIZE != size)) {
                        stopCheck = true;
                        break;
                    }
                    long delayTime = bf.getLong() + bf.getInt();
                    if (TimerLog.UNIT_SIZE == size && TimerState.isMagicOK(magic)) {
                        timerWheel.reviseSlot(delayTime, TimerWheel.IGNORE, sbr.getStartOffset() + position, true);
                    }
                } catch (Exception e) {
                    LOGGER.error("Recover timerLog error", e);
                    stopCheck = true;
                    break;
                }
            }
            sbr.release();
            checkOffset = mappedFiles.get(index).getFileFromOffset() + position;
            if (stopCheck) {
                break;
            }
        }
        if (checkTimerLog) {
            timerLog.getMappedFileQueue().truncateDirtyFiles(checkOffset);
        }
        return checkOffset;
    }

    public void start() {
        final long shouldStartTime = storeConfig.getDisappearTimeAfterStart() + System.currentTimeMillis();
        timerState.maybeMoveWriteTime();
        timerMessageFetcher.start();
        timerWheelLocator.start();
        dequeueWarmService.start();
        timerWheelFetcher.start(shouldStartTime);
        for (int i = 0; i < timerMessageQueries.length; i++) {
            timerMessageQueries[i].start();
        }
        for (int i = 0; i < timerMessageDelivers.length; i++) {
            timerMessageDelivers[i].start();
        }
        timerFlushService.start();

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

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (storeConfig.isTimerEnableCheckMetrics()) {
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
                    }
                } catch (Exception e) {
                    LOGGER.error("Error in cleaning timerLog", e);
                }
            }
        }, 45, 45, TimeUnit.MINUTES);

        timerState.flagRunning();
        LOGGER.info("Timer start ok currReadTimerMs:[{}] queueOffset:[{}]", new Timestamp(timerState.currReadTimeMs), timerState.currQueueOffset);
    }

    public void start(boolean shouldRunningDequeue) {
        this.timerState.shouldRunningDequeue = shouldRunningDequeue;
        this.start();
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
        timerWheelLocator.shutdown();
        dequeueWarmService.shutdown();
        timerWheelFetcher.shutdown();
        for (int i = 0; i < timerMessageQueries.length; i++) {
            timerMessageQueries[i].shutdown();
        }
        for (int i = 0; i < timerMessageDelivers.length; i++) {
            timerMessageDelivers[i].shutdown();
        }
        timerWheel.shutdown(false);

        this.scheduler.shutdown();

    }


    public void setShouldRunningDequeue(final boolean shouldRunningDequeue) {
        this.timerState.shouldRunningDequeue = shouldRunningDequeue;
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


    private long formatTimeMs(long timeMs) {
        return timeMs / precisionMs * precisionMs;
    }


    public String getServiceThreadName() {
        String brokerIdentifier = "";
        if (TimerMessageStore.this.messageStore instanceof DefaultMessageStore) {
            DefaultMessageStore messageStore = (DefaultMessageStore) TimerMessageStore.this.messageStore;
            if (messageStore.getBrokerConfig().isInBrokerContainer()) {
                brokerIdentifier = messageStore.getBrokerConfig().getIdentifier();
            }
        }
        return brokerIdentifier;
    }


    public long getAllCongestNum() {
        return timerWheel.getAllNum(timerState.currReadTimeMs);
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
        ConsumeQueue cq = (ConsumeQueue) messageStore.getConsumeQueue(TIMER_TOPIC, 0);
        long maxOffsetInQueue = cq == null ? 0 : cq.getMaxOffsetInQueue();
        return maxOffsetInQueue - tmpQueueOffset;
    }

    public long getEnqueueBehindMillis() {
        if (System.currentTimeMillis() - timerState.lastEnqueueButExpiredTime < 2000) {
            return (System.currentTimeMillis() - timerState.lastEnqueueButExpiredStoreTime) / 1000;
        }
        return 0;
    }

    public long getEnqueueBehind() {
        return getEnqueueBehindMillis() / 1000;
    }

    public long getDequeueBehindMessages() {
        return timerWheel.getAllNum(timerState.currReadTimeMs);
    }

    public long getDequeueBehindMillis() {
        return System.currentTimeMillis() - timerState.currReadTimeMs;
    }

    public long getDequeueBehind() {
        return getDequeueBehindMillis() / 1000;
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

    public int getPrecisionMs() {
        return precisionMs;
    }

    public TimerMessageFetcher getTimerMessageFetcher() {
        return timerMessageFetcher;
    }

    public void setTimerMessageFetcher(TimerMessageFetcher timerMessageFetcher) {
        this.timerMessageFetcher = timerMessageFetcher;
    }

    public TimerWheelLocator getTimerWheelLocator() {
        return timerWheelLocator;
    }

    public void setTimerWheelLocator(TimerWheelLocator timerWheelLocator) {
        this.timerWheelLocator = timerWheelLocator;
    }

    public TimerDequeueWarmService getDequeueWarmService() {
        return dequeueWarmService;
    }

    public void setDequeueWarmService(
            TimerDequeueWarmService dequeueWarmService) {
        this.dequeueWarmService = dequeueWarmService;
    }

    public TimerWheelFetcher getTimerWheelFetcher() {
        return timerWheelFetcher;
    }

    public void setTimerWheelFetcher(TimerWheelFetcher timerWheelFetcher) {
        this.timerWheelFetcher = timerWheelFetcher;
    }

    public TimerMessageDeliver[] getTimerMessageDelivers() {
        return timerMessageDelivers;
    }

    public void setTimerMessageDelivers(
            TimerMessageDeliver[] timerMessageDelivers) {
        this.timerMessageDelivers = timerMessageDelivers;
    }

    public TimerMessageQuery[] getTimerMessageQueries() {
        return timerMessageQueries;
    }

    public void setTimerMessageQueries(
            TimerMessageQuery[] timerMessageQueries) {
        this.timerMessageQueries = timerMessageQueries;
    }


    public TimerCheckpoint getTimerCheckpoint() {
        return timerCheckpoint;
    }

    public TimerState getTimerState() {
        return timerState;
    }

}
