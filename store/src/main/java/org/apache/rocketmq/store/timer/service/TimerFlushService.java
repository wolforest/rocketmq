package org.apache.rocketmq.store.timer.service;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.timer.Pointer;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerLog;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.timer.TimerMetrics;
import org.apache.rocketmq.store.timer.TimerRequest;
import org.apache.rocketmq.store.timer.TimerWheel;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static org.apache.rocketmq.store.timer.TimerMessageStore.TIMER_TOPIC;

public class TimerFlushService extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final SimpleDateFormat sdf = new SimpleDateFormat("MM-dd HH:mm:ss");
    private TimerMessageStore timerMessageStore;
    private BlockingQueue<TimerRequest> enqueuePutQueue;
    private BlockingQueue<List<TimerRequest>> dequeueGetQueue;
    private BlockingQueue<TimerRequest> dequeuePutQueue;
    private MessageStoreConfig storeConfig;
    private Pointer pointer;
    private TimerMetrics timerMetrics;
    private TimerCheckpoint timerCheckpoint;
    private TimerLog timerLog;
    private TimerWheel timerWheel;

    public TimerFlushService(TimerMessageStore timerMessageStore) {
        this.timerMessageStore = timerMessageStore;
        enqueuePutQueue = timerMessageStore.getEnqueuePutQueue();
        dequeueGetQueue = timerMessageStore.getDequeueGetQueue();
        dequeuePutQueue = timerMessageStore.getDequeuePutQueue();
        storeConfig = timerMessageStore.getMessageStore().getMessageStoreConfig();
        pointer = timerMessageStore.getPointer();
        timerMetrics = timerMessageStore.getTimerMetrics();
        timerCheckpoint = timerMessageStore.getTimerCheckpoint();
        timerLog = timerMessageStore.getTimerLog();
        timerWheel = timerMessageStore.getTimerWheel();
    }

    @Override
    public String getServiceName() {
        String brokerIdentifier = "";
        if (timerMessageStore.getMessageStore() instanceof DefaultMessageStore && ((DefaultMessageStore) timerMessageStore.getMessageStore()).getBrokerConfig().isInBrokerContainer()) {
            brokerIdentifier = ((DefaultMessageStore) timerMessageStore.getMessageStore()).getBrokerConfig().getIdentifier();
        }
        return brokerIdentifier + this.getClass().getSimpleName();
    }

    private String format(long time) {
        return sdf.format(new Date(time));
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service start");
        long start = System.currentTimeMillis();

        while (!this.isStopped()) {
            try {
                timerMessageStore.prepareTimerCheckPoint();
                timerLog.getMappedFileQueue().flush(0);
                timerWheel.flush();
                timerCheckpoint.flush();
                if (System.currentTimeMillis() - start > storeConfig.getTimerProgressLogIntervalMs()) {
                    start = System.currentTimeMillis();
                    long tmpQueueOffset = pointer.currQueueOffset;
                    ConsumeQueue cq = (ConsumeQueue) timerMessageStore.getMessageStore().getConsumeQueue(TIMER_TOPIC, 0);
                    long maxOffsetInQueue = cq == null ? 0 : cq.getMaxOffsetInQueue();
                    LOGGER.info("[{}]Timer progress-check commitRead:[{}] currRead:[{}] currWrite:[{}] readBehind:{} currReadOffset:{} offsetBehind:{} behindMaster:{} " +
                                    "enqPutQueue:{} deqGetQueue:{} deqPutQueue:{} allCongestNum:{} enqExpiredStoreTime:{}",
                            storeConfig.getBrokerRole(),
                            format(pointer.commitReadTimeMs), format(pointer.currReadTimeMs), format(pointer.currWriteTimeMs), timerMessageStore.getDequeueBehind(),
                            tmpQueueOffset, maxOffsetInQueue - tmpQueueOffset, timerCheckpoint.getMasterTimerQueueOffset() - tmpQueueOffset,
                            enqueuePutQueue.size(), dequeueGetQueue.size(), dequeuePutQueue.size(), timerMessageStore.getAllCongestNum(), format(pointer.lastEnqueueButExpiredStoreTime));
                }
                timerMetrics.persist();
                waitForRunning(storeConfig.getTimerFlushIntervalMs());
            } catch (Throwable e) {
                LOGGER.error("Error occurred in " + getServiceName(), e);
            }
        }
        LOGGER.info(this.getServiceName() + " service end");
    }
}


