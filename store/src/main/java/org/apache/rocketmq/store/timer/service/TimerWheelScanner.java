package org.apache.rocketmq.store.timer.service;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.SelectMappedBufferResult;
import org.apache.rocketmq.store.timer.Slot;
import org.apache.rocketmq.store.timer.TimerLog;
import org.apache.rocketmq.store.timer.TimerRequest;
import org.apache.rocketmq.store.timer.TimerState;
import org.apache.rocketmq.store.timer.TimerWheel;
import org.apache.rocketmq.store.util.PerfCounter;

import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class TimerWheelScanner implements Scanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);


    private TimerState timerState;
    private TimerWheel timerWheel;
    private TimerLog timerLog;
    private MessageStoreConfig storeConfig;
    private PerfCounter.Ticks perfCounterTicks;

    private final int precisionMs;
    private final int timerLogFileSize;
    public TimerWheelScanner(TimerState timerState, TimerWheel timerWheel, TimerLog timerLog, MessageStoreConfig storeConfig, PerfCounter.Ticks perfCounterTicks) {
        this.timerState = timerState;
        this.timerWheel = timerWheel;
        this.timerLog = timerLog;
        this.storeConfig = storeConfig;
        this.perfCounterTicks = perfCounterTicks;

        this.precisionMs = storeConfig.getTimerPrecisionMs();
        this.timerLogFileSize = storeConfig.getMappedFileSizeTimerLog();
    }


    @Override
    public ScannResult scan() {
        ScannResult result = new ScannResult();
        Slot slot = timerWheel.getSlot(timerState.currReadTimeMs);
        if (-1 == slot.timeMs) {
            timerState.moveReadTime(precisionMs);
            return result;
        }
        result.code = 1;
        try {
            //clear the flag
            timerState.dequeueStatusChangeFlag = false;

            long currOffsetPy = slot.lastPos;
            Set<String> deleteUniqKeys = new ConcurrentSkipListSet<>();
            LinkedList<SelectMappedBufferResult> sbrs = new LinkedList<>();
            SelectMappedBufferResult timeSbr = null;
            //read the timer log one by one
            while (currOffsetPy != -1) {
                perfCounterTicks.startTick("dequeue_read_timerlog");
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
                    int magic = timeSbr.getByteBuffer().getInt();
                    long enqueueTime = timeSbr.getByteBuffer().getLong();
                    long delayedTime = timeSbr.getByteBuffer().getInt() + enqueueTime;
                    long offsetPy = timeSbr.getByteBuffer().getLong();
                    int sizePy = timeSbr.getByteBuffer().getInt();
                    TimerRequest timerRequest = new TimerRequest(offsetPy, sizePy, delayedTime, enqueueTime, magic);
                    timerRequest.setDeleteList(deleteUniqKeys);
                    if (timerState.needDelete(magic) && !timerState.needRoll(magic)) {
                        result.addDeleteMsgStack(timerRequest);
                    } else {
                        result.addNormalMsgStack(timerRequest);
                    }
                } catch (Exception e) {
                    LOGGER.error("Error in dequeue_read_timerlog", e);
                } finally {
                    currOffsetPy = prevPos;
                    perfCounterTicks.endTick("dequeue_read_timerlog");
                }
            }
            if (result.sizeOfDeleteMsgStack() == 0 && result.sizeOfNormalMsgStack() == 0) {
                LOGGER.warn("dequeue time:{} but read nothing from timerLog", timerState.currReadTimeMs);
            }
            for (SelectMappedBufferResult sbr : sbrs) {
                if (null != sbr) {
                    sbr.release();
                }
            }
        } catch (Throwable t) {
            LOGGER.error("Unknown error in dequeue process", t);
            if (storeConfig.isTimerSkipUnknownError()) {
                timerState.moveReadTime(precisionMs);
            }
        }
        return result;
    }
}
