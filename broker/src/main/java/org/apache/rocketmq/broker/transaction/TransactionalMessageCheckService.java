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
package org.apache.rocketmq.broker.transaction;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.transaction.queue.CheckContext;
import org.apache.rocketmq.broker.transaction.queue.GetResult;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageBridge;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageUtil;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;

/**
 * Transaction check service
 * 1. load prepared message directly from consume queue with special topic:
 *      TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC
 * 2. then check transaction status, then execute commit/rollback
 *
 */
public class TransactionalMessageCheckService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private static final int PULL_MSG_RETRY_NUMBER = 1;
    private static final int MAX_PROCESS_TIME_LIMIT = 60000;
    private static final int MAX_RETRY_TIMES_FOR_ESCAPE = 10;
    private static final int MAX_RETRY_COUNT_WHEN_HALF_NULL = 1;
    private static final int OP_MSG_PULL_NUMS = 32;
    private static final int SLEEP_WHILE_NO_OP = 1000;

    private final BrokerController brokerController;
    private final TransactionalMessageBridge transactionalMessageBridge;
    private final AbstractTransactionalMessageCheckListener transactionalMessageCheckListener;

    private final ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    public TransactionalMessageCheckService(BrokerController brokerController, TransactionalMessageBridge transactionalMessageBridge, AbstractTransactionalMessageCheckListener transactionalMessageCheckListener) {
        this.brokerController = brokerController;
        this.transactionalMessageBridge = transactionalMessageBridge;
        this.transactionalMessageCheckListener = transactionalMessageCheckListener;
    }

    @Override
    public String getServiceName() {
        if (brokerController != null && brokerController.getBrokerConfig().isInBrokerContainer()) {
            return brokerController.getBrokerIdentity().getIdentifier() + TransactionalMessageCheckService.class.getSimpleName();
        }
        return TransactionalMessageCheckService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("Start transaction check service thread!");
        while (!this.isStopped()) {
            long checkInterval = brokerController.getBrokerConfig().getTransactionCheckInterval();

            // execute this.onWaitEnd()
            // then execute TransactionalMessageService.check()
            this.waitForRunning(checkInterval);
        }
        log.info("End transaction check service thread!");
    }

    @Override
    protected void onWaitEnd() {
        long timeout = brokerController.getBrokerConfig().getTransactionTimeOut();
        int checkMax = brokerController.getBrokerConfig().getTransactionCheckMax();
        long begin = System.currentTimeMillis();
        log.info("Begin to check prepare message, begin time:{}", begin);
        this.check(timeout, checkMax, this.transactionalMessageCheckListener);
        log.info("End to check prepare message, consumed time:{}", System.currentTimeMillis() - begin);
    }

    public void check(long transactionTimeout, int transactionCheckMax, AbstractTransactionalMessageCheckListener listener) {
        try {
            String topic = TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;
            Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);
            if (msgQueues == null || msgQueues.size() == 0) {
                log.warn("The queue of topic is empty :" + topic);
                return;
            }
            log.debug("Check topic={}, queues={}", topic, msgQueues);

            // By default, there is only one transactional message queue
            for (MessageQueue messageQueue : msgQueues) {
                checkMessageQueue(messageQueue, transactionTimeout, transactionCheckMax, listener);
            }
        } catch (Throwable e) {
            log.error("Check error", e);
        }
    }

    private void checkMessageQueue(MessageQueue messageQueue, long transactionTimeout, int transactionCheckMax, AbstractTransactionalMessageCheckListener listener) throws InterruptedException {
        CheckContext context = new CheckContext(messageQueue, transactionTimeout, transactionCheckMax, listener);

        context.setOpQueue(getOpQueue(messageQueue));
        context.setHalfOffset(transactionalMessageBridge.fetchConsumeOffset(messageQueue));
        context.setOpOffset(transactionalMessageBridge.fetchConsumeOffset(context.getOpQueue()));
        log.info("Before check, the queue={} msgOffset={} opOffset={}", messageQueue, context.getHalfOffset(), context.getOpOffset());
        if (context.getHalfOffset() < 0 || context.getOpOffset() < 0) {
            log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", messageQueue, context.getHalfOffset(), context.getOpOffset());
            return;
        }

        PullResult removeResult = fillOpRemoveMap(context.getRemoveMap(), context.getOpQueue(), context.getOpOffset(), context.getHalfOffset(), context.getOpMsgMap(), context.getDoneOpOffset());
        if (null == removeResult) {
            log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null", messageQueue, context.getHalfOffset(), context.getOpOffset());
            return;
        }

        // single thread
        context.setPullResult(removeResult);
        context.initOffset();

        while (true) {
            if (System.currentTimeMillis() - context.getStartTime() > MAX_PROCESS_TIME_LIMIT) {
                log.info("Queue={} process time reach max={}", messageQueue, MAX_PROCESS_TIME_LIMIT);
                break;
            }
            if (context.getRemoveMap().containsKey(context.getI())) {
                removeOffset(context);
                continue;
            }

            GetResult getResult = getHalfMsg(messageQueue, context.getI());
            context.setMsgExt(getResult.getMsg());
            if (context.getMsgExt() == null) {
                if (!handleNullHalfMsg(context, getResult)) {
                    break;
                }
                continue;
            }

            if (isSlaveMode()) {
                handleSlaveMode(context);
                continue;
            }

            if (needDiscard(context.getMsgExt(), transactionCheckMax) || needSkip(context.getMsgExt())) {
                skipOrDiscard(context);
                continue;
            }

            if (context.getMsgExt().getStoreTimestamp() >= context.getStartTime()) {
                log.debug("Fresh stored. the miss offset={}, check it later, store={}", context.getI(), new Date(context.getMsgExt().getStoreTimestamp()));
                break;
            }

            long valueOfCurrentMinusBorn = System.currentTimeMillis() - context.getMsgExt().getBornTimestamp();
            String checkImmunityTimeStr = context.getMsgExt().getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
            if (null == checkImmunityTimeStr && 0 <= valueOfCurrentMinusBorn && valueOfCurrentMinusBorn < transactionTimeout) {
                log.debug("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", context.getI(), transactionTimeout, new Date(context.getMsgExt().getBornTimestamp()));
                break;
            }

            Long checkImmunityTime = checkImmunityTime(context, checkImmunityTimeStr, valueOfCurrentMinusBorn);
            if (checkImmunityTime == null) {
                break;
            }

            if (!isNeedCheck(context, valueOfCurrentMinusBorn, checkImmunityTime)) {
                noNeedCheck(context);
                continue;
            }

            if (!putBackHalfMsgQueue(context.getMsgExt(), context.getI())) {
                continue;
            }

            afterPutBackMsg(context);
        }

        updateOffset(context);
    }

    private void removeOffset(CheckContext context) {
        log.debug("Half offset {} has been committed/rolled back", context.getI());
        Long removedOpOffset = context.getRemoveMap().remove(context.getI());
        context.getOpMsgMap().get(removedOpOffset).remove(context.getI());
        if (context.getOpMsgMap().get(removedOpOffset).size() == 0) {
            context.getOpMsgMap().remove(removedOpOffset);
            context.getDoneOpOffset().add(removedOpOffset);
        }
    }

    private boolean handleNullHalfMsg(CheckContext context, GetResult getResult) {
        context.incGetMessageNullCount();
        if (context.getGetMessageNullCount() > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
            return false;
        }
        if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {
            log.debug("No new msg, the miss offset={} in={}, continue check={}, pull result={}", context.getI(),
                context.getMessageQueue(), context.getGetMessageNullCount(), getResult.getPullResult());
            return false;
        }

        log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}",
            context.getI(), context.getMessageQueue(), context.getGetMessageNullCount(), getResult.getPullResult());
        context.setI(getResult.getPullResult().getNextBeginOffset());
        context.setNewOffset(context.getI());

        return true;
    }

    private boolean isSlaveMode() {
        BrokerController brokerController = this.transactionalMessageBridge.getBrokerController();
        BrokerConfig brokerConfig = brokerController.getBrokerConfig();
        MessageStoreConfig storeConfig = brokerController.getMessageStoreConfig();

        if (!brokerConfig.isEnableSlaveActingMaster()) {
            return false;
        }

        if (!BrokerRole.SLAVE.equals(storeConfig.getBrokerRole())) {
            return false;
        }
        return brokerController.getMinBrokerIdInGroup() == brokerController.getBrokerIdentity().getBrokerId();
    }

    private void handleSlaveMode(CheckContext context) throws InterruptedException {
        final MessageExtBrokerInner msgInner = this.transactionalMessageBridge.renewHalfMessageInner(context.getMsgExt());
        final boolean isSuccess = this.transactionalMessageBridge.escapeMessage(msgInner);

        if (isSuccess) {
            context.setEscapeFailCnt(0);
            context.setNewOffset(context.getI() + 1);
            context.incI();

            return;
        }

        log.warn("Escaping transactional message failed {} times! msgId(offsetId)={}, UNIQ_KEY(transactionId)={}",
            context.getEscapeFailCnt() + 1,
            context.getMsgExt().getMsgId(),
            context.getMsgExt().getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        if (context.getEscapeFailCnt() < MAX_RETRY_TIMES_FOR_ESCAPE) {
            context.incEscapeFailCnt();
            Thread.sleep(100L * (2 ^ context.getEscapeFailCnt()));
        } else {
            context.setEscapeFailCnt(0);
            context.setNewOffset(context.getI() + 1);
            context.incI();
        }
    }

    private void skipOrDiscard(CheckContext context) {
        context.getListener().resolveDiscardMsg(context.getMsgExt());
        context.setNewOffset(context.getI() + 1);
        context.incI();
    }

    private Long checkImmunityTime(CheckContext context, String checkImmunityTimeStr, long valueOfCurrentMinusBorn) {
        long checkImmunityTime = context.getTransactionTimeout();
        if (null == checkImmunityTimeStr) {
            return checkImmunityTime;
        }

        checkImmunityTime = getImmunityTime(checkImmunityTimeStr, context.getTransactionTimeout());
        if (valueOfCurrentMinusBorn >= checkImmunityTime) {
            return checkImmunityTime;
        }

        if (!checkPrepareQueueOffset(context.getRemoveMap(), context.getDoneOpOffset(), context.getMsgExt(), checkImmunityTimeStr)) {
            return checkImmunityTime;
        }

        context.setNewOffset(context.getI() + 1);
        context.incI();
        return null;
    }

    private boolean isNeedCheck(CheckContext context, long valueOfCurrentMinusBorn, long checkImmunityTime) {
        if (valueOfCurrentMinusBorn <= -1) {
            return true;
        }

        List<MessageExt> opMsg = context.getPullResult() == null ? null : context.getPullResult().getMsgFoundList();
        if (opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime) {
            return true;
        }

        if (opMsg == null) {
            return false;
        }

        MessageExt lastMsg = opMsg.get(opMsg.size() - 1);
        return lastMsg.getBornTimestamp() - context.getStartTime() > context.getTransactionTimeout();
    }

    private void afterPutBackMsg(CheckContext context) {
        context.incPutInQueueCount();
        log.info("Check transaction. real_topic={},uniqKey={},offset={},commitLogOffset={}",
            context.getMsgExt().getUserProperty(MessageConst.PROPERTY_REAL_TOPIC),
            context.getMsgExt().getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            context.getMsgExt().getQueueOffset(), context.getMsgExt().getCommitLogOffset());
        context.getListener().resolveHalfMsg(context.getMsgExt());

        context.setNewOffset(context.getI() + 1);
        context.incI();
    }

    private void noNeedCheck(CheckContext context) {
        long tmpOffset = context.getPullResult() != null ? context.getPullResult().getNextBeginOffset() : context.getNextOpOffset();
        context.setNextOpOffset(tmpOffset);

        PullResult tmpPullResult = fillOpRemoveMap(context.getRemoveMap(), context.getOpQueue(), context.getNextOpOffset(),
            context.getHalfOffset(), context.getOpMsgMap(), context.getDoneOpOffset());
        context.setPullResult(tmpPullResult);

        if (context.getPullResult() == null || context.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG
            || context.getPullResult().getPullStatus() == PullStatus.OFFSET_ILLEGAL
            || context.getPullResult().getPullStatus() == PullStatus.NO_MATCHED_MSG) {

            ThreadUtils.sleep(SLEEP_WHILE_NO_OP);

        } else {
            log.info("The miss message offset:{}, pullOffsetOfOp:{}, miniOffset:{} get more opMsg.", context.getI(), context.getNextOpOffset(), context.getHalfOffset());
        }
    }

    private void updateOffset(CheckContext context) {
        if (context.getNewOffset() != context.getHalfOffset()) {
            transactionalMessageBridge.updateConsumeOffset(context.getMessageQueue(), context.getNewOffset());
        }
        long newOpOffset = calculateOpOffset(context.getDoneOpOffset(), context.getOpOffset());
        if (newOpOffset != context.getOpOffset()) {
            transactionalMessageBridge.updateConsumeOffset(context.getOpQueue(), newOpOffset);
        }
        GetResult getResult = getHalfMsg(context.getMessageQueue(), context.getNewOffset());
        context.setPullResult(pullOpMsg(context.getOpQueue(), newOpOffset, 1));
        long maxMsgOffset = getResult.getPullResult() == null ? context.getNewOffset() : getResult.getPullResult().getMaxOffset();
        long maxOpOffset = context.getPullResult() == null ? newOpOffset : context.getPullResult().getMaxOffset();
        long msgTime = getResult.getMsg() == null ? System.currentTimeMillis() : getResult.getMsg().getStoreTimestamp();

        log.info("After check, {} opOffset={} opOffsetDiff={} msgOffset={} msgOffsetDiff={} msgTime={} msgTimeDelayInMs={} putInQueueCount={}",
            context.getMessageQueue(), newOpOffset, maxOpOffset - newOpOffset, context.getNewOffset(), maxMsgOffset - context.getNewOffset(), new Date(msgTime),
            System.currentTimeMillis() - msgTime, context.getPutInQueueCount());
    }

    private boolean needDiscard(MessageExt msgExt, int transactionCheckMax) {
        String checkTimes = msgExt.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
        int checkTime = 1;
        if (null != checkTimes) {
            checkTime = getInt(checkTimes);
            if (checkTime >= transactionCheckMax) {
                return true;
            } else {
                checkTime++;
            }
        }
        msgExt.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime));
        return false;
    }

    private boolean needSkip(MessageExt msgExt) {
        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
        if (valueOfCurrentMinusBorn
            > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime()
            * 3600L * 1000) {
            log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}",
                msgExt.getMsgId(), msgExt.getBornTimestamp());
            return true;
        }
        return false;
    }

    private boolean putBackHalfMsgQueue(MessageExt msgExt, long offset) {
        PutMessageResult putMessageResult = putBackToHalfQueueReturnResult(msgExt);
        if (putMessageResult != null && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            msgExt.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());
            msgExt.setCommitLogOffset(putMessageResult.getAppendMessageResult().getWroteOffset());
            msgExt.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            log.debug("Send check message, the offset={} restored in queueOffset={} "
                    + "commitLogOffset={} "
                    + "newMsgId={} realMsgId={} topic={}",
                offset, msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getMsgId(),
                msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                msgExt.getTopic());
            return true;
        } else {
            log.error("PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, msgId: {}",
                msgExt.getTopic(), msgExt.getQueueId(), msgExt.getMsgId());
            return false;
        }
    }

    private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime;

        checkImmunityTime = getLong(checkImmunityTimeStr);
        if (-1 == checkImmunityTime) {
            checkImmunityTime = transactionTimeout;
        } else {
            checkImmunityTime *= 1000;
        }
        return checkImmunityTime;
    }

    /**
     * Read op message, parse op message, and fill removeMap
     *
     * @param removeMap Half message to be remove, key:halfOffset, value: opOffset.
     * @param opQueue Op message queue.
     * @param pullOffsetOfOp The start offset of op message queue.
     * @param miniOffset The current minimum offset of half message queue.
     * @param opMsgMap Map<queueOffset, HashSet<offsetValue>> Half message offset in op message
     * @param doneOpOffset Stored op messages that have been processed.
     * @return Op message result.
     */
    private PullResult fillOpRemoveMap(HashMap<Long, Long> removeMap, MessageQueue opQueue,
        long pullOffsetOfOp, long miniOffset, Map<Long, HashSet<Long>> opMsgMap, List<Long> doneOpOffset) {
        PullResult pullResult = pullOpMsg(opQueue, pullOffsetOfOp, OP_MSG_PULL_NUMS);
        if (!handleIllegalOpMsg(pullResult, opQueue, pullOffsetOfOp)) {
            return pullResult;
        }

        List<MessageExt> opMsg = pullResult.getMsgFoundList();
        if (opMsg == null) {
            log.warn("The miss op offset={} in queue={} is empty, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return pullResult;
        }

        for (MessageExt opMessageExt : opMsg) {
            if (opMessageExt.getBody() == null) {
                log.error("op message body is null. queueId={}, offset={}", opMessageExt.getQueueId(), opMessageExt.getQueueOffset());
                doneOpOffset.add(opMessageExt.getQueueOffset());
                continue;
            }

            HashSet<Long> set = handleMsgWithRemoveTag(opMessageExt, miniOffset, removeMap);

            if (set.size() > 0) {
                opMsgMap.put(opMessageExt.getQueueOffset(), set);
            } else {
                doneOpOffset.add(opMessageExt.getQueueOffset());
            }
        }

        log.debug("Remove map: {}, Done op list: {}, opMsg map: {}", removeMap, doneOpOffset, opMsgMap);
        return pullResult;
    }

    private boolean handleIllegalOpMsg(PullResult pullResult, MessageQueue opQueue, long pullOffsetOfOp) {
        if (null == pullResult) {
            return false;
        }

        if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            log.warn("The miss op offset={} in queue={} is illegal, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            transactionalMessageBridge.updateConsumeOffset(opQueue, pullResult.getNextBeginOffset());
            return false;
        }

        if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
            log.warn("The miss op offset={} in queue={} is NO_NEW_MSG, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return false;
        }

        return true;
    }

    private HashSet<Long> handleMsgWithRemoveTag(MessageExt opMessageExt, long miniOffset, HashMap<Long, Long> removeMap) {
        HashSet<Long> set = new HashSet<>();
        String queueOffsetBody = new String(opMessageExt.getBody(), TransactionalMessageUtil.CHARSET);
        log.debug("Topic: {} tags: {}, OpOffset: {}, HalfOffset: {}", opMessageExt.getTopic(),
            opMessageExt.getTags(), opMessageExt.getQueueOffset(), queueOffsetBody);

        if (!TransactionalMessageUtil.REMOVE_TAG.equals(opMessageExt.getTags())) {
            log.error("Found a illegal tag in opMessageExt= {} ", opMessageExt);
            return set;
        }

        String[] offsetArray = queueOffsetBody.split(TransactionalMessageUtil.OFFSET_SEPARATOR);
        for (String offset : offsetArray) {
            Long offsetValue = getLong(offset);
            if (offsetValue < miniOffset) {
                continue;
            }

            removeMap.put(offsetValue, opMessageExt.getQueueOffset());
            set.add(offsetValue);
        }

        return set;
    }

    /**
     * If return true, skip this msg
     *
     * @param removeMap Op message map to determine whether a half message was responded by producer.
     * @param doneOpOffset Op Message which has been checked.
     * @param msgExt Half message
     * @return Return true if put success, otherwise return false.
     */
    private boolean checkPrepareQueueOffset(HashMap<Long, Long> removeMap, List<Long> doneOpOffset,
        MessageExt msgExt, String checkImmunityTimeStr) {
        String prepareQueueOffsetStr = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        if (null == prepareQueueOffsetStr) {
            return putImmunityMsgBackToHalfQueue(msgExt);
        }

        long prepareQueueOffset = getLong(prepareQueueOffsetStr);
        if (-1 == prepareQueueOffset) {
            return false;
        }

        if (!removeMap.containsKey(prepareQueueOffset)) {
            return putImmunityMsgBackToHalfQueue(msgExt);
        }

        long tmpOpOffset = removeMap.remove(prepareQueueOffset);
        doneOpOffset.add(tmpOpOffset);
        log.info("removeMap contain prepareQueueOffset. real_topic={},uniqKey={},immunityTime={},offset={}",
            msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC),
            msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            checkImmunityTimeStr,
            msgExt.getQueueOffset());
        return true;
    }

    /**
     * Write messageExt to Half topic again
     *
     * @param messageExt Message will be write back to queue
     * @return Put result can used to determine the specific results of storage.
     */
    private PutMessageResult putBackToHalfQueueReturnResult(MessageExt messageExt) {
        PutMessageResult putMessageResult = null;
        try {
            MessageExtBrokerInner msgInner = transactionalMessageBridge.renewHalfMessageInner(messageExt);
            putMessageResult = transactionalMessageBridge.putMessageReturnResult(msgInner);
        } catch (Exception e) {
            log.warn("PutBackToHalfQueueReturnResult error", e);
        }
        return putMessageResult;
    }

    private boolean putImmunityMsgBackToHalfQueue(MessageExt messageExt) {
        MessageExtBrokerInner msgInner = transactionalMessageBridge.renewImmunityHalfMessageInner(messageExt);
        return transactionalMessageBridge.putMessage(msgInner);
    }

    /**
     * Read op message from Op Topic
     *
     * @param mq Target Message Queue
     * @param offset Offset in the message queue
     * @param nums Pull message number
     * @return Messages pulled from operate message queue.
     */
    private PullResult pullOpMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getOpMessage(mq.getQueueId(), offset, nums);
    }

    private Long getLong(String s) {
        long v = -1;
        try {
            v = Long.parseLong(s);
        } catch (Exception e) {
            log.error("GetLong error", e);
        }
        return v;

    }

    private Integer getInt(String s) {
        int v = -1;
        try {
            v = Integer.parseInt(s);
        } catch (Exception e) {
            log.error("GetInt error", e);
        }
        return v;

    }

    private long calculateOpOffset(List<Long> doneOffset, long oldOffset) {
        Collections.sort(doneOffset);
        long newOffset = oldOffset;
        for (Long aLong : doneOffset) {
            if (aLong == newOffset) {
                newOffset++;
            } else {
                break;
            }
        }
        return newOffset;

    }

    /**
     * create op queue map and return the op queue
     *
     * @param messageQueue mq
     * @return op queue
     */
    private MessageQueue getOpQueue(MessageQueue messageQueue) {
        MessageQueue opQueue = opQueueMap.get(messageQueue);
        if (opQueue == null) {
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), messageQueue.getBrokerName(),
                messageQueue.getQueueId());
            opQueueMap.put(messageQueue, opQueue);
        }
        return opQueue;

    }

    private GetResult getHalfMsg(MessageQueue messageQueue, long offset) {
        GetResult getResult = new GetResult();

        PullResult result = pullHalfMsg(messageQueue, offset, PULL_MSG_RETRY_NUMBER);
        if (result == null) {
            return getResult;
        }

        getResult.setPullResult(result);
        List<MessageExt> messageExts = result.getMsgFoundList();
        if (messageExts == null || messageExts.size() == 0) {
            return getResult;
        }
        getResult.setMsg(messageExts.get(0));
        return getResult;
    }

    /**
     * Read half message from Half Topic
     *
     * @param mq Target message queue, in this method, it means the half message queue.
     * @param offset Offset in the message queue.
     * @param nums Pull message number.
     * @return Messages pulled from half message queue.
     */
    private PullResult pullHalfMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getHalfMessage(mq.getQueueId(), offset, nums);
    }

}
