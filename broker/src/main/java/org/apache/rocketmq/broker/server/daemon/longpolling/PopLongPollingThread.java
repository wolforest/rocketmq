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

package org.apache.rocketmq.broker.server.daemon.longpolling;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.netty.channel.ChannelHandlerContext;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.common.domain.topic.KeyBuilder;
import org.apache.rocketmq.common.domain.constant.PopConstants;
import org.apache.rocketmq.common.lang.thread.ServiceThread;
import org.apache.rocketmq.common.domain.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.broker.server.daemon.longpolling.PollingResult.NOT_POLLING;
import static org.apache.rocketmq.broker.server.daemon.longpolling.PollingResult.POLLING_FULL;
import static org.apache.rocketmq.broker.server.daemon.longpolling.PollingResult.POLLING_SUC;
import static org.apache.rocketmq.broker.server.daemon.longpolling.PollingResult.POLLING_TIMEOUT;

public class PopLongPollingThread extends ServiceThread {
    private static final Logger POP_LOGGER =
        LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final Broker broker;
    private final NettyRequestProcessor processor;

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Byte>> topicCidMap;
    private final ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>> pollingMap;
    private long lastCleanTime = 0;

    private final AtomicLong totalPollingNum = new AtomicLong(0);

    public PopLongPollingThread(Broker broker, NettyRequestProcessor processor) {
        this.broker = broker;
        this.processor = processor;
        // 100000 topic default,  100000 lru topic + cid + qid
        this.topicCidMap = new ConcurrentHashMap<>(broker.getBrokerConfig().getPopPollingMapSize());
        this.pollingMap = new ConcurrentLinkedHashMap.Builder<String, ConcurrentSkipListSet<PopRequest>>()
            .maximumWeightedCapacity(this.broker.getBrokerConfig().getPopPollingMapSize()).build();
    }

    @Override
    public String getServiceName() {
        if (broker.getBrokerConfig().isInBrokerContainer()) {
            return broker.getBrokerIdentity().getIdentifier() + PopLongPollingThread.class.getSimpleName();
        }
        return PopLongPollingThread.class.getSimpleName();
    }

    @Override
    public void run() {
        int i = 0;
        while (!this.stopped) {
            try {
                this.waitForRunning(20);
                i++;
                if (pollingMap.isEmpty()) {
                    continue;
                }

                long tmpTotalPollingNum = run(i);

                if (i >= 100) {
                    setPollingNum(tmpTotalPollingNum);
                    i = 0;
                }

                // clean unused
                if (lastCleanTime == 0 || System.currentTimeMillis() - lastCleanTime > 5 * 60 * 1000) {
                    cleanUnusedResource();
                }
            } catch (Throwable e) {
                POP_LOGGER.error("checkPolling error", e);
            }
        }

        wakeUpAll();
    }

    private long run(int i) {
        long tmpTotalPollingNum = 0;
        for (Map.Entry<String, ConcurrentSkipListSet<PopRequest>> entry : pollingMap.entrySet()) {
            String key = entry.getKey();
            ConcurrentSkipListSet<PopRequest> popQ = entry.getValue();
            if (popQ == null) {
                continue;
            }
            PopRequest first;
            do {
                first = popQ.pollFirst();
                if (first == null) {
                    break;
                }
                if (!first.isTimeout()) {
                    if (popQ.add(first)) {
                        break;
                    } else {
                        POP_LOGGER.info("polling, add fail again: {}", first);
                    }
                }
                if (broker.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.info("timeout , wakeUp polling : {}", first);
                }
                totalPollingNum.decrementAndGet();
                wakeUp(first);
            } while (true);


            if (i >= 100) {
                long tmpPollingNum = popQ.size();
                tmpTotalPollingNum = tmpTotalPollingNum + tmpPollingNum;
                if (tmpPollingNum > 100) {
                    POP_LOGGER.info("polling queue {} , size={} ", key, tmpPollingNum);
                }
            }
        }

        return tmpTotalPollingNum;
    }

    private void setPollingNum(long tmpTotalPollingNum) {
        POP_LOGGER.info("pollingMapSize={},tmpTotalSize={},atomicTotalSize={},diffSize={}",
            pollingMap.size(), tmpTotalPollingNum, totalPollingNum.get(), Math.abs(totalPollingNum.get() - tmpTotalPollingNum));
        totalPollingNum.set(tmpTotalPollingNum);
    }

    private void wakeUpAll() {
        // clean all;
        try {
            for (Map.Entry<String, ConcurrentSkipListSet<PopRequest>> entry : pollingMap.entrySet()) {
                ConcurrentSkipListSet<PopRequest> popQ = entry.getValue();
                PopRequest first;
                while ((first = popQ.pollFirst()) != null) {
                    wakeUp(first);
                }
            }
        } catch (Throwable ignored) {
        }
    }

    public void notifyMessageArrivingWithRetryTopic(final String topic, final int queueId) {
        String notifyTopic;
        if (KeyBuilder.isPopRetryTopicV2(topic)) {
            notifyTopic = KeyBuilder.parseNormalTopic(topic);
        } else {
            notifyTopic = topic;
        }
        notifyMessageArriving(notifyTopic, queueId);
    }

    public void notifyMessageArriving(final String topic, final int queueId) {
        ConcurrentHashMap<String, Byte> cids = topicCidMap.get(topic);
        if (cids == null) {
            return;
        }
        for (Map.Entry<String, Byte> cid : cids.entrySet()) {
            if (queueId >= 0) {
                notifyMessageArriving(topic, cid.getKey(), -1);
            }
            notifyMessageArriving(topic, cid.getKey(), queueId);
        }
    }

    public boolean notifyMessageArriving(final String topic, final String cid, final int queueId) {
        ConcurrentSkipListSet<PopRequest> remotingCommands = pollingMap.get(KeyBuilder.buildConsumeKey(topic, cid, queueId));
        if (remotingCommands == null || remotingCommands.isEmpty()) {
            return false;
        }
        PopRequest popRequest = remotingCommands.pollFirst();
        //clean inactive channel
        while (popRequest != null && !popRequest.getChannel().isActive()) {
            totalPollingNum.decrementAndGet();
            popRequest = remotingCommands.pollFirst();
        }

        if (popRequest == null) {
            return false;
        }
        totalPollingNum.decrementAndGet();
        if (broker.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("lock release , new msg arrive , wakeUp : {}", popRequest);
        }
        return wakeUp(popRequest);
    }

    public boolean wakeUp(final PopRequest request) {
        if (request == null || !request.complete()) {
            return false;
        }
        if (!request.getCtx().channel().isActive()) {
            return false;
        }
        Runnable run = () -> {
            try {
                final RemotingCommand response = processor.processRequest(request.getCtx(), request.getRemotingCommand());
                if (response != null) {
                    response.setOpaque(request.getRemotingCommand().getOpaque());
                    response.markResponseType();
                    NettyRemotingAbstract.writeResponse(request.getChannel(), request.getRemotingCommand(), response, future -> {
                        if (!future.isSuccess()) {
                            POP_LOGGER.error("ProcessRequestWrapper response to {} failed", request.getChannel().remoteAddress(), future.cause());
                            POP_LOGGER.error(request.toString());
                            POP_LOGGER.error(response.toString());
                        }
                    });
                }
            } catch (Exception e1) {
                POP_LOGGER.error("ExecuteRequestWhenWakeup run", e1);
            }
        };
        this.broker.getBrokerNettyServer().getPullMessageExecutor().submit(new RequestTask(run, request.getChannel(), request.getRemotingCommand()));
        return true;
    }

    /**
     * @param ctx ctx
     * @param remotingCommand request
     * @param requestHeader requestHeader
     * @return PollingResult
     */
    public PollingResult polling(final ChannelHandlerContext ctx, RemotingCommand remotingCommand,
        final PollingHeader requestHeader) {
        if (requestHeader.getPollTime() <= 0 || this.isStopped()) {
            return NOT_POLLING;
        }
        ConcurrentHashMap<String, Byte> cids = topicCidMap.get(requestHeader.getTopic());
        if (cids == null) {
            cids = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, Byte> old = topicCidMap.putIfAbsent(requestHeader.getTopic(), cids);
            if (old != null) {
                cids = old;
            }
        }
        cids.putIfAbsent(requestHeader.getConsumerGroup(), Byte.MIN_VALUE);
        long expired = requestHeader.getBornTime() + requestHeader.getPollTime();
        final PopRequest request = new PopRequest(remotingCommand, ctx, expired);
        boolean isFull = totalPollingNum.get() >= this.broker.getBrokerConfig().getMaxPopPollingSize();
        if (isFull) {
            POP_LOGGER.info("polling {}, result POLLING_FULL, total:{}", remotingCommand, totalPollingNum.get());
            return POLLING_FULL;
        }
        boolean isTimeout = request.isTimeout();
        if (isTimeout) {
            if (broker.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("polling {}, result POLLING_TIMEOUT", remotingCommand);
            }
            return POLLING_TIMEOUT;
        }
        String key = KeyBuilder.buildConsumeKey(requestHeader.getTopic(), requestHeader.getConsumerGroup(),
            requestHeader.getQueueId());
        ConcurrentSkipListSet<PopRequest> queue = pollingMap.get(key);
        if (queue == null) {
            queue = new ConcurrentSkipListSet<>(PopRequest.COMPARATOR);
            ConcurrentSkipListSet<PopRequest> old = pollingMap.putIfAbsent(key, queue);
            if (old != null) {
                queue = old;
            }
        } else {
            // check size
            int size = queue.size();
            if (size > broker.getBrokerConfig().getPopPollingSize()) {
                POP_LOGGER.info("polling {}, result POLLING_FULL, singleSize:{}", remotingCommand, size);
                return POLLING_FULL;
            }
        }
        if (queue.add(request)) {
            remotingCommand.setSuspended(true);
            totalPollingNum.incrementAndGet();
            if (broker.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("polling {}, result POLLING_SUC", remotingCommand);
            }
            return POLLING_SUC;
        } else {
            POP_LOGGER.info("polling {}, result POLLING_FULL, add fail, {}", request, queue);
            return POLLING_FULL;
        }
    }

    public ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>> getPollingMap() {
        return pollingMap;
    }

    private void cleanUnusedResource() {
        try {
            cleanTopicCidMap();
            cleanPollingMap();
        } catch (Throwable e) {
            POP_LOGGER.error("cleanUnusedResource", e);
        }

        lastCleanTime = System.currentTimeMillis();
    }

    private void cleanTopicCidMap() {
        Iterator<Map.Entry<String, ConcurrentHashMap<String, Byte>>> topicCidMapIter = topicCidMap.entrySet().iterator();
        while (topicCidMapIter.hasNext()) {
            Map.Entry<String, ConcurrentHashMap<String, Byte>> entry = topicCidMapIter.next();
            String topic = entry.getKey();
            if (broker.getTopicConfigManager().selectTopicConfig(topic) == null) {
                POP_LOGGER.info("remove not exit topic {} in topicCidMap!", topic);
                topicCidMapIter.remove();
                continue;
            }
            Iterator<Map.Entry<String, Byte>> cidMapIter = entry.getValue().entrySet().iterator();
            while (cidMapIter.hasNext()) {
                Map.Entry<String, Byte> cidEntry = cidMapIter.next();
                String cid = cidEntry.getKey();
                if (!broker.getSubscriptionGroupManager().getSubscriptionGroupTable().containsKey(cid)) {
                    POP_LOGGER.info("remove not exit sub {} of topic {} in topicCidMap!", cid, topic);
                    cidMapIter.remove();
                }
            }
        }
    }

    private void cleanPollingMap() {
        Iterator<Map.Entry<String, ConcurrentSkipListSet<PopRequest>>> pollingMapIter = pollingMap.entrySet().iterator();
        while (pollingMapIter.hasNext()) {
            Map.Entry<String, ConcurrentSkipListSet<PopRequest>> entry = pollingMapIter.next();
            if (entry.getKey() == null) {
                continue;
            }
            String[] keyArray = entry.getKey().split(PopConstants.SPLIT);
            if (keyArray.length != 3) {
                continue;
            }
            String topic = keyArray[0];
            String cid = keyArray[1];
            if (broker.getTopicConfigManager().selectTopicConfig(topic) == null) {
                POP_LOGGER.info("remove not exit topic {} in pollingMap!", topic);
                pollingMapIter.remove();
                continue;
            }
            if (!broker.getSubscriptionGroupManager().getSubscriptionGroupTable().containsKey(cid)) {
                POP_LOGGER.info("remove not exit sub {} of topic {} in pollingMap!", cid, topic);
                pollingMapIter.remove();
            }
        }
    }

}
