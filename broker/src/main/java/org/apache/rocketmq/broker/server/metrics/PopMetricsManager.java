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
package org.apache.rocketmq.broker.server.metrics;

import com.google.common.collect.Lists;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.ViewBuilder;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.rocketmq.broker.server.Broker;
import org.apache.rocketmq.broker.server.daemon.pop.PopBufferMergeThread;
import org.apache.rocketmq.broker.server.daemon.pop.PopReviveThread;
import org.apache.rocketmq.common.lang.Pair;
import org.apache.rocketmq.common.app.metrics.NopLongCounter;
import org.apache.rocketmq.common.app.metrics.NopLongHistogram;
import org.apache.rocketmq.store.api.dto.PutMessageStatus;
import org.apache.rocketmq.store.api.broker.pop.AckMsg;
import org.apache.rocketmq.store.api.broker.pop.PopCheckPoint;

import static org.apache.rocketmq.broker.server.metrics.PopMetricsConstant.COUNTER_POP_REVIVE_IN_MESSAGE_TOTAL;
import static org.apache.rocketmq.broker.server.metrics.PopMetricsConstant.COUNTER_POP_REVIVE_OUT_MESSAGE_TOTAL;
import static org.apache.rocketmq.broker.server.metrics.PopMetricsConstant.COUNTER_POP_REVIVE_RETRY_MESSAGES_TOTAL;
import static org.apache.rocketmq.broker.server.metrics.PopMetricsConstant.GAUGE_POP_CHECKPOINT_BUFFER_SIZE;
import static org.apache.rocketmq.broker.server.metrics.PopMetricsConstant.GAUGE_POP_OFFSET_BUFFER_SIZE;
import static org.apache.rocketmq.broker.server.metrics.PopMetricsConstant.GAUGE_POP_REVIVE_LAG;
import static org.apache.rocketmq.broker.server.metrics.PopMetricsConstant.GAUGE_POP_REVIVE_LATENCY;
import static org.apache.rocketmq.broker.server.metrics.PopMetricsConstant.HISTOGRAM_POP_BUFFER_SCAN_TIME_CONSUME;
import static org.apache.rocketmq.broker.server.metrics.PopMetricsConstant.LABEL_PUT_STATUS;
import static org.apache.rocketmq.broker.server.metrics.PopMetricsConstant.LABEL_QUEUE_ID;
import static org.apache.rocketmq.broker.server.metrics.PopMetricsConstant.LABEL_REVIVE_MESSAGE_TYPE;

public class PopMetricsManager {
    public static Supplier<AttributesBuilder> attributesBuilderSupplier;

    private static LongHistogram popBufferScanTimeConsume = new NopLongHistogram();
    private static LongCounter popRevivePutTotal = new NopLongCounter();
    private static LongCounter popReviveGetTotal = new NopLongCounter();
    private static LongCounter popReviveRetryMessageTotal = new NopLongCounter();

    public static List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView() {
        List<Double> rpcCostTimeBuckets = Arrays.asList(
            (double) Duration.ofMillis(1).toMillis(),
            (double) Duration.ofMillis(10).toMillis(),
            (double) Duration.ofMillis(100).toMillis(),
            (double) Duration.ofSeconds(1).toMillis(),
            (double) Duration.ofSeconds(2).toMillis(),
            (double) Duration.ofSeconds(3).toMillis()
        );
        InstrumentSelector popBufferScanTimeConsumeSelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_POP_BUFFER_SCAN_TIME_CONSUME)
            .build();
        ViewBuilder popBufferScanTimeConsumeViewBuilder = View.builder()
            .setAggregation(Aggregation.explicitBucketHistogram(rpcCostTimeBuckets));

        return Lists.newArrayList(new Pair<>(popBufferScanTimeConsumeSelector, popBufferScanTimeConsumeViewBuilder));
    }

    public static void initMetrics(Meter meter, Broker broker,
        Supplier<AttributesBuilder> attributesBuilderSupplier) {
        PopMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;

        popBufferScanTimeConsume = meter.histogramBuilder(HISTOGRAM_POP_BUFFER_SCAN_TIME_CONSUME)
            .setDescription("Time consuming of pop buffer scan")
            .setUnit("milliseconds")
            .ofLongs()
            .build();
        popRevivePutTotal = meter.counterBuilder(COUNTER_POP_REVIVE_IN_MESSAGE_TOTAL)
            .setDescription("Total number of put message to revive topic")
            .build();
        popReviveGetTotal = meter.counterBuilder(COUNTER_POP_REVIVE_OUT_MESSAGE_TOTAL)
            .setDescription("Total number of get message from revive topic")
            .build();
        popReviveRetryMessageTotal = meter.counterBuilder(COUNTER_POP_REVIVE_RETRY_MESSAGES_TOTAL)
            .setDescription("Total number of put message to pop retry topic")
            .build();

        meter.gaugeBuilder(GAUGE_POP_OFFSET_BUFFER_SIZE)
            .setDescription("Time number of buffered offset")
            .ofLongs()
            .buildWithCallback(measurement -> calculatePopBufferOffsetSize(broker, measurement));
        meter.gaugeBuilder(GAUGE_POP_CHECKPOINT_BUFFER_SIZE)
            .setDescription("The number of buffered checkpoint")
            .ofLongs()
            .buildWithCallback(measurement -> calculatePopBufferCkSize(broker, measurement));
        meter.gaugeBuilder(GAUGE_POP_REVIVE_LAG)
            .setDescription("The processing lag of revive topic")
            .setUnit("milliseconds")
            .ofLongs()
            .buildWithCallback(measurement -> calculatePopReviveLag(broker, measurement));
        meter.gaugeBuilder(GAUGE_POP_REVIVE_LATENCY)
            .setDescription("The processing latency of revive topic")
            .setUnit("milliseconds")
            .ofLongs()
            .buildWithCallback(measurement -> calculatePopReviveLatency(broker, measurement));
    }

    private static void calculatePopBufferOffsetSize(Broker broker,
        ObservableLongMeasurement measurement) {
        PopBufferMergeThread popBufferMergeThread = broker.getBrokerNettyServer().getPopServiceManager().getPopBufferMergeService();
        measurement.record(popBufferMergeThread.getOffsetTotalSize(), newAttributesBuilder().build());
    }

    private static void calculatePopBufferCkSize(Broker broker,
        ObservableLongMeasurement measurement) {
        PopBufferMergeThread popBufferMergeThread = broker.getBrokerNettyServer().getPopServiceManager().getPopBufferMergeService();
        measurement.record(popBufferMergeThread.getBufferedCKSize(), newAttributesBuilder().build());
    }

    private static void calculatePopReviveLatency(Broker broker,
        ObservableLongMeasurement measurement) {
        PopReviveThread[] popReviveThreads = broker.getBrokerNettyServer().getPopServiceManager().getPopReviveServices();
        for (PopReviveThread popReviveThread : popReviveThreads) {
            measurement.record(popReviveThread.getReviveBehindMillis(), newAttributesBuilder()
                .put(LABEL_QUEUE_ID, popReviveThread.getQueueId())
                .build());
        }
    }

    private static void calculatePopReviveLag(Broker broker,
        ObservableLongMeasurement measurement) {
        PopReviveThread[] popReviveThreads = broker.getBrokerNettyServer().getPopServiceManager().getPopReviveServices();
        for (PopReviveThread popReviveThread : popReviveThreads) {
            measurement.record(popReviveThread.getReviveBehindMessages(), newAttributesBuilder()
                .put(LABEL_QUEUE_ID, popReviveThread.getQueueId())
                .build());
        }
    }

    public static void incPopReviveAckPutCount(AckMsg ackMsg, PutMessageStatus status) {
        incPopRevivePutCount(ackMsg.getConsumerGroup(), ackMsg.getTopic(), PopReviveMessageType.ACK, status, 1);
    }

    public static void incPopReviveCkPutCount(PopCheckPoint checkPoint, PutMessageStatus status) {
        incPopRevivePutCount(checkPoint.getCId(), checkPoint.getTopic(), PopReviveMessageType.CK, status, 1);
    }

    public static void incPopRevivePutCount(String group, String topic, PopReviveMessageType messageType,
        PutMessageStatus status, int num) {
        Attributes attributes = newAttributesBuilder()
            .put(BrokerMetricsConstant.LABEL_CONSUMER_GROUP, group)
            .put(BrokerMetricsConstant.LABEL_TOPIC, topic)
            .put(LABEL_REVIVE_MESSAGE_TYPE, messageType.name())
            .put(LABEL_PUT_STATUS, status.name())
            .build();
        popRevivePutTotal.add(num, attributes);
    }

    public static void incPopReviveAckGetCount(AckMsg ackMsg, int queueId) {
        incPopReviveGetCount(ackMsg.getConsumerGroup(), ackMsg.getTopic(), PopReviveMessageType.ACK, queueId, 1);
    }

    public static void incPopReviveCkGetCount(PopCheckPoint checkPoint, int queueId) {
        incPopReviveGetCount(checkPoint.getCId(), checkPoint.getTopic(), PopReviveMessageType.CK, queueId, 1);
    }

    public static void incPopReviveGetCount(String group, String topic, PopReviveMessageType messageType, int queueId,
        int num) {
        AttributesBuilder builder = newAttributesBuilder();
        Attributes attributes = builder
            .put(BrokerMetricsConstant.LABEL_CONSUMER_GROUP, group)
            .put(BrokerMetricsConstant.LABEL_TOPIC, topic)
            .put(LABEL_QUEUE_ID, queueId)
            .put(LABEL_REVIVE_MESSAGE_TYPE, messageType.name())
            .build();
        popReviveGetTotal.add(num, attributes);
    }

    public static void incPopReviveRetryMessageCount(PopCheckPoint checkPoint, PutMessageStatus status) {
        AttributesBuilder builder = newAttributesBuilder();
        Attributes attributes = builder
            .put(BrokerMetricsConstant.LABEL_CONSUMER_GROUP, checkPoint.getCId())
            .put(BrokerMetricsConstant.LABEL_TOPIC, checkPoint.getTopic())
            .put(LABEL_PUT_STATUS, status.name())
            .build();
        popReviveRetryMessageTotal.add(1, attributes);
    }

    public static void recordPopBufferScanTimeConsume(long time) {
        popBufferScanTimeConsume.record(time, newAttributesBuilder().build());
    }

    public static AttributesBuilder newAttributesBuilder() {
        return attributesBuilderSupplier != null ? attributesBuilderSupplier.get() : Attributes.builder();
    }
}
