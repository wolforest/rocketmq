package org.apache.rocketmq.apitest.manager;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.apache.rocketmq.client.apis.producer.TransactionChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerManager {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerManager.class);
    public static Producer buildProducer(String... topics) {
        return buildProducer(null, null, topics);
    }

    public static Producer buildTransactionalProducer(TransactionChecker checker, String... topics) {
        return buildProducer(null, checker, topics);
    }

    public static Producer buildTransactionalProducer(String accountName, TransactionChecker checker, String... topics) {
        return buildProducer(accountName, checker, topics);
    }

    public static Producer buildProducer(String accountName, TransactionChecker checker, String... topics) {
        ClientConfiguration clientConfig = ConfigManager.buildClientConfig(accountName);

        try {
            ProducerBuilder builder = ClientManager.getProvider()
                .newProducerBuilder()
                .setClientConfiguration(clientConfig);

            if (null != topics && topics.length > 0) {
                builder.setTopics(topics);
            }

            if (null != checker) {
                builder.setTransactionChecker(checker);
            }

            return builder.build();
        } catch (ClientException e) {
            LOG.warn("can't connect to MQ server");
            return null;
        }
    }
}
