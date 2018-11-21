package org.java.kafka.api;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class ConsumerThread<K, V> implements Runnable {
    private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private String clientId;
    private Collection<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
    private int pollValue;
    private Properties properties;
    private ConsumerTask<K, V> consumerTask;

    private ConsumerThread(Properties properties, int pollValue, ConsumerTask<K, V> consumerTask) {
        this.properties = (Properties) properties.clone();
        this.clientId = this.properties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG) + "-C" + CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement();
        this.properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, this.clientId);
        this.pollValue = pollValue;
        this.consumerTask = consumerTask;
    }

    private static ConsumerThread newInstance(KafkaConcurrentConsumer<?, ?> kafkaConcurrentConsumer) {
        return new ConsumerThread(kafkaConcurrentConsumer.getProp(), kafkaConcurrentConsumer.getPollValue(), kafkaConcurrentConsumer.getConsumerTask());
    }

    public static List<ConsumerThread> createConsumerThread(KafkaConcurrentConsumer<?, ?> kafkaConcurrentConsumer) {
        List<ConsumerThread> consumerThreads = new ArrayList<ConsumerThread>();
        int numConsumer = kafkaConcurrentConsumer.getNumConsumer();
        for (int i = 0; i < numConsumer; i++)
            consumerThreads.add(newInstance(kafkaConcurrentConsumer));
        return consumerThreads;
    }

    public void addTopicPartition(TopicPartition topicPartition) {
        this.topicPartitions.add(topicPartition);
    }

    public void run() {
        KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<K, V>(this.properties);
        kafkaConsumer.assign(this.topicPartitions);
        while (true) {
            ConsumerRecords<K, V> records = kafkaConsumer.poll(100);
            this.consumerTask.execute(records);
        }
    }

    private void consume(){
        KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<K, V>(this.properties);
        kafkaConsumer.assign(this.topicPartitions);
        while (true) {
            ConsumerRecords<K, V> records = kafkaConsumer.poll(100);
            this.consumerTask.execute(records);
        }
    }
}
