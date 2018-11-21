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

public class ConsumerThread<K, V> implements Runnable {
    private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private String clientId;
    private Collection<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
    private int pollValue;
    private Properties properties;
    private RecordProcessore<K, V> recordProcessore;

    public static List<ConsumerThread> createConsumerThread(KafkaConcurrentConsumer<?, ?> kafkaConcurrentConsumer) {
        List<ConsumerThread> consumerThreads = new ArrayList<ConsumerThread>();
        int numConsumer = kafkaConcurrentConsumer.getNumConsumer();

        for (int i = 0; i < numConsumer; i++)
            consumerThreads.add(newInstance(kafkaConcurrentConsumer));

        return consumerThreads;
    }

    private static ConsumerThread newInstance(KafkaConcurrentConsumer<?, ?> kafkaConcurrentConsumer) {
        return new ConsumerThread(kafkaConcurrentConsumer.getProp(), kafkaConcurrentConsumer.getPollValue(), kafkaConcurrentConsumer.getRecordProcessore());
    }

    private ConsumerThread(Properties properties, int pollValue, RecordProcessore<K, V> recordProcessore) {
        this.properties = (Properties) properties.clone();
        this.clientId = this.properties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG) + "-C" + CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement();
        this.properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, this.clientId);
        this.pollValue = pollValue;
        this.recordProcessore = recordProcessore;
    }

    public void addTopicPartition(TopicPartition topicPartition) {
        this.topicPartitions.add(topicPartition);
    }

    public void run() {
        KafkaConsumer<K, V> kafkaConsumer = createKafkaConsumer();

        while (true)
            processRecord(kafkaConsumer);
    }

    private KafkaConsumer<K, V> createKafkaConsumer() {
        KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<K, V>(this.properties);
        assignTopic(kafkaConsumer);

        return kafkaConsumer;
    }

    private void assignTopic(KafkaConsumer<K, V> kafkaConsumer) {
        kafkaConsumer.assign(this.topicPartitions);
    }

    private void processRecord(KafkaConsumer<K, V> kafkaConsumer) {
        ConsumerRecords<K, V> records = kafkaConsumer.poll(100);
        if(!records.isEmpty())
            System.out.print(this.clientId+": ");
        this.recordProcessore.process(records);
    }


}
