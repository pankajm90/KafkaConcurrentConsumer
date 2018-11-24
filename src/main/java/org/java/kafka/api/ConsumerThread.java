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

    public static List<ConsumerThread> createConsumerThread(KafkaConcurrentConsumer<?, ?> kafkaConcurrentConsumer)throws CloneNotSupportedException {
        int numConsumer = kafkaConcurrentConsumer.getNumConsumer();
        return newThreads(kafkaConcurrentConsumer, numConsumer);
    }

    private static List<ConsumerThread> newThreads(KafkaConcurrentConsumer<?, ?> kafkaConcurrentConsumer, int numConsumer) throws CloneNotSupportedException {
        List<ConsumerThread> consumerThreads = new ArrayList<ConsumerThread>();

        for (int i = 0; i < numConsumer; i++)
            consumerThreads.add(newInstance(kafkaConcurrentConsumer));
        return consumerThreads;
    }

    private static ConsumerThread newInstance(KafkaConcurrentConsumer<?, ?> kafkaConcurrentConsumer) throws CloneNotSupportedException{
        return new ConsumerThread(kafkaConcurrentConsumer.getProp(), kafkaConcurrentConsumer.getPollValue(), kafkaConcurrentConsumer.getRecordProcessore());
    }

    private ConsumerThread(Properties properties, int pollValue, RecordProcessore<K, V> recordProcessore) throws CloneNotSupportedException {
        this.properties = (Properties) properties.clone();
        this.pollValue = pollValue;
        setClientId();
        setRecordProcessore(recordProcessore);
    }

    private void setClientId() {
        this.clientId = this.properties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG) + "-C" + CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement();
        this.properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, this.clientId);
    }

    private void setRecordProcessore(RecordProcessore<K, V> recordProcessore) throws CloneNotSupportedException {
        try{
            this.recordProcessore = recordProcessore.clone();
        }catch(CloneNotSupportedException clex){
            clex.printStackTrace();
            throw new CloneNotSupportedException();
        }
    }

    public void addTopicPartition(TopicPartition topicPartition) {
        this.topicPartitions.add(topicPartition);
    }

    public void run() {
        createKafkaConsumer();
        assignTopic();
        startConsumer();
    }

    public void stop(){
        this.recordProcessore.stop();
    }

    private void createKafkaConsumer() {
        System.out.println(String.format("'%s' initialized ",this.clientId));
        this.recordProcessore.initalize(this.properties);
    }

    private void assignTopic() {
        this.recordProcessore.assignTopic(this.topicPartitions);
    }

    private void startConsumer() {
        this.recordProcessore.startConsumer(this.pollValue);
    }

}
