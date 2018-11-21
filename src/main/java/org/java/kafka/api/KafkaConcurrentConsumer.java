package org.java.kafka.api;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaConcurrentConsumer<K, V> {
    private static final AtomicInteger CONCURRENT_CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private static final AtomicInteger CONCURRENT_CONSUMER_GROUP_ID_SEQUENCE = new AtomicInteger(1);
    private String clientId;
    private String groupId;
    private Properties prop;
    private int numConsumer;
    private Collection<String> topics=new ArrayList<String>();
    private Collection<TopicPartition> partitions=new ArrayList<TopicPartition>();
    private int pollValue = 100;
    private ConsumerTask<?, ?> consumerTask;


    private KafkaConcurrentConsumer(Builder builder) {
        this.clientId = (String) builder.prop.get(ConsumerConfig.CLIENT_ID_CONFIG);
        if (this.clientId.isEmpty()) {
            this.clientId = "concurrent-consumer-" + CONCURRENT_CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement();
            builder.prop.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, this.clientId);
        }
        this.groupId=(String)builder.prop.get(ConsumerConfig.GROUP_ID_CONFIG);
        if(this.groupId.isEmpty()){
            this.groupId="group-"+CONCURRENT_CONSUMER_GROUP_ID_SEQUENCE.getAndIncrement();
            builder.prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        }
        this.prop = builder.prop;
        this.numConsumer = builder.numConsumer;
        this.topics = builder.topics;
        this.partitions = builder.partitions;
        this.pollValue = builder.pollValue;
        KafkaConsumer<K, V> consumer = new KafkaConsumer<K, V>(this.prop);
        for (String topic : topics) {
            List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
            for (PartitionInfo partitionInfo : partitionInfoList) {
                this.partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            }
        }
        consumer.close();
        if (this.numConsumer == 0) {
            this.numConsumer = this.partitions.size();
        }

    }

    public void consume(ConsumerTask<K, V> consumerTask) {
        this.consumerTask=consumerTask;
        ConsumerExecutor.createConsumerExecutor(this).start();
    }


    public Properties getProp() {
        return this.prop;
    }

    public int getNumConsumer() {
        return this.numConsumer;
    }

    public Collection<String> getTopics() {
        return this.topics;
    }

    public Collection<TopicPartition> getPartitions() {
        return this.partitions;
    }

    public int getPollValue() {
        return this.pollValue;
    }

    public ConsumerTask<?, ?> getConsumerTask() {
        return this.consumerTask;
    }

    public static class Builder {
        private Properties prop;
        private int numConsumer;
        private Collection<String> topics = new ArrayList<String>();
        private Collection<TopicPartition> partitions = new ArrayList<TopicPartition>();
        private int pollValue = 100;

        public Builder(Properties prop) {
            this.prop = prop;
        }

        public Builder numConsumer(int numConsumer) {
            this.numConsumer = numConsumer;
            return this;
        }

        public Builder topics(Collection<String> topics) {
            this.topics = topics;
            return this;
        }

        public Builder partitions(Collection<TopicPartition> partitions) {
            this.partitions = partitions;
            return this;
        }

        public Builder PollValue(int pollValue) {
            this.pollValue = pollValue;
            return this;
        }


        public KafkaConcurrentConsumer create() {
            return new KafkaConcurrentConsumer(this);
        }

    }

}
