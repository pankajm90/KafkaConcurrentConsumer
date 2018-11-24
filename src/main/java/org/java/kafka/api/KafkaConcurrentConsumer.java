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
    private RecordProcessore<?, ?> recordProcessore;
    private ConsumerExecutor consumerExecutor;

    private KafkaConcurrentConsumer(Builder builder) {
        setGroupId(builder);
        setClientId(builder);
        this.prop = builder.prop;
        this.numConsumer = builder.numConsumer;
        this.topics = builder.topics;
        this.partitions = builder.partitions;
        this.pollValue = builder.pollValue;
        setPartitions();
        setNumConsumer();
    }

    private void setGroupId(Builder builder) {
        this.groupId=(String)builder.prop.get(ConsumerConfig.GROUP_ID_CONFIG);
        if(isGroupIdPresent(this.groupId)){
            this.groupId=newGroupId();
            builder.prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        }
    }

    private boolean isGroupIdPresent(String p) {
        return p == null || p.isEmpty();
    }

    private String newGroupId(){
        return "group-"+CONCURRENT_CONSUMER_GROUP_ID_SEQUENCE.getAndIncrement();
    }

    private void setClientId(Builder builder) {
        this.clientId = (String) builder.prop.get(ConsumerConfig.CLIENT_ID_CONFIG);
        if (isClientIdPresent(this.clientId)) {
            this.clientId = newClientId();
            builder.prop.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, this.clientId);
        }
    }

    private boolean isClientIdPresent(String p) {
        return p == null || p.isEmpty();
    }

    private String newClientId(){
        return "concurrent-consumer-" + CONCURRENT_CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement();
    }

    private void setPartitions() {
        KafkaConsumer<K, V> consumer = new KafkaConsumer<K, V>(this.prop);

        for (String topic : topics)
            addPartitions(consumer, topic);

        consumer.close();
    }

    private void addPartitions(KafkaConsumer<K, V> consumer, String topic) {
        List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);

        for (PartitionInfo partitionInfo : partitionInfoList)
            this.partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
    }

    private void setNumConsumer() {
        if (this.numConsumer == 0) {
            this.numConsumer = this.partitions.size();
        }
    }

    public void consume(RecordProcessore<K, V> recordProcessore) throws Exception {
        this.recordProcessore = recordProcessore;
        this.consumerExecutor = ConsumerExecutor.createConsumerExecutor(this);
        this.consumerExecutor.start();
    }

    public void shutdown(){
        this.consumerExecutor.stop();
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

    public RecordProcessore<?, ?> getRecordProcessore() {
        return this.recordProcessore;
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
