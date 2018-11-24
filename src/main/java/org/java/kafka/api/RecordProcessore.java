package org.java.kafka.api;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Properties;

abstract public class RecordProcessore<K, V> implements Cloneable {
    protected String consumerInfo;
    protected KafkaConsumer kafkaConsumer;

    public void initalize(Properties properties) {
        this.consumerInfo=properties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG);
        this.kafkaConsumer = new KafkaConsumer<K, V>(properties);
    }

    final public void assignTopic(Collection<TopicPartition> topicPartitions){
        this.kafkaConsumer.assign(topicPartitions);
    }

    final public void startConsumer(int pollValue){
        while(true)
            process(this.kafkaConsumer.poll(pollValue));
    }

    final public void stop(){
        System.out.println(String.format("%s stopped",consumerInfo));
        this.kafkaConsumer.close();
    }
    public RecordProcessore<K, V> clone() throws CloneNotSupportedException {
        return (RecordProcessore<K, V>)super.clone();
    }


    abstract public void process(ConsumerRecords<K, V> records);
}
