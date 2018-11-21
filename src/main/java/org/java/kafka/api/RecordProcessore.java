package org.java.kafka.api;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface RecordProcessore<K, T> {
    void process(ConsumerRecords<K, T> records);
}
