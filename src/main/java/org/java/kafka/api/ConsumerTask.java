package org.java.kafka.api;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface ConsumerTask<K, T> {
    void execute(ConsumerRecords<K, T> records);
}
