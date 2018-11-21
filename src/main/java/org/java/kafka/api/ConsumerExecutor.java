package org.java.kafka.api;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerExecutor {
    private ExecutorService executorService;
    private List<ConsumerThread> consumerThreads;

    public static ConsumerExecutor createConsumerExecutor(KafkaConcurrentConsumer<?, ?> kafkaConcurrentConsumer){
        return new ConsumerExecutor(kafkaConcurrentConsumer);
    }

    private ConsumerExecutor(KafkaConcurrentConsumer<?, ?> kafkaConcurrentConsumer) {
        int threadCount = kafkaConcurrentConsumer.getNumConsumer();
        this.executorService = Executors.newFixedThreadPool(threadCount);
        this.consumerThreads = ConsumerThread.createConsumerThread(kafkaConcurrentConsumer);
        assignPartitioms(kafkaConcurrentConsumer.getPartitions());
    }

    private void assignPartitioms(Collection<TopicPartition> partitions){
        Iterator<ConsumerThread> iterator = this.consumerThreads.iterator();
        for (TopicPartition tp : partitions) {
            if (iterator.hasNext())
                iterator.next().addTopicPartition(tp);
            else {
                iterator = consumerThreads.iterator();
                if (iterator.hasNext())
                    iterator.next().addTopicPartition(tp);
            }
        }
    }

    public void start() {
        for(ConsumerThread ct: this.consumerThreads)
            this.executorService.execute(ct);
    }

    public void stop(){
        this.executorService.shutdown();
    }
}
