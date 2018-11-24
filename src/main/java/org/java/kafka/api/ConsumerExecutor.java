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

    public static ConsumerExecutor createConsumerExecutor(KafkaConcurrentConsumer<?, ?> kafkaConcurrentConsumer) throws Exception{
        return new ConsumerExecutor(kafkaConcurrentConsumer);
    }

    private ConsumerExecutor(KafkaConcurrentConsumer<?, ?> kafkaConcurrentConsumer)throws Exception {
        int threadCount = kafkaConcurrentConsumer.getNumConsumer();
        this.executorService = Executors.newFixedThreadPool(threadCount);
        this.consumerThreads = ConsumerThread.createConsumerThread(kafkaConcurrentConsumer);
        assignPartitions(kafkaConcurrentConsumer.getPartitions());
    }

    private void assignPartitions(Collection<TopicPartition> partitions){
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
        System.out.println("KafkaConcurrentConsumer stopped");
        this.executorService.shutdown();
    }
}
