package com.example.kafka_consumer_demo.config;

import com.enterprise.order.avro.OrderEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Component
public class CustomRebalanceListener implements ConsumerAwareRebalanceListener {
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            System.out.println("Assigned to partition: " + partition.partition() + " of topic: " + partition.topic());
        }
    }
}
