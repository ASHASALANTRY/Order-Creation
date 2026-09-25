package com.example.justfordependency.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public KafkaTransactionManager<String, Object> kafkaTransactionManager(
            ProducerFactory<String, Object> producerFactory) {
        return new KafkaTransactionManager<>(producerFactory);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            KafkaTransactionManager<String, Object> kafkaTransactionManager) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // The container manages the transaction boundary using this manager
        factory.getContainerProperties().setKafkaAwareTransactionManager(kafkaTransactionManager);
        return factory;
    }


    @Bean
    public NewTopic orderEventTopic(){
        return TopicBuilder.name("order-events")
                .partitions(3)
                .replicas(1)
                .build();
    }
    @Bean
    public NewTopic inventoryTopic(){
        return TopicBuilder.name("inventory-updates")
                .partitions(2)
                .replicas(1)
                .build();
    }
    @Bean
    public NewTopic orderCheckoutTopic(){
        return TopicBuilder.name("order-checkout-events").partitions(2).replicas(1).build();
    }
}
