//package com.example.kafka_consumer_demo.config;
//
//import com.enterprise.OrderCheckout.avro.OrderCheckoutSubmittedEvent;
//import com.enterprise.order.avro.OrderEvent;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
//import org.springframework.kafka.listener.DefaultErrorHandler;
//import org.springframework.util.backoff.FixedBackOff;
//
//@Configuration
//public class ErrorHandlerConfig {
//    private static final Logger log= LoggerFactory.getLogger(ErrorHandlerConfig.class);
//
//    @Bean
//    public DefaultErrorHandler errorHandler(KafkaTemplate<String, OrderCheckoutSubmittedEvent> template) {
//        // Automatically route permanently failed messages to an underlying .DLQ topic
//        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
//
//        // Retry 3 times, waiting 2000ms (2 seconds) between each attempt
//        FixedBackOff backOff = new FixedBackOff(2000L, 3);
//
//        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
//
//        // Log errors cleanly during retries
//        errorHandler.setLogLevel(org.springframework.kafka.KafkaException.Level.WARN);
//
//        return errorHandler;
//    }
//}
