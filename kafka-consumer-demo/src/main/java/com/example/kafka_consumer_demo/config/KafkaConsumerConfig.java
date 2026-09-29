package com.example.kafka_consumer_demo.config;

import com.enterprise.OrderCheckout.avro.OrderCheckoutSubmittedEvent;

import com.example.kafka_consumer_demo.service.OrderProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Objects;

@Component
public class KafkaConsumerConfig {
    private static final Logger log=LoggerFactory.getLogger(KafkaConsumerConfig.class);
    private final OrderProcessingService orderProcessingService;

    public KafkaConsumerConfig(OrderProcessingService orderProcessingService) {
        this.orderProcessingService = orderProcessingService;
    }

    // concurrency = "3" spins up 3 separate consumer threads for parallel processing

    @RetryableTopic(
            attempts = "3",
            backOff = @BackOff(delay = 2000, multiplier = 2.0),
            dltStrategy = DltStrategy.FAIL_ON_ERROR
//            include = { java.sql.SQLException.class }
    )
        @KafkaListener(topics="order-checkout-events", groupId="order-processing-group"/*, concurrency = "3"*/)
    public void orderConsume(ConsumerRecord<String, OrderCheckoutSubmittedEvent> record,
                             Acknowledgment acknowledgment,
                             @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
            log.info("received checked out order partition[{}] at offset [{}] with key [{}]",
                    record.partition(),record.offset(),record.key());
    OrderCheckoutSubmittedEvent checkoutSubmittedEvent=record.value();

    org.apache.kafka.common.header.Header idempotencyKeyHeader=record.headers().lastHeader("idempotencyKey");
    String idempotencyKey = "";
    if(Objects.nonNull(idempotencyKeyHeader))
        idempotencyKey=new String(idempotencyKeyHeader.value(), StandardCharsets.UTF_8);

   log.info("Received header idempotencyKey: {}",idempotencyKey);

        orderProcessingService.process(checkoutSubmittedEvent,idempotencyKey);

    acknowledgment.acknowledge();
    }

    @DltHandler
    public void handleDeadLetterMessage(
            ConsumerRecord<String, String> record,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String errorMessage) {

        log.error("CRITICAL: Message with key {} from topic {} moved to DLQ. Reason: {}",
                record.key(), topic, errorMessage);
    }



}
