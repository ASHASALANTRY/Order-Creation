package com.example.justfordependency.config;

import com.enterprise.order.avro.OrderEvent;
import com.example.justfordependency.dto.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class OrderEventPublisher {
    private static final Logger log= LoggerFactory.getLogger(OrderEventPublisher.class);
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderEventPublisher(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
/*When you MUST explicitly create a ProducerRecordYou should skip the shortcut and manually instantiate
 a ProducerRecord if you need to manipulate low-level metadata before sending the message.
 The three most common reasons are:
 Adding Custom Headers: Sending tracing metadata (like a Zipkin/Jaeger Correlation ID for distributed microservice tracing) or adding
 security tokens.
 Explicit Partition Targeting: Forcing a message to go to a specific partition number (e.g., Partition 2),
  overriding Kafka's default hashing strategy.
Manual Timestamps: Overriding the message creation timestamp with a custom or historic date.*/

    /*public void publishWithHeaders(OrderEvent event, String correlationId) {

    // 1. Manually create the ProducerRecord
    ProducerRecord<String, OrderEvent> record = new ProducerRecord<>(
        "order-events", // Topic
        null,           // Partition (null lets Kafka hash the key)
        event.orderId(),// Key
        event           // Value
    );

    // 2. Inject custom metadata headers
    record.headers().add(new RecordHeader("X-Correlation-ID", correlationId.getBytes()));
    record.headers().add(new RecordHeader("X-App-Version", "v1.2.0".getBytes()));

    // 3. Pass the raw record straight to KafkaTemplate
    kafkaTemplate.send(record);
}*/
    public void publish(Order event, Boolean useNewSchema) {
        // Use orderId as the partition key to guarantee order processing sequence
        OrderEvent avroPayload = OrderEvent.newBuilder()
                .setOrderId(event.orderId())
                .setStatus(event.status())
                .setAmount(event.amount())
                .setCustomerNotes(null)
                .build();
        if(useNewSchema)
            avroPayload.setCustomerNotes("created");
        CompletableFuture<SendResult<String,OrderEvent>> future=
                kafkaTemplate.send("order-events",avroPayload.getOrderId(),avroPayload);
        // Production Best Practice: Always handle responses asynchronously via callbacks
        future.whenComplete((result,ex)->{
            if(ex==null){
                log.info("message sent successfully to partition[{}] with offset[{}]",
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }  else{
                log .error("failed to send message to kafka due to error",ex);
                // Production tip: Here you would push data to a fallback database or a retry table
            }
        });
    }
}
