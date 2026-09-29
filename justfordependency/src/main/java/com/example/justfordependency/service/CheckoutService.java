package com.example.justfordependency.service;

import com.enterprise.OrderCheckout.avro.CheckoutItemAvro;
import com.enterprise.OrderCheckout.avro.OrderCheckoutSubmittedEvent;
import com.enterprise.OrderCheckout.avro.ShippingDetailsAvro;
import com.example.justfordependency.dto.CheckoutOrderRequest;
import com.example.justfordependency.dto.OrderItemRequest;
import com.example.justfordependency.dto.ShippingAddressRequest;
import com.example.justfordependency.exception.custom.KafkaException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class CheckoutService {
    private static final Logger log= LoggerFactory.getLogger(CheckoutService.class);
    private final KafkaTemplate<String, OrderCheckoutSubmittedEvent> kafkaTemplate;
    private static final String KAFKA_TOPIC = "order-checkout-events";
    private static final String STATIC_EVENT_TYPE = "ORDER_CHECKOUT_SUBMITTED";
    private final RedisTemplate<String,String> redisTemplate;


    public CheckoutService(KafkaTemplate<String, OrderCheckoutSubmittedEvent> kafkaTemplate,RedisTemplate<String, String> redisTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.redisTemplate = redisTemplate;

    }

    public String initialOrderFlow(String orderId, CheckoutOrderRequest request){

            String idempotencyKey = MDC.get("idempotencyKey");
            if (idempotencyKey == null) {
                throw new IllegalArgumentException("Idempotency key is missing from context.");
            }
        try {
            OrderCheckoutSubmittedEvent orderCheckoutSubmittedEvent = OrderCheckoutSubmittedEvent.newBuilder().
                    setEventId(idempotencyKey).
                    setEventType(STATIC_EVENT_TYPE).
                    setTimestamp(Instant.now()).setOrderId(orderId).setCustomerId(request.getCustomerId())
                    .setItems(mapItems(request.getItems())).
                    setShippingDetails(mapShippingAddress(request.getShippingAddress())).build();



            ProducerRecord<String, OrderCheckoutSubmittedEvent> producerRecord=new ProducerRecord<>(KAFKA_TOPIC, orderId, orderCheckoutSubmittedEvent);
            // Retrieve the key instantly from the current thread context
            producerRecord.headers().add("idempotencyKey",idempotencyKey.getBytes(StandardCharsets.UTF_8));
            SendResult<String, OrderCheckoutSubmittedEvent> result = kafkaTemplate.send(producerRecord)     .get(10, TimeUnit.SECONDS);
            log.info("Event sent successfully to partition [{}] with offset [{}]",
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());

            // Update Redis status to completed since everything succeeded
            try {
                redisTemplate.opsForValue().set(idempotencyKey, "PUBLISHED", 1, TimeUnit.DAYS);
            } catch (Exception e) {
                log.error("Failed to update final status in Redis for key: {}", idempotencyKey, e);
                // We don't fail the order just because Redis status write failed at the end
            }

        } catch (Exception e) {
            log.error("Checkout process failed for order [{}]. Initiating state rollback.", orderId, e);

            // Clean up Redis synchronously so the user can immediately retry their request
            try {
                redisTemplate.delete(idempotencyKey);
            } catch (Exception redisEx) {
                log.error("Failed to clean up idempotency key from Redis during exception handling", redisEx);
            }

            // Propagate the failure so Spring can roll back the Kafka transaction.
            throw new KafkaException("Failed to publish order checkout event.",e);
        }

        return "Your request is successfully Published";
    }
    private ShippingDetailsAvro mapShippingAddress(
            ShippingAddressRequest address) {

        return ShippingDetailsAvro.newBuilder()
                .setStreet(address.getStreet())
                .setCity(address.getCity())
                .setPostalCode(address.getPostalCode())
                .setCountry(address.getCountry())
                .build();
    }
    private List<CheckoutItemAvro> mapItems(
            List<OrderItemRequest> items) {

        return items.stream()
                .map(item ->
                        CheckoutItemAvro.newBuilder()
                                .setSku(item.getSku())
                                .setQuantity(item.getQuantity())
                                .setPrice(
                                        item.getUnitPrice().doubleValue())
                                .build())
                .toList();
    }
}
