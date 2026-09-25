package com.example.justfordependency.service;

import com.enterprise.OrderCheckout.avro.CheckoutItemAvro;
import com.enterprise.OrderCheckout.avro.OrderCheckoutSubmittedEvent;
import com.enterprise.OrderCheckout.avro.ShippingDetailsAvro;
import com.example.justfordependency.dto.OrderItemDto;
import com.example.justfordependency.dto.ShippingAddressDto;
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
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class CheckoutService {
    private static final Logger log= LoggerFactory.getLogger(CheckoutService.class);
    private final KafkaTemplate<String, OrderCheckoutSubmittedEvent> kafkaTemplate;
    private static final String KAFKA_TOPIC = "order-checkout-events";
    private static final String STATIC_EVENT_ID = "evt_demo_12345";
    private static final String STATIC_ORDER_ID = "ord_demo_67890";
    private static final String STATIC_CUSTOMER_ID = "cust_demo_999";
    private static final String STATIC_EVENT_TYPE = "ORDER_CHECKOUT_SUBMITTED";
    private final RedisTemplate<String,String> redisTemplate;


    public CheckoutService(KafkaTemplate<String, OrderCheckoutSubmittedEvent> kafkaTemplate,RedisTemplate<String, String> redisTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.redisTemplate = redisTemplate;

    }
    @Transactional
    public String initialOrderFlow(String orderId){
        try {
            OrderCheckoutSubmittedEvent orderCheckoutSubmittedEvent = OrderCheckoutSubmittedEvent.newBuilder().setEventId(UUID.randomUUID()).setEventType(STATIC_EVENT_TYPE).setTimestamp(Instant.now()).setOrderId(UUID.randomUUID()).setCustomerId(UUID.randomUUID())
                    .setCartId(null) // Explicitly set missing union fields
                    .setPaymentMethodToken(null) // <-- This prevents the crash
                    .setItems(List.of(CheckoutItemAvro.newBuilder().setProductId(UUID.randomUUID())
                    .setQuantity(1)
                    .setPrice(1200.00).build(), CheckoutItemAvro.newBuilder().setProductId(UUID.randomUUID())
                    .setQuantity(2)
                    .setPrice(25.50).build())).setShippingDetails(ShippingDetailsAvro.newBuilder().setFullName("John Doe")
                    .setAddressLine1("123 Demo Lane")
                    .setCity("Tech City")
                    .setCountry("CA")
                    .setPostalCode("1111").build()).build();
            ProducerRecord<String, OrderCheckoutSubmittedEvent> producerRecord=new ProducerRecord<>(KAFKA_TOPIC, orderId, orderCheckoutSubmittedEvent);
            // Retrieve the key instantly from the current thread context
            String idempotencyKey = MDC.get("idempotencyKey");
            producerRecord.headers().add("idempotencyKey",idempotencyKey.getBytes(StandardCharsets.UTF_8));
            CompletableFuture<SendResult<String, OrderCheckoutSubmittedEvent>> future = kafkaTemplate.send(producerRecord);
            future.whenComplete((result, ex) -> {

                if (ex == null) {
                    log.info("event sent successfully to partition[{}] with offset[{}]",
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                    try{
                        redisTemplate.opsForValue().set(idempotencyKey, "PUBLISHED");
                    }catch(Exception e)
                        {
                            log.error("some thing went wrong with redis");
                        }
                }
                else{
                    log.info("failed to send event to kafka due to error", ex);
                try{
                redisTemplate.delete(idempotencyKey);
            }catch(Exception e)
            {
                log.error("some thing went wrong with redis");
            }
                throw new KafkaException("failed to send event to kafka");
                }
            });
        }catch(Exception e){
            log.error("error while parsing {}",e.getMessage());
            try{
                redisTemplate.delete(MDC.get("idempotencyKey"));
            }catch(Exception ex)
            {
                log.error("some thing went wrong with redis");
            }
            throw new KafkaException("failed to send event to kafka");

        }
        return "Your request is successfully sent to kafka";
    }
    /**
     * Helper to generate a static list of mock items for the demo payload.
     *//*
    private List<OrderItemDto> createMockItems() {
        OrderItemDto item1 = new OrderItemDto();
        item1.setProductId("prod_laptop_001");
        item1.setQuantity(1);
        item1.setPrice(1200.00); // Fixed price mapping

        OrderItemDto item2 = new OrderItemDto();
        item2.setProductId("prod_mouse_002");
        item2.setQuantity(2);
        item2.setPrice(25.50);

        return Arrays.asList(item1, item2);
    }

    *//**
     * Helper to generate a static mock shipping address object.
     *//*
    private ShippingAddressDto createMockShippingDetails() {
        ShippingAddressDto shipping = new ShippingAddressDto();
        shipping.setFullName("John Doe");
        shipping.setAddressLine1("123 Demo Lane");
        shipping.setCity("Tech City");
        shipping.setCountry("CA");
        shipping.setPostalCode("94016");
        return shipping;
    }*/
}
