package com.example.kafka_consumer_demo.service;

import com.enterprise.OrderCheckout.avro.OrderCheckoutSubmittedEvent;
import com.example.kafka_consumer_demo.entity.Order;
import com.example.kafka_consumer_demo.entity.OrderItem;
import com.example.kafka_consumer_demo.entity.ProcessedEvent;
import com.example.kafka_consumer_demo.entity.ShippingAddress;
import com.example.kafka_consumer_demo.repository.OrderRepository;
import com.example.kafka_consumer_demo.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class OrderProcessingService {
    private static final Logger log= LoggerFactory.getLogger(OrderProcessingService.class);

   private final OrderRepository orderRepository;
   private final ProcessedEventRepository processedEventRepository;
   private final RedisTemplate<String, String> redisTemplate;

    public OrderProcessingService(OrderRepository orderRepository, ProcessedEventRepository processedEventRepository, RedisTemplate<String, String> redisTemplate) {
        this.orderRepository = orderRepository;
        this.processedEventRepository = processedEventRepository;
        this.redisTemplate = redisTemplate;
    }
    @Transactional
    public void
    process(OrderCheckoutSubmittedEvent event, String idempotencyKey)  {

        if(processedEventRepository.existsById(event.getEventId())){
//            throw new IllegalArgumentException();
            updateRedis(idempotencyKey);
            return;
        }

        Order order=new Order(); // Implement your actual production database updates here
        AtomicReference<OrderItem> orderItem= new AtomicReference<>(new OrderItem());
        ArrayList<OrderItem> orderItems=new ArrayList<>();
        event.getItems().forEach(avroItem->{
            OrderItem item = new OrderItem();
            item.setProductId(avroItem.getProductId());
            item.setQuantity(avroItem.getQuantity());
            item.setPrice(avroItem.getPrice()); // Ensure matching types (Double/BigDecimal)
            order.addItem(item);
        });
        ShippingAddress shippingAddress=new ShippingAddress();
        ArrayList<ShippingAddress> shipping=new ArrayList<>();
//        shippingAddress=objectMapper.convertValue(event.getShippingDetails(),ShippingAddress.class);
        // 2. Manually map shipping details without using ObjectMapper
        if (event.getShippingDetails() != null) {
            var avroShipping = event.getShippingDetails();
            shippingAddress.setFullName(avroShipping.getFullName());
            shippingAddress.setAddressLine1(avroShipping.getAddressLine1());
            shippingAddress.setCity(avroShipping.getCity());
            shippingAddress.setCountry(avroShipping.getCountry());
            shippingAddress.setPostalCode(avroShipping.getPostalCode());
        }
            order.setShippingAddress(shippingAddress);
//        order.setShippingAddress(objectMapper.convertValue(event.getShippingDetails().getSchema(),ShippingAddress.class));
        order.setId(event.getOrderId());
        order.setCustomerId(event.getCustomerId());
        order.setStatus("PENDING");
        ProcessedEvent processedEvent=new ProcessedEvent();
        processedEvent.setEventId(event.getEventId());
        processedEvent.setIdempotencyKey(idempotencyKey);
        processedEventRepository.save(processedEvent);
        orderRepository.save(order);
//        objectMapper.convertValue(OrderCheckoutSubmittedEvent,)
        log.info("Successfully processed order business logic for ID: {}",event);
        System.out.printf("Successfully processed order business logic for ID: %s%n" ,event);
        // Example: If database throws an error here, the ErrorHandler configuration
        // will safely catch it, retry 3 times, and forward to the DLQ if it keeps failing.
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit(){
                updateRedis(idempotencyKey);
            }
        });
    }

    void updateRedis(String idempotencyKey){
        try {
            redisTemplate.opsForValue().set(idempotencyKey, "PROCESSED");
        }catch (Exception e){
            log.error("some issue with redis");}
    }
}
