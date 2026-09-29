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
import java.util.Objects;
import java.util.concurrent.TimeUnit;
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

        if(processedEventRepository.existsByEventId(idempotencyKey)){
//            throw new IllegalArgumentException();
            updateRedis(idempotencyKey);
            log.info("this order is already processed.Successfully updated status to redis");
            return;
        }
        Order order=new Order();

        Order orderAlreadyExist=orderRepository.getByOrderId(event.getOrderId());
        if(Objects.nonNull(orderAlreadyExist))
            order.setId(orderAlreadyExist.getId());
        order.setOrderId(event.getOrderId());

        ArrayList<OrderItem> orderItems=new ArrayList<>();
        event.getItems().forEach(avroItem->{
            OrderItem item = new OrderItem();
            item.setSku(avroItem.getSku());
            item.setQuantity(avroItem.getQuantity());
            item.setUnitPrice(avroItem.getPrice()); // Ensure matching types (Double/BigDecimal)
            order.addItem(item);
        });
        ShippingAddress shippingAddress=new ShippingAddress();
        ArrayList<ShippingAddress> shipping=new ArrayList<>();

        if (event.getShippingDetails() != null) {
            var avroShipping = event.getShippingDetails();
            shippingAddress.setStreet(avroShipping.getStreet());
            shippingAddress.setCity(avroShipping.getCity());
            shippingAddress.setCountry(avroShipping.getCountry());
            shippingAddress.setPostalCode(avroShipping.getPostalCode());
        }
            order.setShippingAddress(shippingAddress);
        order.setCustomerId(event.getCustomerId());
        order.setStatus("PENDING");
        ProcessedEvent processedEvent=new ProcessedEvent();
        processedEvent.setEventId(event.getEventId());
        processedEvent.setEventType(event.getEventType());
        processedEvent.setStatus("PROCESSED");

        processedEventRepository.save(processedEvent);
        orderRepository.save(order);

        log.info("Successfully processed order business logic for ID: {}",event.getOrderId());
        // Example: If database throws an error here, the ErrorHandler configuration
        // will safely catch it, retry 3 times, and forward to the DLQ if it keeps failing.
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit(){
                updateRedis(idempotencyKey);
                log.info("Successfully updated status to redis");
            }
        });
    }

    void updateRedis(String idempotencyKey){
            redisTemplate.opsForValue().set(idempotencyKey, "PROCESSED", 1, TimeUnit.DAYS);

    }
}
