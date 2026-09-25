package com.example.justfordependency.service;

import com.enterprise.inventory.avro.InventoryUpdateEvent;
import com.example.justfordependency.config.OrderEventPublisher;
import com.example.justfordependency.dto.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class OrderService {

    private final OrderEventPublisher orderEventPublisher;
    private final KafkaTemplate<String, InventoryUpdateEvent> kafkaTemplate;

    public OrderService(OrderEventPublisher orderEventPublisher, KafkaTemplate<String, InventoryUpdateEvent> kafkaTemplate) {
        this.orderEventPublisher = orderEventPublisher;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void processOrderCreation(String orderId, double price, String sku){
        // Executing standard internal business logic here

        Order event=new Order(orderId,"CREATED",price);
        // Hand-off directly to publisher wrapper cleanly
        orderEventPublisher.publish(event, true);

        InventoryUpdateEvent inventoryUpdateEvent=InventoryUpdateEvent.newBuilder()
                .setSku(sku).setQuantityChanged(-1).build();
        kafkaTemplate.send("inventory-updates",inventoryUpdateEvent.getSku(),inventoryUpdateEvent);

    }
}
