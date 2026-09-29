package com.example.kafka_consumer_demo.repository;

import com.example.kafka_consumer_demo.entity.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;
@Repository
public interface OrderRepository extends JpaRepository<Order, UUID> {
    Order getByOrderId(String orderId);
}
