package com.example.kafka_consumer_demo.repository;

import com.example.kafka_consumer_demo.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;
@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {
    boolean existsById(UUID eventId);
}
