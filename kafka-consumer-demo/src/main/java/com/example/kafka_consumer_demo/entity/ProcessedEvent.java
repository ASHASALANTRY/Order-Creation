package com.example.kafka_consumer_demo.entity;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "processed_events")
@Getter
@Setter
public class ProcessedEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id")
    private UUID id;

    @Column(name = "event_id", length = 256, nullable = false)
    private String eventId; // Maps to the Kafka Header Idempotency-Key

    @Column(name = "event_type", length = 100, nullable = false)
    private String eventType; // e.g., ORDER_CHECKOUT_SUBMITTED

    @Column(name = "status", length = 32, nullable = false)
    private String status; // e.g., SUCCESS, INVALID_PAYLOAD, FAILED_VALIDATION

    @Column(name = "processed_at", nullable = false, updatable = false)
    private Instant processedAt = Instant.now();
}