package com.example.kafka_consumer_demo.entity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
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
    @Column(name = "event_id", length = 64, nullable = false)
    private UUID eventId;

    @Column(name="idempotency_key", nullable=false)
    private String idempotencyKey;

    @Column(name = "processed_at", nullable = false, updatable = false)
    private Instant processedAt = Instant.now();
}