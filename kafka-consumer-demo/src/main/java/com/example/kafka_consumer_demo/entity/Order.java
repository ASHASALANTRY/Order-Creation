package com.example.kafka_consumer_demo.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "orders")
@Getter
@Setter
public class Order {

    @Id
    @Column(name = "id", length = 64)
    private UUID id; // Matches the orderId generated upstream

    @Column(name = "customer_id", length = 64)
    private UUID customerId;

    @Column(name = "status", length = 32)
    private String status; // e.g., PENDING, PAID, CANCELLED

    @Column(name = "total_amount", precision = 10)
    private Double totalAmount;



    @Embedded // Flattens shipping columns into the orders table directly
    private ShippingAddress shippingAddress;

    @Column(name = "created_at", updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at")
    private Instant updatedAt;

    // Relationship to line items
    @OneToMany(
            mappedBy = "order",
            cascade = CascadeType.ALL,
            orphanRemoval = true,
            fetch = FetchType.LAZY
    )
    private List<OrderItem> items = new ArrayList<>();

    // Helper method to keep bidirectional relationship in sync
    public void addItem(OrderItem item) {
        items.add(item);
        item.setOrder(this);
    }

    @PrePersist
    protected void onCreate() {
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = Instant.now();
    }
}
