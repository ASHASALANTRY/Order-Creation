package com.example.justfordependency.dto;

import lombok.Data;

import java.time.Instant;
import java.util.List;
@Data
public class OrderCheckoutSubmittedEvent {
    private String eventId;
    private String eventType;
    private Instant timestamp;
    private String orderId;
    private String customerId;
    private List<OrderItemDto> items;
    private ShippingAddressDto shippingDetails;
}
