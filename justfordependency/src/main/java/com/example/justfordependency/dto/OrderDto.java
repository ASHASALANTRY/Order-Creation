package com.example.justfordependency.dto;

import lombok.Data;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
@Data
public class OrderDto {
    private String id;
    private String customerId;
    private String status;
    private BigDecimal totalAmount;
    private ShippingAddressDto shippingAddress;
    private Instant createdAt;
    private Instant updatedAt;
    private List<OrderItemDto> items;
}