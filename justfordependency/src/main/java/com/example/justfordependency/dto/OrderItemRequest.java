package com.example.justfordependency.dto;


import lombok.Data;
import java.math.BigDecimal;

@Data
public class OrderItemRequest {
    private String sku;
    private Integer quantity;
    private BigDecimal unitPrice;
}

