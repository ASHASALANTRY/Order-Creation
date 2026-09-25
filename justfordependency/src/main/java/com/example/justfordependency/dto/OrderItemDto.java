package com.example.justfordependency.dto;


import lombok.Data;
import java.math.BigDecimal;

@Data
public class OrderItemDto {
    private Long id;
    private String productId;
    private Integer quantity;
    private Double price;
}

