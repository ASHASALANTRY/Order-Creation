package com.example.justfordependency.dto;

import lombok.Data;

import java.util.List;
@Data
public class CheckoutOrderRequest {

    private String customerId;
    private List<OrderItemRequest> items;
    private ShippingAddressRequest shippingAddress;
}
