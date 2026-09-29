package com.example.justfordependency.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import lombok.Data;

import java.util.List;
@Data
public class CheckoutOrderRequest {
    @NotBlank(message = "Customer ID is required")
    private String customerId;
    @NotEmpty(message = "Order must contain at least one item")
    @Valid
    private List<OrderItemRequest> items;
    private ShippingAddressRequest shippingAddress;
}
