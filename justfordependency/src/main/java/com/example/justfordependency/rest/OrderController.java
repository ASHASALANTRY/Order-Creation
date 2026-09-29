package com.example.justfordependency.rest;

import com.example.justfordependency.dto.CheckoutOrderRequest;
import com.example.justfordependency.service.CheckoutService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;

@RestController
public class OrderController {
    private final CheckoutService checkoutService;
    public OrderController( CheckoutService checkoutService) {
        this.checkoutService= checkoutService;
    }


    @PostMapping(value="/orders/{orderId}/checkout")
    public String orderCheckout(@PathVariable(name="orderId")String orderId, @Valid @RequestBody CheckoutOrderRequest request) {
        return checkoutService.initialOrderFlow(orderId, request);
    }
}
