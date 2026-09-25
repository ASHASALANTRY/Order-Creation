package com.example.justfordependency.rest;

import com.example.justfordependency.service.CheckoutService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderController {
    private final CheckoutService checkoutService;
    public OrderController( CheckoutService checkoutService) {

        this.checkoutService= checkoutService;
    }


    @GetMapping(value="/order-checkout/{orderId}")
    public String orderCheckout(@PathVariable(name="orderId")String orderId){
        return checkoutService.initialOrderFlow(orderId);
    }
}
