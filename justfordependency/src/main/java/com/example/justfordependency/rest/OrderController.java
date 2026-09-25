package com.example.justfordependency.rest;

import com.example.justfordependency.service.CheckoutService;
import com.example.justfordependency.service.OrderService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderController {
    private final OrderService orderService;
    private final CheckoutService checkoutService;
    public OrderController(OrderService orderService, CheckoutService checkoutService) {

        this.orderService = orderService;
        this.checkoutService= checkoutService;
    }

    @GetMapping(value = "/order-create/{id}/{sku}")
    public void orderCreate(@PathVariable(name ="id") String id,@PathVariable(name ="sku") String sku){
        orderService.processOrderCreation(id,11.23d,sku);
    }
    @GetMapping(value="/order-checkout/{orderId}")
    public String orderCheckout(@PathVariable(name="orderId")String orderId){
        return checkoutService.initialOrderFlow(orderId);
    }
}
