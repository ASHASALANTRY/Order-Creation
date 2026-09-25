package com.example.kafka_consumer_demo.dto;

public record Order(String orderId, String status, double amount) {
}
