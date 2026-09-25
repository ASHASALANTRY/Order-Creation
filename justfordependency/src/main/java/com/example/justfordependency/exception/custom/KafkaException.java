package com.example.justfordependency.exception.custom;

public class KafkaException extends RuntimeException {
    public KafkaException(String message){
        super(message);
    }

}
