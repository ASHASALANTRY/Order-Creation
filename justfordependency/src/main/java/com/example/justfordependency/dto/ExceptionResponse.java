package com.example.justfordependency.dto;

import lombok.Data;

import java.time.LocalDateTime;
@Data
public class ExceptionResponse {
    private String exceptionType;

    private LocalDateTime timeStamp = LocalDateTime.now();

    private String message;

}
