package com.example.justfordependency.exception;

import com.example.justfordependency.dto.ExceptionResponse;
import com.example.justfordependency.exception.custom.KafkaException;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@ControllerAdvice
public class ExceptionHandlerControllerAdvice {

    private static final Logger log =
            LoggerFactory.getLogger(ExceptionHandlerControllerAdvice.class);


    // -----------------------------------------
    // 1. Request validation errors
    // -----------------------------------------

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ExceptionResponse> handleValidationException(
            MethodArgumentNotValidException e) {

        String message = e.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(error ->
                        error.getField()
                                + ": "
                                + error.getDefaultMessage())
                .distinct()
                .collect(Collectors.joining(", "));

        log.warn("Request validation failed: {}", message);

        ExceptionResponse response = prepareResponse(message);

        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(response);
    }


    // -----------------------------------------
    // 2. Kafka errors
    // -----------------------------------------

    @ExceptionHandler(KafkaException.class)
    public ResponseEntity<ExceptionResponse> handleKafkaException(
            HttpServletRequest request,
            KafkaException e) {

        logExceptionDetails(
                e,
                "Kafka operation failed"
        );

        ExceptionResponse response =
                prepareResponse(e.getMessage());

        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(response);
    }


    // -----------------------------------------
    // 3. Generic/unexpected errors
    // -----------------------------------------

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ExceptionResponse> handleGenericException(
            HttpServletRequest request,
            Exception e) {

        logExceptionDetails(
                e,
                "Unexpected error while processing request"
        );

        ExceptionResponse response =
                prepareResponse(
                        "An unexpected error occurred"
                );

        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(response);
    }


    // -----------------------------------------
    // Build standard error response
    // -----------------------------------------

    private ExceptionResponse prepareResponse(String message) {

        ExceptionResponse error =
                new ExceptionResponse();

        error.setExceptionType(message);
        error.setMessage(message);

        return error;
    }


    // -----------------------------------------
    // Logging
    // -----------------------------------------

    private void logExceptionDetails(
            Exception ex,
            String customMessage) {

        StackTraceElement[] stackTrace =
                ex.getStackTrace();

        StackTraceElement rootCause =
                stackTrace.length > 0
                        ? stackTrace[0]
                        : null;

        String errorMessage = ex.getMessage();

        String innerException =
                ex.getCause() != null
                        ? ex.getCause().toString()
                        : "Not_Available";

        Timestamp errorTime =
                new Timestamp(System.currentTimeMillis());

        log.error(
                "Error_Time: {}, Custom_Message: {}, Error_Message: {}, Inner_Exception: {}",
                errorTime,
                customMessage,
                errorMessage,
                innerException
        );

        if (rootCause != null) {

            log.error(
                    "Class: {}, File: {}, Method: {}, Line: {}",
                    rootCause.getClassName(),
                    rootCause.getFileName(),
                    rootCause.getMethodName(),
                    rootCause.getLineNumber()
            );
        }
    }
}