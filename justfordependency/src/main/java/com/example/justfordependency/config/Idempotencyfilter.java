package com.example.justfordependency.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Component
public class Idempotencyfilter extends OncePerRequestFilter {
    private static final String IDEMPOTENCY_HEADER="Idempotency-Key";
    private final RedisTemplate<String,String> redisTemplate;

    public Idempotencyfilter(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException {
        //Target state change method only
        if("GET".equalsIgnoreCase(request.getMethod())){
            try {
            String indempotencyKey=request.getHeader(IDEMPOTENCY_HEADER);
            if(Objects.isNull(indempotencyKey) || indempotencyKey.isBlank()) {
                sendErrorResponse(response,  HttpStatus.BAD_REQUEST,"Missing required Idempotency-key header ");
                return;
            }
            String redisKey=indempotencyKey;

            Boolean isFirstRequest=redisTemplate.opsForValue().setIfAbsent(redisKey,"PROCESSING", 1,TimeUnit.DAYS);
           if(Boolean.FALSE.equals(isFirstRequest)) {
               String status = redisTemplate.opsForValue().get(redisKey);
               if (status.equals("PROCESSING")) {
                   sendErrorResponse1(response,  HttpStatus.CONFLICT,"Your request is already registered and is currently being processed");
               } else {
                   // 1. Set the key into the thread-local MDC context
                   response.setStatus(HttpServletResponse.SC_OK);
                   response.setHeader("X-cache-Lookup", "HIT");
                   response.getWriter().write("Your request is already "+status);//return saved previous response
               }

               return;//blocks controller from execution twice

           }
            // 4. Attach information to the request context so your controllers can read it if needed

                MDC.put("idempotencyKey", indempotencyKey);

                request.setAttribute("validatedIdempotencyKey", indempotencyKey);

                response.setHeader("X-idempotency-status", "Accepted");

                filterChain.doFilter(request, response);
            }catch (Exception e){
                logger.info("error in setting validated header: {}",e);
                filterChain.doFilter(request, response);

            }
           }



    }
    private void sendErrorResponse(HttpServletResponse response, HttpStatus status, String errorMessage) throws IOException {
        response.setStatus(status.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        // Return a clean JSON error body directly from the filter level
        String jsonError=String.format("{\"error\":\"%s\",\"status\":%d}",errorMessage,status.value());
        response.getWriter().write(jsonError);
    }
    private void sendErrorResponse1(HttpServletResponse response, HttpStatus status, String errorMessage) throws IOException {
        response.setStatus(status.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        // Return a clean JSON error body directly from the filter level
        String jsonError=String.format("{\"error\":\"%s\",\"status\":%d}",errorMessage,status.value());
        response.getWriter().write(jsonError);
    }
}
