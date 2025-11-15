package com.learnkafkastreams.exceptionhandler;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalErrorHandler {

    @ExceptionHandler
    public ProblemDetail handleIllegalStateException(IllegalStateException exception)
    {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, exception.getMessage());

        problemDetail.setProperty("additionalInfo", "SVP entrez un type de commande valide: general_orders ou restaurant_orders");

        return problemDetail;
    }
}
