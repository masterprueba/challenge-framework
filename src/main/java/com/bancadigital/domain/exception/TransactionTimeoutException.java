package com.bancadigital.domain.exception;

public class TransactionTimeoutException extends RuntimeException {
    public TransactionTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
