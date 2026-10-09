package com.bancadigital.domain.exception;

public class IdempotencyUnavailableException extends RuntimeException {
    public IdempotencyUnavailableException(String message, Throwable cause) { super(message, cause); }
}
