package com.bancadigital.domain.exception;

public class AccountSystemTimeoutException extends RuntimeException {
    public AccountSystemTimeoutException(String message) {
        super(message);
    }

    public AccountSystemTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
