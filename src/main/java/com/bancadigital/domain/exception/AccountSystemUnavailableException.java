package com.bancadigital.domain.exception;

public class AccountSystemUnavailableException extends RuntimeException {
    public AccountSystemUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
