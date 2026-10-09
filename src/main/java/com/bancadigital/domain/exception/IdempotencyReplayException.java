package com.bancadigital.domain.exception;

public class IdempotencyReplayException extends RuntimeException {
    private final int status;
    public IdempotencyReplayException(int status, String message) {
        super(message);
        this.status = status;
    }
    public int getStatus() { return status; }
}
