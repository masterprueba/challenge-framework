package com.bancadigital.infrastructure.exception;

import com.bancadigital.domain.exception.IdempotencyConflictException;
import com.bancadigital.domain.exception.AccountNotFoundException;
import com.bancadigital.domain.exception.InsufficientFundsException;
import com.bancadigital.domain.exception.AccountSystemTimeoutException;
import com.bancadigital.domain.exception.TransactionProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.support.WebExchangeBindException;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.stream.Collectors;

@ControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(IdempotencyConflictException.class)
    public Mono<ResponseEntity<ErrorResponse>> handleIdempotencyConflict(IdempotencyConflictException ex) {
        log.warn("Conflicto de idempotencia detectado: {}", ex.getMessage());
        return response(HttpStatus.CONFLICT, "CONFLICT", ex.getMessage());
    }

    @ExceptionHandler(AccountNotFoundException.class)
    public Mono<ResponseEntity<ErrorResponse>> handleAccountNotFound(AccountNotFoundException ex) {
        log.warn("Cuenta no encontrada: {}", ex.getMessage());
        return response(HttpStatus.NOT_FOUND, "ACCOUNT_NOT_FOUND", ex.getMessage());
    }

    @ExceptionHandler(InsufficientFundsException.class)
    public Mono<ResponseEntity<ErrorResponse>> handleInsufficientFunds(InsufficientFundsException ex) {
        log.warn("Fondos insuficientes: {}", ex.getMessage());
        return response(HttpStatus.UNPROCESSABLE_ENTITY, "INSUFFICIENT_FUNDS", ex.getMessage());
    }

    @ExceptionHandler(AccountSystemTimeoutException.class)
    public Mono<ResponseEntity<ErrorResponse>> handleAccountSystemTimeout(AccountSystemTimeoutException ex) {
        log.error("Timeout del sistema de cuentas: {}", ex.getMessage());
        return response(HttpStatus.GATEWAY_TIMEOUT, "ACCOUNT_SYSTEM_TIMEOUT", ex.getMessage());
    }

    @ExceptionHandler(TransactionProcessingException.class)
    public Mono<ResponseEntity<ErrorResponse>> handleTransactionProcessing(TransactionProcessingException ex) {
        log.error("Error procesando transacción: {}", ex.getMessage());
        return response(HttpStatus.INTERNAL_SERVER_ERROR, "TRANSACTION_PROCESSING_ERROR", ex.getMessage());
    }

    @ExceptionHandler(WebExchangeBindException.class)
    public Mono<ResponseEntity<ErrorResponse>> handleValidationErrors(WebExchangeBindException ex) {
        String errors = ex.getBindingResult().getFieldErrors().stream()
            .map(error -> error.getField() + ": " + error.getDefaultMessage())
            .collect(Collectors.joining(", "));
        log.warn("Errores de validación: {}", errors);
        return response(HttpStatus.BAD_REQUEST, "VALIDATION_ERROR", errors);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public Mono<ResponseEntity<ErrorResponse>> handleIllegalArgument(IllegalArgumentException ex) {
        log.warn("Argumento ilegal: {}", ex.getMessage());
        return response(HttpStatus.BAD_REQUEST, "BAD_REQUEST", ex.getMessage());
    }

    @ExceptionHandler(Exception.class)
    public Mono<ResponseEntity<ErrorResponse>> handleGenericException(Exception ex) {
        log.error("Error inesperado: ", ex);
        return response(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_SERVER_ERROR", "Ha ocurrido un error inesperado. Por favor, contacte al administrador.");
    }

    private Mono<ResponseEntity<ErrorResponse>> response(HttpStatus status, String code, String message) {
        return Mono.just(ResponseEntity.status(status)
                .body(new ErrorResponse(status.value(), code, message, LocalDateTime.now())));
    }

    public record ErrorResponse(
        int status,
        String code,
        String message,
        LocalDateTime timestamp
    ) {}
}
