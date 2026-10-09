package com.bancadigital.application.usecase;

import com.bancadigital.domain.exception.AccountSystemTimeoutException;
import com.bancadigital.domain.exception.AccountSystemUnavailableException;
import com.bancadigital.domain.exception.IdempotencyConflictException;
import com.bancadigital.domain.exception.IdempotencyReplayException;
import com.bancadigital.domain.exception.IdempotencyUnavailableException;
import com.bancadigital.domain.exception.TransactionTimeoutException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.web.server.ResponseStatusException;

/** The same public error is returned on the first request and on a cached replay. */
public record TransactionFailure(int status, String message) {
    public static TransactionFailure from(Throwable error) {
        if (error instanceof IdempotencyReplayException replay)
            return new TransactionFailure(replay.getStatus(), replay.getMessage());
        if (error instanceof AccountSystemTimeoutException || error instanceof TransactionTimeoutException)
            return new TransactionFailure(504, error.getMessage());
        if (error instanceof AccountSystemUnavailableException || error instanceof IdempotencyUnavailableException)
            return new TransactionFailure(503, error.getMessage());
        if (error instanceof CallNotPermittedException)
            return new TransactionFailure(503, "Sistema de cuentas temporalmente no disponible");
        if (error instanceof DataAccessResourceFailureException)
            return new TransactionFailure(503, "Servicio temporalmente saturado o base de datos no disponible");
        if (error instanceof IdempotencyConflictException)
            return new TransactionFailure(409, error.getMessage());
        if (error instanceof IllegalArgumentException)
            return new TransactionFailure(422, error.getMessage());
        if (error instanceof ResponseStatusException response)
            return new TransactionFailure(response.getStatusCode().value(), response.getReason());
        return new TransactionFailure(500, "Error interno del servidor");
    }
}
