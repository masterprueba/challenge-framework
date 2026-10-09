package com.bancadigital.infrastructure.rest;

import com.bancadigital.application.usecase.ProcessTransactionUseCase;
import com.bancadigital.domain.port.TransactionRepository;
import com.bancadigital.domain.exception.AccountSystemTimeoutException;
import com.bancadigital.domain.exception.AccountSystemUnavailableException;
import com.bancadigital.domain.exception.TransactionTimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TransactionControllerTest {
    @Mock private ProcessTransactionUseCase useCase;
    @Mock private TransactionRepository repository;
    private WebTestClient http;

    @BeforeEach
    void setUp() {
        http = WebTestClient.bindToController(new TransactionController(useCase, repository)).build();
    }

    @Test
    void accountTimeoutReturns504() {
        assertUseCaseFailure(new AccountSystemTimeoutException("Tiempo de espera agotado",
                new TimeoutException()), 504);
    }

    @Test
    void accountConnectionFailureReturns503() {
        assertUseCaseFailure(new AccountSystemUnavailableException("Conexión cerrada",
                new IOException()), 503);
    }

    @Test
    void transactionTimeoutReturns504() {
        assertUseCaseFailure(new TransactionTimeoutException("Tiempo máximo de transacción agotado",
                new TimeoutException()), 504);
    }

    private void assertUseCaseFailure(RuntimeException failure, int expectedStatus) {
        when(repository.findByIdempotencyKey(any())).thenReturn(Mono.empty());
        when(useCase.execute(any(), any(), any(), any(), any())).thenReturn(Mono.error(failure));
        http.post().uri("/api/v1/transactions").bodyValue(Map.of(
                "operationNumber", "TIMEOUT-OP", "channel", "WEB",
                "amount", 100, "accountFrom", "10001", "accountTo", "20002")).exchange()
                .expectStatus().isEqualTo(expectedStatus).expectBody()
                .jsonPath("$.status").isEqualTo("ERROR")
                .jsonPath("$.message").isEqualTo(failure.getMessage());
    }

    @Test
    void databaseConnectionFailureReturns503WithExplanation() {
        when(repository.findByIdempotencyKey(any())).thenReturn(Mono.error(
                new DataAccessResourceFailureException("Connection acquisition timed out after 500ms")));
        http.post().uri("/api/v1/transactions").bodyValue(Map.of(
                "operationNumber", "DB-UNAVAILABLE", "channel", "WEB",
                "amount", 100, "accountFrom", "10001", "accountTo", "20002")).exchange()
                .expectStatus().isEqualTo(503).expectBody()
                .jsonPath("$.status").isEqualTo("ERROR")
                .jsonPath("$.message").isEqualTo("Servicio temporalmente saturado o base de datos no disponible");
        verifyNoInteractions(useCase);
    }
}
