package com.bancadigital.infrastructure.rest;

import com.bancadigital.Application;
import com.bancadigital.application.usecase.ProcessTransactionUseCase;
import com.bancadigital.domain.model.Transaction;
import com.bancadigital.domain.model.Transaction.TransactionStatus;
import com.bancadigital.domain.port.TransactionRepository;
import com.sun.net.httpserver.HttpServer;
import io.r2dbc.spi.ConnectionFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.reactive.context.ReactiveWebServerApplicationContext;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.web.reactive.server.WebTestClient;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.dao.DataIntegrityViolationException;
import reactor.test.StepVerifier;
import reactor.netty.resources.ConnectionProvider;
import io.r2dbc.pool.ConnectionPool;

import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Opt-in PostgreSQL regression tests. Each run creates and removes its own schema.
 * Example: TRANSACTION_TEST_DATABASE_URL=r2dbc:postgresql://postgres:postgres@localhost:5432/bancadb
 */
@EnabledIfEnvironmentVariable(named = "TRANSACTION_TEST_DATABASE_URL", matches = ".+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TransactionFlowIntegrationTest {
    private final String schema = "transaction_regression_" + UUID.randomUUID().toString().replace("-", "");
    private final AtomicInteger transfers = new AtomicInteger();
    private DatabaseClient admin;
    private DatabaseClient database;
    private ReactiveWebServerApplicationContext context;
    private HttpServer accounts;
    private WebTestClient http;
    private TransactionRepository repository;

    @BeforeAll
    void setUp() throws Exception {
        String url = System.getenv("TRANSACTION_TEST_DATABASE_URL");
        admin = DatabaseClient.create(ConnectionFactories.get(url));
        admin.sql("CREATE SCHEMA " + schema).fetch().rowsUpdated().block(Duration.ofSeconds(10));
        String testUrl = url + (url.contains("?") ? "&" : "?") + "schema=" + schema;
        database = DatabaseClient.create(ConnectionFactories.get(testUrl));
        database.sql("""
                CREATE TABLE transactions (
                    transaction_id VARCHAR(36) PRIMARY KEY,
                    operation_number TEXT NOT NULL CHECK (operation_number NOT LIKE 'DBFAIL-%'),
                    channel TEXT NOT NULL,
                    amount NUMERIC NOT NULL, status TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,
                    account_from TEXT NOT NULL, account_to TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE
                )
                """).fetch().rowsUpdated().block(Duration.ofSeconds(10));

        accounts = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        accounts.createContext("/api/accounts/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            exchange.getRequestBody().readAllBytes();
            String response;
            if (path.endsWith("/validate")) {
                response = "{\"valid\":true}";
            } else if (path.endsWith("/funds")) {
                response = "{\"sufficient\":true}";
            } else if (path.endsWith("/transfer")) {
                transfers.incrementAndGet();
                response = "{\"success\":true}";
            } else {
                throw new IllegalStateException("Unexpected account path: " + path);
            }
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        accounts.start();
        context = (ReactiveWebServerApplicationContext) new SpringApplicationBuilder(Application.class)
                .run("--server.port=0",
                        "--spring.profiles.active=load",
                        "--management.server.port=0",
                        "--spring.r2dbc.url=" + testUrl,
                        "--account-system.base-url=http://localhost:" + accounts.getAddress().getPort(),
                        "--logging.level.root=WARN",
                        "--logging.level.com.bancadigital=WARN",
                        "--logging.level.org.springframework.r2dbc=WARN",
                        "--logging.level.io.r2dbc.postgresql=WARN",
                        "--logging.level.reactor.netty=WARN");
        repository = context.getBean(TransactionRepository.class);
        http = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + context.getWebServer().getPort())
                .responseTimeout(Duration.ofSeconds(10)).build();
    }

    @AfterAll
    void tearDown() {
        if (context != null) context.close();
        if (accounts != null) accounts.stop(0);
        if (admin != null) {
            admin.sql("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
                    .fetch().rowsUpdated().block(Duration.ofSeconds(10));
        }
    }

    @Test
    void savesPendingThenUpdatesSameRowToCompleted() {
        Transaction pending = pending("PERSIST-" + UUID.randomUUID());
        repository.save(pending).block(Duration.ofSeconds(10));
        Transaction completed = pending.complete();
        repository.save(completed).block(Duration.ofSeconds(10));

        Transaction found = repository.findById(pending.getTransactionId()).block(Duration.ofSeconds(10));
        assertNotNull(found);
        assertEquals(TransactionStatus.COMPLETED, found.getStatus());
        assertEquals(pending.getTransactionId(), found.getTransactionId());
        assertTrue(Math.abs(Duration.between(pending.getCreatedAt(), found.getCreatedAt()).toNanos()) < 1000);
        Long count = database.sql("SELECT COUNT(*) FROM transactions WHERE transaction_id = :id")
                .bind("id", pending.getTransactionId().toString())
                .map((row, metadata) -> row.get(0, Long.class)).one().block(Duration.ofSeconds(10));
        assertEquals(1L, count);
    }

    @Test
    void unknownIdDoesNotReturnAnotherTransaction() {
        Transaction existing = pending("LOOKUP-" + UUID.randomUUID());
        repository.save(existing).block(Duration.ofSeconds(10));
        assertNull(repository.findById(UUID.randomUUID()).block(Duration.ofSeconds(10)));
    }

    @Test
    void postReturns201CompletedAndRepeatReturnsSameTransactionWithoutTransfer() {
        String operation = "HTTP-" + UUID.randomUUID();
        Map<String, Object> body = Map.of("operationNumber", operation, "channel", "WEB",
                "amount", 100, "accountFrom", "10001", "accountTo", "20002");
        int transfersBefore = transfers.get();
        Map<?, ?> created = http.post().uri("/api/v1/transactions").bodyValue(body).exchange()
                .expectStatus().isCreated()
                .expectBody(Map.class).returnResult().getResponseBody();
        assertNotNull(created);
        assertEquals("COMPLETED", created.get("status"));
        assertEquals(transfersBefore + 1, transfers.get());
        String id = created.get("transactionId").toString();

        http.post().uri("/api/v1/transactions").bodyValue(body).exchange()
                .expectStatus().isOk().expectBody()
                .jsonPath("$.status").isEqualTo("COMPLETED")
                .jsonPath("$.transactionId").isEqualTo(id);
        assertEquals(transfersBefore + 1, transfers.get());
        http.get().uri("/api/v1/transactions/" + id).exchange()
                .expectStatus().isOk().expectBody()
                .jsonPath("$.transactionId").isEqualTo(id)
                .jsonPath("$.status").isEqualTo("COMPLETED");
        http.get().uri("/api/v1/transactions/idempotency/WEB_" + operation).exchange()
                .expectStatus().isOk().expectBody()
                .jsonPath("$.exists").isEqualTo(true)
                .jsonPath("$.transactionId").isEqualTo(id);
    }

    @Test
    void databaseErrorsDoNotOpenAccountCircuit() {
        CircuitBreaker breaker = context.getBean(CircuitBreakerRegistry.class).circuitBreaker("accountSystem");
        breaker.reset();
        int transfersBefore = transfers.get();
        ProcessTransactionUseCase useCase = context.getBean(ProcessTransactionUseCase.class);
        try {
            for (int attempt = 0; attempt < 5; attempt++) {
                StepVerifier.create(useCase.execute("DBFAIL-" + UUID.randomUUID(), "WEB",
                        "10001", "20002", new BigDecimal("100.00")))
                        .expectError(DataIntegrityViolationException.class).verify(Duration.ofSeconds(5));
            }
            assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
            assertEquals(0, breaker.getMetrics().getNumberOfFailedCalls());
            assertEquals(transfersBefore, transfers.get());
        } finally {
            breaker.reset();
        }
    }

    @Test
    void openAccountCircuitReturns503WithoutTransfer() {
        CircuitBreaker breaker = context.getBean(CircuitBreakerRegistry.class).circuitBreaker("accountSystem");
        breaker.transitionToOpenState();
        int transfersBefore = transfers.get();
        try {
            http.post().uri("/api/v1/transactions").bodyValue(Map.of(
                    "operationNumber", "OPEN-" + UUID.randomUUID(), "channel", "WEB",
                    "amount", 100, "accountFrom", "10001", "accountTo", "20002")).exchange()
                    .expectStatus().isEqualTo(503).expectBody()
                    .jsonPath("$.status").isEqualTo("ERROR")
                    .jsonPath("$.message").isEqualTo("Sistema de cuentas temporalmente no disponible");
            assertEquals(transfersBefore, transfers.get());
        } finally {
            breaker.reset();
        }
    }

    @Test
    void loadProfileBindsPoolLimitsAndExposesMetrics() {
        assertEquals(1, context.getBean(RetryRegistry.class).retry("accountSystem")
                .getRetryConfig().getMaxAttempts());
        assertEquals(256, context.getBean("accountSystemConnectionProvider", ConnectionProvider.class)
                .maxConnections());
        assertEquals(40, context.getBean(ConnectionPool.class).getMetrics().orElseThrow()
                .getMaxAllocatedSize());
        Integer managementPort = context.getEnvironment().getProperty("local.management.port", Integer.class);
        assertNotNull(managementPort);
        WebTestClient.bindToServer().baseUrl("http://localhost:" + managementPort).build()
                .get().uri("/actuator/prometheus").exchange().expectStatus().isOk();
    }

    private Transaction pending(String operation) {
        return Transaction.createPendingTransaction(operation, "WEB", new BigDecimal("100.00"),
                "10001", "20002", "WEB_" + operation);
    }
}
