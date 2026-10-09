package com.bancadigital.infrastructure.adapter;

import com.sun.net.httpserver.HttpServer;
import com.bancadigital.domain.exception.AccountSystemTimeoutException;
import com.bancadigital.domain.exception.AccountSystemUnavailableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AccountSystemWebClientTest {
    private HttpServer server;
    private AccountSystemWebClient client;
    private int status = 200;
    private String response;
    private final AtomicReference<String> requestUri = new AtomicReference<>();
    private final AtomicReference<String> requestBody = new AtomicReference<>();
    private final AtomicReference<String> requestMethod = new AtomicReference<>();

    @BeforeEach
    void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", exchange -> {
            requestUri.set(exchange.getRequestURI().toString());
            requestMethod.set(exchange.getRequestMethod());
            requestBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        client = new AccountSystemWebClient(WebClient.builder()
                .baseUrl("http://localhost:" + server.getAddress().getPort()).build(), 2000, 3, 2, 2);
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.stop(0);
    }

    @Test
    void validatesAccountFromHttpResponse() {
        response = "{\"valid\":true}";
        StepVerifier.create(client.validateAccount("10001")).expectNext(true).verifyComplete();
        assertEquals("/api/accounts/10001/validate", requestUri.get());
        assertEquals("GET", requestMethod.get());
    }

    @Test
    void preservesInvalidAccountResult() {
        response = "{\"valid\":false}";
        StepVerifier.create(client.validateAccount("10001")).expectNext(false).verifyComplete();
    }

    @Test
    void reportsMissingAccount() {
        status = 404;
        response = "{}";
        StepVerifier.create(client.validateAccount("10001"))
                .expectErrorMessage("Cuenta no encontrada: 10001").verify();
    }

    @Test
    void checksFundsUsingAmountQuery() {
        response = "{\"sufficient\":true}";
        StepVerifier.create(client.checkSufficientFunds("10001", new BigDecimal("100.00")))
                .expectNext(true).verifyComplete();
        assertEquals("/api/accounts/10001/funds?amount=100.00", requestUri.get());
    }

    @Test
    void preservesInsufficientFundsResult() {
        response = "{\"sufficient\":false}";
        StepVerifier.create(client.checkSufficientFunds("10001", new BigDecimal("100.00")))
                .expectNext(false).verifyComplete();
    }

    @Test
    void serializesTransferAndReadsSuccess() {
        response = "{\"success\":true}";
        StepVerifier.create(client.transferFunds("10001", "20002", new BigDecimal("100.00"), "OP001"))
                .expectNext(true).verifyComplete();
        assertEquals("POST", requestMethod.get());
        assertEquals("/api/accounts/transfer", requestUri.get());
        assertTrue(requestBody.get().contains("\"fromAccount\":\"10001\""));
        assertTrue(requestBody.get().contains("\"toAccount\":\"20002\""));
        assertTrue(requestBody.get().contains("\"amount\":100.00"));
        assertTrue(requestBody.get().contains("\"operationNumber\":\"OP001\""));
    }

    @Test
    void zeroRetriesPreservesConnectionFailureAndDoesNotRepeatRequests() {
        List<Function<AccountSystemWebClient, Mono<Boolean>>> operations = List.of(
                accounts -> accounts.validateAccount("10001"),
                accounts -> accounts.checkSufficientFunds("10001", BigDecimal.ONE),
                accounts -> accounts.transferFunds("10001", "20002", BigDecimal.ONE, "OP001"));
        for (Function<AccountSystemWebClient, Mono<Boolean>> operation : operations) {
            AtomicInteger attempts = new AtomicInteger();
            WebClientRequestException original = connectionFailure();
            AccountSystemWebClient accounts = new AccountSystemWebClient(
                    failingTransport(attempts, original), 100, 0, 0, 0);
            StepVerifier.create(operation.apply(accounts))
                    .expectErrorMatches(error -> error instanceof AccountSystemUnavailableException
                            && error.getCause() == original).verify();
            assertEquals(1, attempts.get());
        }
    }

    @Test
    void zeroRetriesPreservesTimeoutForAllAccountOperations() {
        List<Function<AccountSystemWebClient, Mono<Boolean>>> operations = List.of(
                accounts -> accounts.validateAccount("10001"),
                accounts -> accounts.checkSufficientFunds("10001", BigDecimal.ONE),
                accounts -> accounts.transferFunds("10001", "20002", BigDecimal.ONE, "OP001"));
        for (Function<AccountSystemWebClient, Mono<Boolean>> operation : operations) {
            WebClient silent = WebClient.builder().baseUrl("http://accounts.invalid")
                    .exchangeFunction(request -> Mono.never()).build();
            AccountSystemWebClient accounts = new AccountSystemWebClient(silent, 100, 0, 0, 0);
            StepVerifier.withVirtualTime(() -> operation.apply(accounts))
                    .thenAwait(Duration.ofSeconds(2))
                    .expectErrorMatches(error -> error instanceof AccountSystemTimeoutException
                            && error.getCause() instanceof TimeoutException).verify();
        }
    }

    @Test
    void exhaustedPositiveRetriesPreserveLastConnectionFailure() {
        AtomicInteger attempts = new AtomicInteger();
        WebClientRequestException original = connectionFailure();
        AccountSystemWebClient accounts = new AccountSystemWebClient(
                failingTransport(attempts, original), 100, 2, 0, 0);
        StepVerifier.withVirtualTime(() -> accounts.validateAccount("10001"))
                .thenAwait(Duration.ofSeconds(10))
                .expectErrorMatches(error -> error instanceof AccountSystemUnavailableException
                        && error.getCause() == original).verify();
        assertEquals(3, attempts.get());
    }

    private WebClient failingTransport(AtomicInteger attempts, WebClientRequestException failure) {
        return WebClient.builder().baseUrl("http://accounts.invalid")
                .exchangeFunction(request -> Mono.defer(() -> {
                    attempts.incrementAndGet();
                    return Mono.error(failure);
                })).build();
    }

    private WebClientRequestException connectionFailure() {
        return new WebClientRequestException(new IOException("Connection prematurely closed BEFORE response"),
                HttpMethod.GET, URI.create("http://accounts.invalid"), new HttpHeaders());
    }

    @Test
    void preservesFailedTransferResult() {
        response = "{\"success\":false}";
        StepVerifier.create(client.transferFunds("10001", "20002", new BigDecimal("100.00"), "OP001"))
                .expectNext(false).verifyComplete();
    }
}
