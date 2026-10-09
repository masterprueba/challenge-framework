package com.bancadigital.infrastructure.adapter;

import com.bancadigital.domain.exception.AccountSystemTimeoutException;
import com.bancadigital.domain.exception.AccountSystemUnavailableException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AccountSystemWebClientTest {
    @Test
    void transferFailuresRemainTypedAndAreNeverRetried() {
        AtomicInteger timeoutAttempts = new AtomicInteger();
        WebClient silent = WebClient.builder().baseUrl("http://accounts.invalid")
                .exchangeFunction(request -> Mono.defer(() -> {
                    timeoutAttempts.incrementAndGet();
                    return Mono.never();
                })).build();
        AccountSystemWebClient timeoutClient = new AccountSystemWebClient(silent, 100, 0, 0);
        StepVerifier.withVirtualTime(() -> timeoutClient.transferFunds("10001", "20002", BigDecimal.ONE, "OP001"))
                .thenAwait(Duration.ofSeconds(2))
                .expectErrorMatches(error -> error instanceof AccountSystemTimeoutException
                        && error.getCause() instanceof TimeoutException).verify();
        assertEquals(1, timeoutAttempts.get());

        AtomicInteger connectionAttempts = new AtomicInteger();
        WebClientRequestException original = new WebClientRequestException(new IOException("Connection closed"),
                HttpMethod.POST, URI.create("http://accounts.invalid"), new HttpHeaders());
        WebClient failing = WebClient.builder().baseUrl("http://accounts.invalid")
                .exchangeFunction(request -> Mono.defer(() -> {
                    connectionAttempts.incrementAndGet();
                    return Mono.error(original);
                })).build();
        AccountSystemWebClient connectionClient = new AccountSystemWebClient(failing, 100, 0, 0);
        StepVerifier.create(connectionClient.transferFunds("10001", "20002", BigDecimal.ONE, "OP001"))
                .expectErrorMatches(error -> error instanceof AccountSystemUnavailableException
                        && error.getCause() == original).verify();
        assertEquals(1, connectionAttempts.get());
    }
}
