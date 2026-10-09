package com.bancadigital.infrastructure.adapter;

import com.bancadigital.domain.port.AccountSystemClient;
import com.bancadigital.domain.exception.AccountSystemTimeoutException;
import com.bancadigital.domain.exception.AccountSystemUnavailableException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

@Component
public class AccountSystemWebClient implements AccountSystemClient {

    private static final Logger log = LoggerFactory.getLogger(AccountSystemWebClient.class);

    private final WebClient webClient;
    private final Duration responseTimeout;
    private final int validationRetries;
    private final int fundsRetries;
    private final int transferRetries;

    public AccountSystemWebClient(
            @Qualifier("accountSystemHttpClient") WebClient webClient,
            @Value("${account-system.response-timeout-ms:2000}") int responseTimeoutMs,
            @Value("${account-system.retry.validation:3}") int validationRetries,
            @Value("${account-system.retry.funds:2}") int fundsRetries,
            @Value("${account-system.retry.transfer:2}") int transferRetries) {
        this.webClient = webClient;
        this.responseTimeout = Duration.ofMillis(responseTimeoutMs);
        this.validationRetries = validationRetries;
        this.fundsRetries = fundsRetries;
        this.transferRetries = transferRetries;
    }

    @Override
    @CircuitBreaker(name = "accountSystem")
    public Mono<Boolean> validateAccount(String accountNumber) {
        log.debug("Validando cuenta: {}", accountNumber);
        return webClient.get()
                .uri("/api/accounts/{accountNumber}/validate", accountNumber)
                .retrieve()
                .onStatus(HttpStatusCode::is4xxClientError, response -> {
                    log.warn("Cuenta no encontrada: {}", accountNumber);
                    return Mono.just(new AccountNotFoundException("Cuenta no encontrada: " + accountNumber));
                })
                .onStatus(HttpStatusCode::is5xxServerError, response -> {
                    log.error("Error del servidor al validar cuenta: {}", accountNumber);
                    return Mono.just(new AccountSystemException("Error interno del sistema de cuentas"));
                })
                .bodyToMono(AccountValidationResponse.class)
                .timeout(responseTimeout)
                .transform(response -> applyRetryPolicy(response, validationRetries, Duration.ofMillis(500),
                        error -> !(error instanceof AccountNotFoundException)))
                .map(AccountValidationResponse::isValid)
                .doOnSuccess(result -> log.debug("Validación de cuenta {} completada: {}", accountNumber, result))
                .doOnError(error -> log.debug("Error validando cuenta {}: {}", accountNumber, error.getMessage()));
    }

    @Override
    @CircuitBreaker(name = "accountSystem")
    public Mono<Boolean> checkSufficientFunds(String accountNumber, BigDecimal amount) {
        log.debug("Verificando fondos para cuenta: {}, monto: {}", accountNumber, amount);
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/api/accounts/{accountNumber}/funds")
                        .queryParam("amount", amount.toString())
                        .build(accountNumber))
                .retrieve()
                .onStatus(HttpStatusCode::is4xxClientError, response -> {
                    log.warn("Fondos insuficientes para cuenta: {}", accountNumber);
                    return Mono.just(new InsufficientFundsException("Fondos insuficientes en cuenta: " + accountNumber));
                })
                .bodyToMono(FundsCheckResponse.class)
                .timeout(responseTimeout)
                .transform(response -> applyRetryPolicy(response, fundsRetries, Duration.ofMillis(300),
                        error -> !(error instanceof InsufficientFundsException)))
                .map(FundsCheckResponse::isSufficient)
                .doOnSuccess(result -> log.debug("Verificación de fondos para {} completada: {}", accountNumber, result))
                .doOnError(error -> log.debug("Error verificando fondos para {}: {}", accountNumber, error.getMessage()));
    }

    @Override
    @CircuitBreaker(name = "accountSystem")
    public Mono<Boolean> transferFunds(String fromAccount, String toAccount, BigDecimal amount,
                                        String operationNumber) {
        log.info("Iniciando transferencia: {} -> {}, monto: {}, operación: {}",
                fromAccount, toAccount, amount, operationNumber);

        TransferRequest request = new TransferRequest(fromAccount, toAccount, amount, operationNumber);

        return webClient.post()
                .uri("/api/accounts/transfer")
                .bodyValue(request)
                .retrieve()
                .onStatus(HttpStatusCode::is4xxClientError, response -> {
                    log.error("Error de cliente en transferencia: {}", response.statusCode());
                    return Mono.just(new TransferFailedException("Error en la solicitud de transferencia"));
                })
                .onStatus(HttpStatusCode::is5xxServerError, response -> {
                    log.error("Error de servidor en transferencia: {}", response.statusCode());
                    return Mono.just(new AccountSystemException("Error del sistema de cuentas durante transferencia"));
                })
                .bodyToMono(TransferResponse.class)
                .timeout(responseTimeout.plus(Duration.ofSeconds(1)))
                .transform(response -> applyRetryPolicy(response, transferRetries, Duration.ofMillis(500),
                        error -> !(error instanceof TransferFailedException)))
                .map(TransferResponse::isSuccess)
                .doOnSuccess(result -> log.info("Transferencia {} -> {} completada: {}",
                        fromAccount, toAccount, result))
                .doOnError(error -> log.debug("Error en transferencia {} -> {}: {}",
                        fromAccount, toAccount, error.getMessage()));
    }

    private <T> Mono<T> applyRetryPolicy(Mono<T> request, int retries, Duration backoff,
                                       Predicate<Throwable> retryable) {
        Mono<T> result = retries == 0 ? request : request.retryWhen(Retry.backoff(retries, backoff)
                .filter(retryable)
                .onRetryExhaustedThrow((spec, signal) -> signal.failure()));
        return result
                .onErrorMap(TimeoutException.class, error -> new AccountSystemTimeoutException(
                        "Tiempo de espera agotado para el sistema de cuentas", error))
                .onErrorMap(WebClientRequestException.class, error -> {
                    Throwable cause = error.getCause();
                    if (cause instanceof io.netty.handler.timeout.TimeoutException
                            || cause instanceof io.netty.channel.ConnectTimeoutException
                            || cause instanceof java.net.SocketTimeoutException) {
                        return new AccountSystemTimeoutException(
                                "Tiempo de espera agotado para el sistema de cuentas", error);
                    }
                    return new AccountSystemUnavailableException(
                            "No se pudo obtener una respuesta del sistema de cuentas", error);
                })
                .onErrorMap(AccountSystemException.class, error -> new AccountSystemUnavailableException(
                        "El sistema de cuentas devolvió un error", error));
    }

    private static class AccountValidationResponse {
        private boolean valid;

        public boolean isValid() { return valid; }
        public void setValid(boolean valid) { this.valid = valid; }
    }

    private static class FundsCheckResponse {
        private boolean sufficient;

        public boolean isSufficient() { return sufficient; }
        public void setSufficient(boolean sufficient) { this.sufficient = sufficient; }
    }

    private static class TransferRequest {
        private String fromAccount;
        private String toAccount;
        private BigDecimal amount;
        private String operationNumber;

        public TransferRequest(String fromAccount, String toAccount, BigDecimal amount, String operationNumber) {
            this.fromAccount = fromAccount;
            this.toAccount = toAccount;
            this.amount = amount;
            this.operationNumber = operationNumber;
        }

        public String getFromAccount() { return fromAccount; }
        public String getToAccount() { return toAccount; }
        public BigDecimal getAmount() { return amount; }
        public String getOperationNumber() { return operationNumber; }
    }

    private static class TransferResponse {
        private boolean success;

        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }
    }

    private static class AccountNotFoundException extends RuntimeException {
        public AccountNotFoundException(String message) { super(message); }
    }

    private static class InsufficientFundsException extends RuntimeException {
        public InsufficientFundsException(String message) { super(message); }
    }

    private static class TransferFailedException extends RuntimeException {
        public TransferFailedException(String message) { super(message); }
    }

    private static class AccountSystemException extends RuntimeException {
        public AccountSystemException(String message) { super(message); }
    }
}
