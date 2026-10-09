package com.bancadigital.application.usecase;

import com.bancadigital.domain.model.Transaction;
import com.bancadigital.domain.port.AccountSystemClient;
import com.bancadigital.domain.port.TransactionRepository;
import com.bancadigital.domain.exception.TransactionTimeoutException;
import com.bancadigital.domain.exception.IdempotencyConflictException;
import com.bancadigital.domain.exception.IdempotencyReplayException;
import com.bancadigital.domain.exception.IdempotencyUnavailableException;
import com.bancadigital.domain.model.IdempotencyRecord;
import com.bancadigital.domain.model.TransactionResult;
import com.bancadigital.domain.port.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

@Service
public class ProcessTransactionUseCase {

    private static final Logger log = LoggerFactory.getLogger(ProcessTransactionUseCase.class);

    private final TransactionRepository transactionRepository;
    private final AccountSystemClient accountSystemClient;
    private final Duration operationTimeout;
    private final IdempotencyStore idempotencyStore;
    private final Duration duplicateWait;

    public ProcessTransactionUseCase(
            TransactionRepository transactionRepository,
            AccountSystemClient accountSystemClient,
            IdempotencyStore idempotencyStore,
            @Value("${transaction.timeout.seconds:2}") int timeoutSeconds,
            @Value("${idempotency.duplicate-wait-ms:3000}") int duplicateWaitMs) {
        this.transactionRepository = transactionRepository;
        this.accountSystemClient = accountSystemClient;
        this.operationTimeout = Duration.ofSeconds(timeoutSeconds);
        this.idempotencyStore = idempotencyStore;
        this.duplicateWait = Duration.ofMillis(duplicateWaitMs);
    }

    public Mono<TransactionResult> executeWithResult(String operationNumber, String channel,
            String accountFrom, String accountTo, BigDecimal amount) {
        return Mono.defer(() -> {
            String key = buildIdempotencyKey(operationNumber, channel);
            String fingerprint = fingerprint(operationNumber, channel, accountFrom, accountTo, amount);
            IdempotencyRecord pending = new IdempotencyRecord(UUID.randomUUID().toString(),
                    fingerprint, null, null, null);
            return idempotencyStore.reserve(key, pending).flatMap(owner -> {
                if (!owner) return awaitResult(key, fingerprint);
                // Timeout applies to processing only, never cancels a duplicate's reservation.
                return createAndProcessTransaction(operationNumber, channel, accountFrom, accountTo, amount, key)
                        .timeout(operationTimeout)
                        .onErrorMap(TimeoutException.class, error -> new TransactionTimeoutException(
                                "Tiempo máximo de procesamiento de la transacción agotado", error))
                        .switchIfEmpty(Mono.error(new IllegalStateException("Procesamiento sin resultado")))
                        .materialize()
                        .flatMap(signal -> {
                            if (signal.hasValue()) {
                                Transaction result = signal.get();
                                return idempotencyStore.complete(key, new IdempotencyRecord(pending.owner(),
                                        fingerprint, result, null, null))
                                        .thenReturn(new TransactionResult(result, false));
                            }
                            Throwable error = signal.getThrowable();
                            TransactionFailure failure = TransactionFailure.from(error);
                            return idempotencyStore.complete(key, new IdempotencyRecord(pending.owner(),
                                    fingerprint, null, failure.status(), failure.message()))
                                    .then(Mono.<TransactionResult>error(error));
                        });
            });
        });
    }

    public Mono<IdempotencyRecord> findIdempotency(String key) {
        return idempotencyStore.find(key);
    }

    private Mono<TransactionResult> awaitResult(String key, String fingerprint) {
        return Mono.defer(() -> idempotencyStore.find(key)
                        .switchIfEmpty(Mono.error(new IdempotencyUnavailableException(
                                "La reserva de idempotencia ya no está disponible", null)))
                        .flatMap(entry -> {
                            if (!entry.fingerprint().equals(fingerprint)) return Mono.error(
                                    new IdempotencyConflictException("La clave de idempotencia ya fue usada con otros datos"));
                            return entry.finished() ? Mono.just(entry) : Mono.empty();
                        }))
                .repeatWhenEmpty(repeats -> repeats.delayElements(Duration.ofMillis(50)))
                .timeout(duplicateWait, Mono.error(new IdempotencyConflictException(
                        "La transacción con esta clave todavía está en proceso")))
                .flatMap(entry -> entry.errorStatus() != null
                        ? Mono.error(new IdempotencyReplayException(entry.errorStatus(), entry.errorMessage()))
                        : Mono.just(new TransactionResult(entry.transaction(), true)));
    }

    private String fingerprint(String operation, String channel, String from, String to, BigDecimal amount) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String field : List.of(operation, channel, from, to, amount.stripTrailingZeros().toPlainString())) {
                byte[] value = field.getBytes(StandardCharsets.UTF_8);
                digest.update(java.nio.ByteBuffer.allocate(4).putInt(value.length).array());
                digest.update(value);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException(error);
        }
    }

    private Mono<Transaction> createAndProcessTransaction(String operationNumber, String channel,
                                                           String accountFrom, String accountTo,
                                                           BigDecimal amount, String idempotencyKey) {
        log.info("Creando nueva transacción con clave de idempotencia: {}", idempotencyKey);

        Transaction pendingTransaction = Transaction.createPendingTransaction(operationNumber, channel,
                amount, accountFrom, accountTo, idempotencyKey);

        return transactionRepository.save(pendingTransaction)
                .flatMap(this::validateAndExecuteTransfer)
                .doOnError(error -> log.debug("Error al procesar transacción: {}", error.getMessage()));
    }

    private Mono<Transaction> validateAndExecuteTransfer(Transaction transaction) {
        return accountSystemClient.validateAccount(transaction.getAccountFrom())
                .filter(Boolean::booleanValue)
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Cuenta de origen inválida")))
                .then(Mono.defer(() -> accountSystemClient.validateAccount(transaction.getAccountTo())))
                .filter(Boolean::booleanValue)
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Cuenta de destino inválida")))
                .then(Mono.defer(() -> accountSystemClient.checkSufficientFunds(transaction.getAccountFrom(),
                        transaction.getAmount())))
                .filter(Boolean::booleanValue)
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Fondos insuficientes")))
                .then(Mono.defer(() -> accountSystemClient.transferFunds(transaction.getAccountFrom(),
                        transaction.getAccountTo(), transaction.getAmount(),
                        transaction.getOperationNumber())))
                .filter(Boolean::booleanValue)
                .switchIfEmpty(Mono.error(new IllegalStateException("Falló la transferencia en sistema de cuentas")))
                .then(Mono.defer(() -> transactionRepository.save(transaction.complete())));
    }

    private String buildIdempotencyKey(String operationNumber, String channel) {
        // Escape the separator so (WEB_A, B) and (WEB, A_B) are different identities.
        return escapeKeyPart(channel) + "_" + escapeKeyPart(operationNumber);
    }

    private String escapeKeyPart(String part) {
        return part.replace("%", "%25").replace("_", "%5F");
    }
}
