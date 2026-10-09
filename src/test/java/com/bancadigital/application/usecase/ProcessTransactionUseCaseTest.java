package com.bancadigital.application.usecase;

import com.bancadigital.domain.model.Transaction;
import com.bancadigital.domain.model.Transaction.TransactionStatus;
import com.bancadigital.domain.port.AccountSystemClient;
import com.bancadigital.domain.port.TransactionRepository;
import com.bancadigital.domain.port.IdempotencyStore;
import com.bancadigital.domain.model.IdempotencyRecord;
import com.bancadigital.domain.exception.IdempotencyConflictException;
import com.bancadigital.domain.exception.IdempotencyReplayException;
import com.bancadigital.domain.exception.IdempotencyUnavailableException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ProcessTransactionUseCaseTest {
    @Mock private TransactionRepository repository;
    @Mock private AccountSystemClient accounts;
    @Mock private IdempotencyStore idempotency;
    private ProcessTransactionUseCase useCase;
    private final BigDecimal amount = new BigDecimal("100.00");

    @BeforeEach
    void setUp() {
        useCase = new ProcessTransactionUseCase(repository, accounts, idempotency, 2, 3000);
    }

    private void newTransaction() {
        when(idempotency.reserve(any(), any())).thenReturn(Mono.just(true));
        when(idempotency.complete(any(), any())).thenReturn(Mono.empty());
        when(repository.save(any())).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
    }

    @Test
    void successfulTransferPreservesIdAndPersistsCompletedState() {
        newTransaction();
        when(accounts.validateAccount("10001")).thenReturn(Mono.just(true));
        when(accounts.validateAccount("20002")).thenReturn(Mono.just(true));
        when(accounts.checkSufficientFunds("10001", amount)).thenReturn(Mono.just(true));
        when(accounts.transferFunds("10001", "20002", amount, "OP001")).thenReturn(Mono.just(true));

        StepVerifier.create(useCase.executeWithResult("OP001", "WEB", "10001", "20002", amount))
                .expectNextMatches(result -> !result.replayed()
                        && result.transaction().getStatus() == TransactionStatus.COMPLETED)
                .verifyComplete();

        ArgumentCaptor<Transaction> saved = ArgumentCaptor.forClass(Transaction.class);
        verify(repository, times(2)).save(saved.capture());
        Transaction pending = saved.getAllValues().get(0);
        Transaction completed = saved.getAllValues().get(1);
        assertEquals(TransactionStatus.PENDING, pending.getStatus());
        assertEquals(TransactionStatus.COMPLETED, completed.getStatus());
        assertEquals(pending.getTransactionId(), completed.getTransactionId());
        assertEquals(pending.getCreatedAt(), completed.getCreatedAt());
        assertEquals("WEB_OP001", completed.getIdempotencyKey());
        verify(accounts).transferFunds("10001", "20002", amount, "OP001");
    }

    @Test
    void existingTransactionDoesNotSaveOrCallAccounts() {
        Transaction existing = Transaction.createPendingTransaction(
                "OP001", "WEB", amount, "10001", "20002", "WEB_OP001").complete();
        when(idempotency.reserve(any(), any())).thenAnswer(invocation -> {
            IdempotencyRecord pending = invocation.getArgument(1);
            when(idempotency.find(any())).thenReturn(Mono.just(new IdempotencyRecord(
                    "original", pending.fingerprint(), existing, null, null)));
            return Mono.just(false);
        });

        StepVerifier.create(useCase.executeWithResult("OP001", "WEB", "10001", "20002", amount))
                .assertNext(result -> {
                    assertSame(existing, result.transaction());
                    org.junit.jupiter.api.Assertions.assertTrue(result.replayed());
                })
                .verifyComplete();

        verify(repository, never()).save(any());
        verifyNoInteractions(accounts);
    }

    @Test
    void insufficientFundsDoesNotTransfer() {
        newTransaction();
        when(accounts.validateAccount("10001")).thenReturn(Mono.just(true));
        when(accounts.validateAccount("20002")).thenReturn(Mono.just(true));
        when(accounts.checkSufficientFunds("10001", amount)).thenReturn(Mono.just(false));
        StepVerifier.create(useCase.executeWithResult("OP001", "WEB", "10001", "20002", amount))
                .expectErrorMessage("Fondos insuficientes")
                .verify();
        verify(accounts, never()).transferFunds(any(), any(), any(), any());
        verify(repository).save(any());
    }

    @Test
    void unavailableRedisFailsBeforeDatabaseOrTransfer() {
        when(idempotency.reserve(any(), any())).thenReturn(Mono.error(
                new IdempotencyUnavailableException("Redis no disponible", null)));
        StepVerifier.create(useCase.executeWithResult("OP001", "WEB", "10001", "20002", amount))
                .expectError(IdempotencyUnavailableException.class).verify();
        verifyNoInteractions(repository, accounts);
    }

    @Test
    void changedPayloadReturnsConflictBeforeDatabaseOrTransfer() {
        when(idempotency.reserve(any(), any())).thenReturn(Mono.just(false));
        when(idempotency.find(any())).thenReturn(Mono.just(
                new IdempotencyRecord("original", "different-payload", null, null, null)));
        StepVerifier.create(useCase.executeWithResult("OP001", "WEB", "10001", "20002", amount))
                .expectError(IdempotencyConflictException.class).verify();
        verifyNoInteractions(repository, accounts);
    }

    @Test
    void ambiguousTransferFailureIsCachedAndDuplicateNeverRepeatsIt() {
        AtomicReference<IdempotencyRecord> cached = new AtomicReference<>();
        when(idempotency.reserve(any(), any())).thenAnswer(invocation ->
                Mono.just(cached.compareAndSet(null, invocation.getArgument(1))));
        when(idempotency.complete(any(), any())).thenAnswer(invocation -> {
            cached.set(invocation.getArgument(1));
            return Mono.empty();
        });
        when(idempotency.find(any())).thenAnswer(invocation -> Mono.just(cached.get()));
        when(repository.save(any())).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(accounts.validateAccount(any())).thenReturn(Mono.just(true));
        when(accounts.checkSufficientFunds(any(), any())).thenReturn(Mono.just(true));
        when(accounts.transferFunds(any(), any(), any(), any())).thenReturn(Mono.never());
        StepVerifier.withVirtualTime(() -> useCase.executeWithResult("OP001", "WEB", "10001", "20002", amount))
                .thenAwait(Duration.ofSeconds(3)).expectErrorMatches(error ->
                        TransactionFailure.from(error).status() == 504).verify();
        StepVerifier.create(useCase.executeWithResult("OP001", "WEB", "10001", "20002", new BigDecimal("100")))
                .expectErrorMatches(error -> error instanceof IdempotencyReplayException replay
                        && replay.getStatus() == 504).verify();
        verify(accounts, times(1)).transferFunds(any(), any(), any(), any());
        verify(repository, times(1)).save(any());
    }


}
