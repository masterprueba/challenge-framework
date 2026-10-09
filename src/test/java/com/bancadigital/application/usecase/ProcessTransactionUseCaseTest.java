package com.bancadigital.application.usecase;

import com.bancadigital.domain.model.Transaction;
import com.bancadigital.domain.model.Transaction.TransactionStatus;
import com.bancadigital.domain.port.AccountSystemClient;
import com.bancadigital.domain.port.TransactionRepository;
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
    private ProcessTransactionUseCase useCase;
    private final BigDecimal amount = new BigDecimal("100.00");

    @BeforeEach
    void setUp() {
        useCase = new ProcessTransactionUseCase(repository, accounts, 2);
    }

    private void newTransaction() {
        when(repository.findByIdempotencyKey("WEB_OP001")).thenReturn(Mono.empty());
        when(repository.save(any())).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
    }

    @Test
    void successfulTransferPreservesIdAndPersistsCompletedState() {
        newTransaction();
        when(accounts.validateAccount("10001")).thenReturn(Mono.just(true));
        when(accounts.validateAccount("20002")).thenReturn(Mono.just(true));
        when(accounts.checkSufficientFunds("10001", amount)).thenReturn(Mono.just(true));
        when(accounts.transferFunds("10001", "20002", amount, "OP001")).thenReturn(Mono.just(true));

        StepVerifier.create(useCase.execute("OP001", "WEB", "10001", "20002", amount))
                .expectNextMatches(tx -> tx.getStatus() == TransactionStatus.COMPLETED)
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
        when(repository.findByIdempotencyKey("WEB_OP001")).thenReturn(Mono.just(existing));

        StepVerifier.create(useCase.execute("OP001", "WEB", "10001", "20002", amount))
                .assertNext(tx -> assertSame(existing, tx))
                .verifyComplete();

        verify(repository, never()).save(any());
        verifyNoInteractions(accounts);
    }

    @Test
    void invalidSourceDoesNotTransferOrSaveCompletedState() {
        newTransaction();
        when(accounts.validateAccount("10001")).thenReturn(Mono.just(false));
        StepVerifier.create(useCase.execute("OP001", "WEB", "10001", "20002", amount))
                .expectErrorMessage("Cuenta de origen inválida")
                .verify();
        verify(repository).save(any());
        verify(accounts, never()).validateAccount("20002");
        verify(accounts, never()).transferFunds(any(), any(), any(), any());
    }

    @Test
    void insufficientFundsDoesNotTransfer() {
        newTransaction();
        when(accounts.validateAccount("10001")).thenReturn(Mono.just(true));
        when(accounts.validateAccount("20002")).thenReturn(Mono.just(true));
        when(accounts.checkSufficientFunds("10001", amount)).thenReturn(Mono.just(false));
        StepVerifier.create(useCase.execute("OP001", "WEB", "10001", "20002", amount))
                .expectErrorMessage("Fondos insuficientes")
                .verify();
        verify(accounts, never()).transferFunds(any(), any(), any(), any());
        verify(repository).save(any());
    }

    @Test
    void accountErrorPreservesOriginalCause() {
        newTransaction();
        RuntimeException original = new RuntimeException("Connection refused");
        when(accounts.validateAccount("10001")).thenReturn(Mono.error(original));
        StepVerifier.create(useCase.execute("OP001", "WEB", "10001", "20002", amount))
                .expectErrorMatches(error -> error == original)
                .verify();
        verify(repository).save(any());
        verify(accounts, never()).transferFunds(any(), any(), any(), any());
    }
}
