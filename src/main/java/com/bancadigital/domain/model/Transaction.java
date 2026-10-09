package com.bancadigital.domain.model;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Value
@Builder(toBuilder = true)
@Jacksonized
public class Transaction {
    UUID transactionId;
    String operationNumber;
    String channel;
    BigDecimal amount;
    TransactionStatus status;
    LocalDateTime createdAt;
    LocalDateTime updatedAt;
    String accountFrom;
    String accountTo;
    String idempotencyKey;

    public enum TransactionStatus {
        PENDING,
        COMPLETED,
        FAILED,
        ROLLED_BACK
    }

    public static Transaction createPendingTransaction(String operationNumber, String channel,
                                                      BigDecimal amount, String accountFrom,
                                                      String accountTo, String idempotencyKey) {
        return Transaction.builder()
                .transactionId(UUID.randomUUID())
                .operationNumber(operationNumber)
                .channel(channel)
                .amount(amount)
                .status(TransactionStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .accountFrom(accountFrom)
                .accountTo(accountTo)
                .idempotencyKey(idempotencyKey)
                .build();
    }

    public Transaction complete() {
        return this.toBuilder()
                .status(TransactionStatus.COMPLETED)
                .updatedAt(LocalDateTime.now())
                .build();
    }

    public Transaction fail() {
        return this.toBuilder()
                .status(TransactionStatus.FAILED)
                .updatedAt(LocalDateTime.now())
                .build();
    }

    public Transaction rollback() {
        return this.toBuilder()
                .status(TransactionStatus.ROLLED_BACK)
                .updatedAt(LocalDateTime.now())
                .build();
    }
}
